"""
Yahoo Finance PostgreSQL storage.

Symbols catalog: unified table "symbols" filled from nasdaqtrader.nasdaqlisted + otherlisted.
equity_info: one row per (symbol, property): symbol, info (property name), value (JSONB scalar), updated_at.
extended_data: valuation_measures, analyst_recommendations, etc. (JSONB) per symbol.
Uses datalake database, yfinance schema.
"""

import json
import math
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

import psycopg2
from tqdm import tqdm
from psycopg2.extras import execute_values, Json

from src.postgres_connection import get_datalake_connection

_SCHEMA = "yfinance"


def _to_json_safe(obj: Any) -> Any:
    """Convert to JSON-serializable types for JSONB storage."""
    if obj is None:
        return None
    if hasattr(obj, "isoformat"):  # datetime, date
        return obj.isoformat()
    if hasattr(obj, "item"):  # numpy scalar
        return obj.item()
    if isinstance(obj, (bytes, bytearray)):
        return obj.decode("utf-8", errors="replace")
    if isinstance(obj, dict):
        return {k: _to_json_safe(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_to_json_safe(v) for v in obj]
    if isinstance(obj, (str, int, bool)):
        return obj
    if isinstance(obj, float):
        return obj if math.isfinite(obj) else None  # Infinity/NaN -> null for JSONB
    return str(obj)


def get_postgres_connection(
    dbname: Optional[str] = None,
    user: Optional[str] = None,
    host: Optional[str] = None,
    password: Optional[str] = None,
    port: Optional[int] = None,
) -> psycopg2.extensions.connection:
    """Connect to the datalake yfinance schema. Uses ENV for host/user; POSTGRES_PASSWORD, POSTGRES_PORT."""
    return get_datalake_connection(
        _SCHEMA,
        user=user,
        host=host,
        password=password,
        port=port,
    )


def init_yfinance_postgres_tables(conn: psycopg2.extensions.connection) -> None:
    """
    Create symbols table if it doesn't exist.
    Used by --generate-catalog so the table is created when missing (DB must already exist).
    """
    cur = conn.cursor()
    # exchange: max length 32 from nasdaqtrader.exchanges (e.g. "Investors' Exchange, LLC (IEXG)");
    # "nasdaq" for nasdaqlisted, joined exchange name for otherlisted
    cur.execute("""
        CREATE TABLE IF NOT EXISTS symbols (
            symbol VARCHAR(20) NOT NULL,
            exchange VARCHAR(32) NOT NULL,
            security_name TEXT,
            source VARCHAR(20) NOT NULL,
            etf VARCHAR(5),
            round_lot_size INTEGER,
            test_issue VARCHAR(5),
            upload_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (symbol, exchange)
        )
    """)
    # Normalized: one row per (symbol, property name); value is the scalar (JSONB).
    cur.execute("""
        SELECT column_name FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = 'equity_info'
    """)
    cols = [r[0] for r in cur.fetchall()]
    if cols and "value" not in cols:
        cur.execute("DROP TABLE IF EXISTS equity_info")
    cur.execute("""
        CREATE TABLE IF NOT EXISTS equity_info (
            symbol VARCHAR(20) NOT NULL,
            info VARCHAR(128) NOT NULL,
            value JSONB,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (symbol, info)
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS extended_data (
            symbol VARCHAR(20) PRIMARY KEY,
            valuation_measures JSONB DEFAULT '[]',
            eps_revisions JSONB DEFAULT '[]',
            revenue_estimates JSONB DEFAULT '[]',
            analyst_recommendations JSONB DEFAULT '[]',
            analyst_price_targets JSONB DEFAULT '[]',
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS timeseries_ohlcv (
            symbol VARCHAR(20) NOT NULL,
            date DATE NOT NULL,
            open NUMERIC,
            high NUMERIC,
            low NUMERIC,
            close NUMERIC,
            volume BIGINT,
            dividends NUMERIC,
            stock_splits NUMERIC,
            source VARCHAR(20) NOT NULL,
            PRIMARY KEY (symbol, date, source)
        )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_timeseries_ohlcv_symbol ON timeseries_ohlcv(symbol, source)")
    conn.commit()
    cur.close()


def get_max_date_for_symbol(
    conn: psycopg2.extensions.connection,
    symbol: str,
    source: str,
) -> Optional[datetime]:
    """Return the latest date for symbol in timeseries_ohlcv, or None if no rows."""
    cur = conn.cursor()
    cur.execute(
        "SELECT MAX(date) FROM timeseries_ohlcv WHERE symbol = %s AND source = %s",
        (symbol, source),
    )
    row = cur.fetchone()
    cur.close()
    if row and row[0]:
        return row[0] if isinstance(row[0], datetime) else datetime.combine(row[0], datetime.min.time())
    return None


def insert_timeseries_rows(
    conn: psycopg2.extensions.connection,
    symbol: str,
    df: "pd.DataFrame",
    source: str,
) -> int:
    """Insert OHLCV rows from DataFrame. Uses ON CONFLICT DO UPDATE. Returns rows inserted."""
    import pandas as pd

    if df is None or df.empty:
        return 0
    df = df.copy()
    df = df.reset_index()
    date_col = next((c for c in ("Date", "date", "index") if c in df.columns), df.columns[0] if len(df.columns) > 0 else None)
    if date_col is None:
        return 0
    df["date"] = pd.to_datetime(df[date_col]).dt.date
    col_map = {"Open": "open", "High": "high", "Low": "low", "Close": "close", "Volume": "volume", "Dividends": "dividends", "Stock Splits": "stock_splits"}
    for yf_col, db_col in col_map.items():
        if yf_col in df.columns:
            df[db_col] = df[yf_col]
        else:
            df[db_col] = None
    df["symbol"] = symbol
    df["source"] = source
    rows = []
    for _, r in df.iterrows():
        rows.append((
            r["symbol"],
            r["date"],
            r.get("open"),
            r.get("high"),
            r.get("low"),
            r.get("close"),
            int(r["volume"]) if pd.notna(r.get("volume")) and r.get("volume") is not None else None,
            r.get("dividends"),
            r.get("stock_splits"),
            r["source"],
        ))
    if not rows:
        return 0
    cur = conn.cursor()
    execute_values(
        cur,
        """
        INSERT INTO timeseries_ohlcv (symbol, date, open, high, low, close, volume, dividends, stock_splits, source)
        VALUES %s
        ON CONFLICT (symbol, date, source) DO UPDATE SET
            open = EXCLUDED.open,
            high = EXCLUDED.high,
            low = EXCLUDED.low,
            close = EXCLUDED.close,
            volume = EXCLUDED.volume,
            dividends = EXCLUDED.dividends,
            stock_splits = EXCLUDED.stock_splits
        """,
        rows,
        page_size=500,
    )
    conn.commit()
    cur.close()
    return len(rows)


def get_timeseries_as_dataframe(
    conn: psycopg2.extensions.connection,
    symbol: str,
    source: str,
) -> Optional["pd.DataFrame"]:
    """Return full OHLCV history for symbol as DataFrame (DatetimeIndex, Open/High/Low/Close/Volume)."""
    import pandas as pd

    cur = conn.cursor()
    cur.execute(
        """
        SELECT date, open, high, low, close, volume, dividends, stock_splits
        FROM timeseries_ohlcv
        WHERE symbol = %s AND source = %s
        ORDER BY date
        """,
        (symbol, source),
    )
    rows = cur.fetchall()
    cur.close()
    if not rows:
        return None
    df = pd.DataFrame(
        rows,
        columns=["Date", "Open", "High", "Low", "Close", "Volume", "Dividends", "Stock Splits"],
    )
    df["Date"] = pd.to_datetime(df["Date"])
    df = df.set_index("Date")
    return df


def require_symbols_table(conn: psycopg2.extensions.connection) -> None:
    """
    Raise if the yfinance DB does not have a table named symbols.
    Call before generate_catalog_from_nasdaqtrader when table must pre-exist.
    """
    cur = conn.cursor()
    cur.execute("""
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = 'public' AND table_name = 'symbols'
    """)
    if cur.fetchone() is None:
        cur.close()
        raise RuntimeError(
            "yfinance database or table 'symbols' does not exist. "
            "Create the yfinance database and the symbols table first."
        )
    cur.close()


def generate_catalog_from_nasdaqtrader(
    nasdaqtrader_conn: psycopg2.extensions.connection,
    yfinance_conn: psycopg2.extensions.connection,
) -> int:
    """
    Read nasdaqlisted and otherlisted from nasdaqtrader DB, concatenate into a
    unified list, upsert into yfinance.symbols. Returns total rows upserted.
    Caller should call init_yfinance_postgres_tables first to create the table if missing.
    """
    cur_nt = nasdaqtrader_conn.cursor()
    cur_nt.execute("""
        SELECT symbol, security_name, test_issue, round_lot_size, etf
        FROM nasdaqlisted
    """)
    nasdaq_rows = cur_nt.fetchall()
    # Join otherlisted with exchanges so the catalog gets full exchange names (exchanges.exchange), not the code (otherlisted.exchange)
    cur_nt.execute("""
        SELECT o.act_symbol, COALESCE(e.exchange, o.exchange), o.security_name, o.test_issue,
               o.round_lot_size, o.etf
        FROM otherlisted o
        LEFT JOIN exchanges e ON e.symbol = o.exchange
    """)
    other_rows = cur_nt.fetchall()
    cur_nt.close()

    # Normalize to (symbol, exchange, security_name, source, etf, round_lot_size, test_issue). Skip symbols with "$" (e.g. ABR$D).
    values = []
    for r in nasdaq_rows:
        symbol, security_name, test_issue, round_lot_size, etf = r
        if "$" in (symbol or ""):
            continue
        values.append((symbol, "nasdaq", security_name, "nasdaqlisted", etf, round_lot_size, test_issue))
    for r in other_rows:
        act_symbol, exchange, security_name, test_issue, round_lot_size, etf = r
        if "$" in (act_symbol or ""):
            continue
        values.append((act_symbol, exchange, security_name, "otherlisted", etf, round_lot_size, test_issue))

    if not values:
        return 0

    cur_yf = yfinance_conn.cursor()
    execute_values(
        cur_yf,
        """
        INSERT INTO symbols (symbol, exchange, security_name, source, etf, round_lot_size, test_issue)
        VALUES %s
        ON CONFLICT (symbol, exchange) DO UPDATE SET
            security_name = EXCLUDED.security_name,
            source = EXCLUDED.source,
            etf = EXCLUDED.etf,
            round_lot_size = EXCLUDED.round_lot_size,
            test_issue = EXCLUDED.test_issue,
            upload_date = CURRENT_TIMESTAMP
        """,
        values,
        page_size=2000,
    )
    cur_yf.close()
    yfinance_conn.commit()
    return len(values)


def get_symbols_from_catalog(conn: psycopg2.extensions.connection, limit: Optional[int] = None) -> list[str]:
    """Return distinct symbol list from symbols table, optionally limited."""
    cur = conn.cursor()
    cur.execute("SELECT DISTINCT symbol FROM symbols ORDER BY symbol")
    rows = cur.fetchall()
    cur.close()
    symbols = [r[0] for r in rows]
    if limit is not None:
        symbols = symbols[:limit]
    return symbols


def get_etf_symbols_from_catalog(conn: psycopg2.extensions.connection, limit: Optional[int] = None) -> list[str]:
    """Return ETF tickers from symbols table (etf = 'Y'), optionally limited."""
    cur = conn.cursor()
    cur.execute("""
        SELECT DISTINCT symbol FROM symbols
        WHERE UPPER(TRIM(COALESCE(etf, ''))) = 'Y'
        ORDER BY symbol
    """)
    rows = cur.fetchall()
    cur.close()
    symbols = [r[0] for r in rows]
    if limit is not None:
        symbols = symbols[:limit]
    return symbols


def upsert_equity_info(conn: psycopg2.extensions.connection, symbol: str, info: Dict[str, Any]) -> int:
    """
    Upsert equity info into equity_info table (one row per property).
    info should be schema-cleaned. Returns number of rows upserted.
    """
    if not info:
        return 0
    cur = conn.cursor()
    cur.execute("DELETE FROM equity_info WHERE symbol = %s", (symbol,))
    rows = [(symbol, k, Json(_to_json_safe(v))) for k, v in info.items()]
    execute_values(
        cur,
        """
        INSERT INTO equity_info (symbol, info, value, updated_at)
        VALUES %s
        """,
        rows,
        template="(%s, %s, %s, CURRENT_TIMESTAMP)",
    )
    conn.commit()
    cur.close()
    return len(rows)


def upsert_extended_data(
    conn: psycopg2.extensions.connection,
    symbol: str,
    valuation_measures: list,
    eps_revisions: list,
    revenue_estimates: list,
    analyst_recommendations: list,
    analyst_price_targets: list,
) -> None:
    """Upsert extended data sections into extended_data table."""
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO extended_data (
            symbol, valuation_measures, eps_revisions, revenue_estimates,
            analyst_recommendations, analyst_price_targets, updated_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
        ON CONFLICT (symbol) DO UPDATE SET
            valuation_measures = EXCLUDED.valuation_measures,
            eps_revisions = EXCLUDED.eps_revisions,
            revenue_estimates = EXCLUDED.revenue_estimates,
            analyst_recommendations = EXCLUDED.analyst_recommendations,
            analyst_price_targets = EXCLUDED.analyst_price_targets,
            updated_at = CURRENT_TIMESTAMP
        """,
        (
            symbol,
            Json(_to_json_safe(valuation_measures)),
            Json(_to_json_safe(eps_revisions)),
            Json(_to_json_safe(revenue_estimates)),
            Json(_to_json_safe(analyst_recommendations)),
            Json(_to_json_safe(analyst_price_targets)),
        ),
    )
    conn.commit()
    cur.close()


def upload_equity_info_from_json_dir(
    conn: psycopg2.extensions.connection,
    json_dir: Path,
    schema_path: Optional[Path] = None,
) -> int:
    """
    Read each .json file in json_dir, apply equity_info schema (filter + convert types), upsert into equity_info
    as one row per (symbol, property): symbol, info (property name), value. Filename must be {symbol}.json.
    Returns number of rows upserted (property rows).
    """
    from .equity_info_schema import apply_schema, load_schema

    schema = load_schema(schema_path) if schema_path else load_schema()
    json_dir = Path(json_dir)
    paths = sorted(json_dir.glob("*.json"))
    count = 0
    for path in tqdm(paths, desc="Upload equity_info", unit="symbol"):
        symbol = path.stem
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        cleaned = apply_schema(raw, schema)
        count += upsert_equity_info(conn, symbol, cleaned)
    return count
