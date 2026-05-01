"""
iShares PostgreSQL storage.

Tables: etfs (from ishares_etfs.csv), holdings_summary and holdings_detailed
(from holdings/summary/*.csv and holdings/detailed/*.csv). On each upload run:
TRUNCATE then INSERT (full replace).
"""

import os
import re
import sys
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Optional

import pandas as pd
import psycopg2
from tqdm import tqdm
from psycopg2.extras import execute_values
from psycopg2 import sql

from src.postgres_connection import get_datalake_connection

_SCHEMA = "ishares"


def _sanitize_column(name: str) -> str:
    """Convert column name to valid PostgreSQL identifier."""
    s = re.sub(r"[^\w]", "_", str(name).strip())
    s = re.sub(r"_+", "_", s).strip("_").lower()
    return s or "col"


def get_postgres_connection(
    dbname: Optional[str] = None,
    user: Optional[str] = None,
    host: Optional[str] = None,
    password: Optional[str] = None,
    port: Optional[int] = None,
) -> psycopg2.extensions.connection:
    """Connect to the datalake ishares schema. Uses ENV for host/user; POSTGRES_PASSWORD, POSTGRES_PORT."""
    return get_datalake_connection(
        _SCHEMA,
        user=user,
        host=host,
        password=password,
        port=port,
    )


_HOLDINGS_TABLE_DDL = """
    etf_ticker VARCHAR(20) NOT NULL,
    ticker VARCHAR(20),
    name VARCHAR(200) NOT NULL,
    sector VARCHAR(200),
    asset_class VARCHAR(200),
    market_value DOUBLE PRECISION,
    weight DECIMAL(8, 4),
    notional_value DOUBLE PRECISION,
    quantity DOUBLE PRECISION,
    price DOUBLE PRECISION,
    location VARCHAR(200),
    exchange VARCHAR(200),
    currency VARCHAR(5),
    fx_rate DOUBLE PRECISION,
    market_currency VARCHAR(5),
    accrual_date TIMESTAMP,
    par_value DOUBLE PRECISION,
    cusip VARCHAR(20),
    isin VARCHAR(20),
    sedol VARCHAR(20),
    duration DOUBLE PRECISION,
    ytm DOUBLE PRECISION,
    maturity TIMESTAMP,
    coupon DOUBLE PRECISION,
    mod_duration DOUBLE PRECISION,
    yield_to_call DOUBLE PRECISION,
    yield_to_worst DOUBLE PRECISION,
    real_duration DOUBLE PRECISION,
    real_ytm DOUBLE PRECISION,
    effective_date TIMESTAMP,
    "type" VARCHAR(200),
    strike_price DOUBLE PRECISION,
    notional_weight DOUBLE PRECISION,
    market_weight DOUBLE PRECISION,
    updated_at TIMESTAMP
"""


def init_ishares_postgres_tables(conn: psycopg2.extensions.connection) -> None:
    """Create etfs, holdings_summary, and holdings_detailed tables if they don't exist.
    Drops the legacy single holdings table if present."""
    cur = conn.cursor()
    cur.execute("DROP TABLE IF EXISTS holdings")
    cur.execute("""
        CREATE TABLE IF NOT EXISTS etfs (
            ticker VARCHAR(20) NOT NULL PRIMARY KEY,
            name VARCHAR(200) NOT NULL,
            asset_class TEXT,
            expense_ratio DOUBLE PRECISION,
            total_net_assets DOUBLE PRECISION,
            ytd_return DOUBLE PRECISION,
            one_year_return DOUBLE PRECISION,
            three_year_return DOUBLE PRECISION,
            five_year_return DOUBLE PRECISION,
            ten_year_return DOUBLE PRECISION,
            inception_date TIMESTAMP,
            primary_benchmark VARCHAR(200),
            fund_url TEXT,
            created_at TIMESTAMP NOT NULL,
            updated_at TIMESTAMP NOT NULL
        )
    """)
    for table in ("holdings_summary", "holdings_detailed"):
        cur.execute(f"CREATE TABLE IF NOT EXISTS {table} ({_HOLDINGS_TABLE_DDL})")
    conn.commit()
    cur.close()


def get_etf_tickers(conn: psycopg2.extensions.connection, limit: Optional[int] = None) -> list[str]:
    """Return ticker list from ishares.etfs table, optionally limited."""
    cur = conn.cursor()
    cur.execute("SELECT ticker FROM etfs ORDER BY ticker")
    rows = cur.fetchall()
    cur.close()
    tickers = [r[0] for r in rows if r[0]]
    if limit is not None:
        tickers = tickers[:limit]
    return tickers


def get_holdings_tickers_updated_today(conn: psycopg2.extensions.connection) -> set[str]:
    """
    Return set of etf_ticker values that have holdings updated today in either
    holdings_summary or holdings_detailed. Used so the download pipeline
    never re-downloads those tickers on the same day.
    """
    cur = conn.cursor()
    cur.execute("""
        SELECT DISTINCT etf_ticker FROM (
            SELECT etf_ticker FROM holdings_summary
            WHERE updated_at IS NOT NULL AND updated_at::date = CURRENT_DATE
            UNION
            SELECT etf_ticker FROM holdings_detailed
            WHERE updated_at IS NOT NULL AND updated_at::date = CURRENT_DATE
        ) u
    """)
    rows = cur.fetchall()
    cur.close()
    return {r[0] for r in rows if r[0]}


_ETFS_COLUMNS = [
    "ticker", "name", "asset_class", "expense_ratio", "total_net_assets",
    "ytd_return", "one_year_return", "three_year_return", "five_year_return",
    "ten_year_return", "inception_date", "primary_benchmark", "fund_url",
    "created_at", "updated_at",
]

_ETFS_FLOAT_COLUMNS = {
    "expense_ratio", "total_net_assets", "ytd_return", "one_year_return",
    "three_year_return", "five_year_return", "ten_year_return",
}
_ETFS_TIMESTAMP_COLUMNS = {"inception_date", "created_at", "updated_at"}
_ETFS_VARCHAR_MAX_LEN = {"ticker": 20, "name": 200, "primary_benchmark": 200}


def _coerce_etf_value(col: str, val) -> Optional[object]:
    """Coerce a single value for etfs table (float, timestamp, or string)."""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    if col in _ETFS_FLOAT_COLUMNS:
        if isinstance(val, (int, float)):
            return float(val) if not pd.isna(val) else None
        s = str(val).strip().rstrip("%").replace(",", "")
        if not s:
            return None
        try:
            return float(s)
        except ValueError:
            return None
    if col in _ETFS_TIMESTAMP_COLUMNS:
        if isinstance(val, pd.Timestamp):
            return val.to_pydatetime() if pd.notna(val) else None
        if hasattr(val, "timestamp"):
            return val
        s = str(val).strip() if val is not None else ""
        if not s:
            return None
        try:
            return pd.to_datetime(s, errors="raise").to_pydatetime()
        except Exception:
            return None
    # string columns: truncate to max length if defined
    s = str(val).strip() if val is not None else ""
    max_len = _ETFS_VARCHAR_MAX_LEN.get(col)
    if max_len is not None and len(s) > max_len:
        s = s[:max_len]
    return s if s else None


def upload_etfs(conn: psycopg2.extensions.connection, csv_path: Path) -> int:
    """
    Replace all rows in etfs: TRUNCATE then INSERT from ishares_etfs.csv.
    Coerces types to match table (float, timestamp, varchar). Returns number of rows inserted.
    """
    df = pd.read_csv(csv_path)
    if df.empty:
        return 0
    columns = [c for c in _ETFS_COLUMNS if c in df.columns]
    if not columns:
        return 0
    df = df[columns].copy()

    # Ensure created_at/updated_at exist; fill with now() if missing/empty
    now = pd.Timestamp.utcnow()
    for ts_col in ("created_at", "updated_at"):
        if ts_col in df.columns:
            df[ts_col] = df[ts_col].where(pd.notna(df[ts_col]) & (df[ts_col].astype(str).str.strip() != ""), now)
        else:
            df[ts_col] = now

    cur = conn.cursor()
    cur.execute("TRUNCATE TABLE etfs")
    rows_list = list(
        tqdm(
            df.itertuples(index=False),
            total=len(df),
            desc="Upload ETFs",
            unit="row",
            dynamic_ncols=True,
            leave=True,
            mininterval=0.3,
            file=sys.stderr,
        )
    )
    col_names = list(columns)
    values = [
        tuple(_coerce_etf_value(col_names[i], getattr(row, col_names[i])) for i in range(len(col_names)))
        for row in rows_list
    ]
    # NOT NULL: ticker, name, created_at, updated_at - ensure we don't insert None
    for i, row in enumerate(values):
        r = list(row)
        if r[0] is None or (isinstance(r[0], str) and not r[0].strip()):
            r[0] = f"_missing_{i}"
        if r[1] is None or (isinstance(r[1], str) and not r[1].strip()):
            r[1] = ""
        if r[-2] is None:
            r[-2] = now.to_pydatetime()
        if r[-1] is None:
            r[-1] = now.to_pydatetime()
        values[i] = tuple(r)
    cols_sql = sql.SQL(", ").join(sql.Identifier(c) for c in columns)
    execute_values(
        cur,
        sql.SQL("INSERT INTO etfs ({}) VALUES %s").format(cols_sql),
        values,
        page_size=500,
    )
    conn.commit()
    n = len(values)
    cur.close()
    return n


# Holdings table: fixed schema (no coercion on upload)
_HOLDINGS_COLUMNS = [
    "etf_ticker", "ticker", "name", "sector", "asset_class",
    "market_value", "weight", "notional_value", "quantity", "price",
    "location", "exchange", "currency", "fx_rate", "market_currency",
    "accrual_date", "par_value", "cusip", "isin", "sedol",
    "duration", "ytm", "maturity", "coupon", "mod_duration",
    "yield_to_call", "yield_to_worst", "real_duration", "real_ytm",
    "effective_date", "type", "strike_price", "notional_weight", "market_weight",
    "updated_at",
]
# Map sanitized CSV header -> schema column (for minor naming variants)
_HOLDINGS_HEADER_ALIASES = {
    "marketvalue": "market_value", "notionalvalue": "notional_value",
    "fxrate": "fx_rate", "marketcurrency": "market_currency",
    "accrualdate": "accrual_date", "parvalue": "par_value",
    "modduration": "mod_duration", "yieldtocall": "yield_to_call",
    "yieldtoworst": "yield_to_worst", "realduration": "real_duration",
    "realytm": "real_ytm", "effectivedate": "effective_date",
    "strikeprice": "strike_price", "notionalweight": "notional_weight",
    "marketweight": "market_weight", "assetclass": "asset_class",
    "etfticker": "etf_ticker",
}

# Columns stored as double (comma = thousands; normalize on upload: replace ',' with '', then float())
_HOLDINGS_DOUBLE_COLUMNS = (
    "market_value", "notional_value", "quantity", "price", "fx_rate",
    "par_value", "duration", "ytm", "coupon", "mod_duration",
    "yield_to_call", "yield_to_worst", "real_duration", "real_ytm",
    "strike_price", "notional_weight", "market_weight",
)
# Columns stored as DECIMAL in DB (use Decimal for exact representation)
_HOLDINGS_DECIMAL_COLUMNS = ("weight",)


def _to_float(v) -> Optional[float]:
    """Replace ',' with '' and float(); empty/invalid -> None."""
    if v is None or pd.isna(v):
        return None
    s = str(v).strip().replace(",", "")
    if not s or s.lower() in ("nan", "none", "n/a", ""):
        return None
    try:
        return float(s)
    except ValueError as e:
        raise e


def _to_decimal(v) -> Optional[Decimal]:
    """Replace ',' with '' and Decimal(); empty/invalid -> None."""
    if v is None or pd.isna(v):
        return None
    s = str(v).strip().replace(",", "")
    if not s or s.lower() in ("nan", "none", "n/a", ""):
        return None
    try:
        return Decimal(s)
    except Exception:
        return None


def _cell_to_db(v):
    """Convert a cell to DB representation: NA -> None, Timestamp -> datetime."""
    if pd.isna(v):
        return None
    if isinstance(v, pd.Timestamp):
        return v.to_pydatetime()
    return v


def _upload_holdings_to_table(
    cur,
    table: str,
    holdings_subdir: Path,
    run_ts: datetime,
) -> int:
    """TRUNCATE table and INSERT from all *_holdings.csv in holdings_subdir. Returns row count."""
    files = sorted(holdings_subdir.glob("*_holdings.csv"))
    if not files:
        return 0
    cur.execute(sql.SQL("TRUNCATE TABLE {}").format(sql.Identifier(table)))
    cols_sql = sql.SQL(", ").join(sql.Identifier(c) for c in _HOLDINGS_COLUMNS)
    insert_sql = sql.SQL("INSERT INTO {} ({}) VALUES %s").format(
        sql.Identifier(table), cols_sql
    )
    total = 0
    pbar = tqdm(
        files,
        desc=f"Upload {table}",
        unit="file",
        dynamic_ncols=True,
        leave=True,
        mininterval=0.3,
        file=sys.stderr,
    )
    for f in pbar:
        try:
            df = pd.read_csv(f)
        except Exception as e:
            pbar.set_postfix(file=f.name, refresh=False)
            raise ValueError(f"file {f.name!r}: {e}") from e
        if df.empty:
            continue
        col_map = {}
        for c in df.columns:
            sane = _sanitize_column(c)
            canonical = _HOLDINGS_HEADER_ALIASES.get(sane, sane)
            if canonical in _HOLDINGS_COLUMNS:
                col_map[c] = canonical
        if not col_map:
            continue
        df = df.rename(columns=col_map)
        if "etf_ticker" not in df.columns:
            etf_ticker = f.stem.replace("_holdings", "").strip()
            df.insert(0, "etf_ticker", etf_ticker)
        keep = [c for c in _HOLDINGS_COLUMNS if c in df.columns]
        df = df[keep].copy()
        for c in _HOLDINGS_COLUMNS:
            if c not in df.columns:
                df[c] = None
        df = df[_HOLDINGS_COLUMNS]
        for col in _HOLDINGS_DOUBLE_COLUMNS:
            if col in df.columns:
                df[col] = df[col].apply(_to_float)
        for col in _HOLDINGS_DECIMAL_COLUMNS:
            if col in df.columns:
                df[col] = df[col].apply(_to_decimal)
        df["updated_at"] = run_ts
        values = [tuple(_cell_to_db(v) for v in row) for row in df.values]
        if not values:
            continue
        try:
            execute_values(cur, insert_sql, values, page_size=min(1000, len(values)))
            total += len(values)
        except Exception as e:
            pbar.set_postfix(file=f.name, refresh=False)
            raise ValueError(f"file {f.name!r}: {e}") from e
        pbar.set_postfix(rows=total, refresh=False)
    return total


def upload_holdings(conn: psycopg2.extensions.connection, holdings_dir: Path) -> int:
    """
    Replace all rows in holdings_summary and holdings_detailed from
    holdings_dir/summary/*.csv and holdings_dir/detailed/*.csv.
    TRUNCATE then INSERT per table. Returns total rows inserted (summary + detailed).
    """
    run_ts = datetime.utcnow()
    cur = conn.cursor()
    try:
        n_summary = _upload_holdings_to_table(
            cur, "holdings_summary", holdings_dir / "summary", run_ts
        )
        n_detailed = _upload_holdings_to_table(
            cur, "holdings_detailed", holdings_dir / "detailed", run_ts
        )
        conn.commit()
        return n_summary + n_detailed
    except Exception as e:
        conn.rollback()
        raise
    finally:
        cur.close()


_VIEWS_DIR = Path(__file__).resolve().parent / "views-sql"


def apply_views(conn: psycopg2.extensions.connection) -> None:
    """
    Execute all .sql files in views-sql/ to create or replace views.
    Run after upload_holdings() so views reflect current data.
    """
    if not _VIEWS_DIR.exists():
        return
    cur = conn.cursor()
    for sql_file in sorted(_VIEWS_DIR.glob("*.sql")):
        try:
            cur.execute(sql_file.read_text())
            conn.commit()
        except Exception as e:
            conn.rollback()
            cur.close()
            raise RuntimeError(f"Failed to apply {sql_file.name}: {e}") from e
    cur.close()
