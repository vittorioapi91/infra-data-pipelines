"""
NASDAQ Trader PostgreSQL storage.

Tables: nasdaqlisted, otherlisted (from nasdaqlisted.txt and otherlisted.txt).
Both have upload_date DEFAULT CURRENT_TIMESTAMP; uploads are upserts.
Uses datalake database, nasdaqtrader schema.
"""

import os
from pathlib import Path
from typing import Optional

import psycopg2
from psycopg2.extras import execute_values

from src.postgres_connection import get_datalake_connection

_SCHEMA = "nasdaqtrader"


def get_postgres_connection(
    dbname: Optional[str] = None,
    user: Optional[str] = None,
    host: Optional[str] = None,
    password: Optional[str] = None,
    port: Optional[int] = None,
) -> psycopg2.extensions.connection:
    """Connect to the datalake nasdaqtrader schema. Uses ENV for host/user; POSTGRES_PASSWORD, POSTGRES_PORT."""
    return get_datalake_connection(
        _SCHEMA,
        user=user,
        host=host,
        password=password,
        port=port,
    )


def init_nasdaqtrader_postgres_tables(conn: psycopg2.extensions.connection) -> None:
    """
    Create nasdaqlisted and otherlisted tables if they don't exist.
    Both have upload_date with server default for insert/update.
    """
    cur = conn.cursor()

    # nasdaqlisted: columns match nasdaqlisted.txt (pipe-delimited)
    # Symbol|Security Name|Market Category|Test Issue|Financial Status|Round Lot Size|ETF|NextShares
    cur.execute("""
        CREATE TABLE IF NOT EXISTS nasdaqlisted (
            symbol VARCHAR(20) PRIMARY KEY,
            security_name TEXT,
            market_category VARCHAR(10),
            test_issue VARCHAR(5),
            financial_status VARCHAR(5),
            round_lot_size INTEGER,
            etf VARCHAR(5),
            next_shares VARCHAR(5),
            upload_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # otherlisted: ACT Symbol|Security Name|Exchange|CQS Symbol|ETF|Round Lot Size|Test Issue|NASDAQ Symbol
    cur.execute("""
        CREATE TABLE IF NOT EXISTS otherlisted (
            act_symbol VARCHAR(20) NOT NULL,
            exchange VARCHAR(5) NOT NULL,
            security_name TEXT,
            cqs_symbol VARCHAR(20),
            etf VARCHAR(5),
            round_lot_size INTEGER,
            test_issue VARCHAR(5),
            nasdaq_symbol VARCHAR(20),
            upload_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (act_symbol, exchange)
        )
    """)

    # exchanges: reference table (symbol -> exchange name), one-off upload
    cur.execute("""
        CREATE TABLE IF NOT EXISTS exchanges (
            symbol VARCHAR(5) PRIMARY KEY,
            exchange TEXT NOT NULL
        )
    """)

    conn.commit()
    cur.close()


# One-off reference data for exchanges table (symbol as in otherlisted.exchange)
EXCHANGES_REFERENCE_DATA = [
    ("A", "NYSE MKT"),
    ("N", "New York Stock Exchange (NYSE)"),
    ("P", "NYSE ARCA"),
    ("Z", "BATS Global Markets (BATS)"),
    ("V", "Investors' Exchange, LLC (IEXG)"),
]


def exchanges_table_exists(conn: psycopg2.extensions.connection) -> bool:
    """Return True if the exchanges table exists and has at least one row."""
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'exchanges'"
        )
        if not cur.fetchone():
            return False
        cur.execute("SELECT 1 FROM exchanges LIMIT 1")
        return cur.fetchone() is not None
    finally:
        cur.close()


def upload_exchanges_reference_data(conn: psycopg2.extensions.connection) -> int:
    """
    One-off: upsert the static exchanges reference data into nasdaqtrader.exchanges.
    Returns number of rows upserted.
    """
    cur = conn.cursor()
    execute_values(
        cur,
        """
        INSERT INTO exchanges (symbol, exchange)
        VALUES %s
        ON CONFLICT (symbol) DO UPDATE SET exchange = EXCLUDED.exchange
        """,
        EXCHANGES_REFERENCE_DATA,
    )
    conn.commit()
    count = cur.rowcount
    cur.close()
    return count if count >= 0 else len(EXCHANGES_REFERENCE_DATA)


def _read_pipe_file(path: Path, skip_footer: bool = True) -> list[tuple]:
    """Read pipe-delimited file; first line = header. Optionally skip last line (trailer)."""
    lines = path.read_text(encoding="utf-8", errors="replace").strip().splitlines()
    if not lines:
        return []
    # Header
    header = [c.strip() for c in lines[0].split("|")]
    data_lines = lines[1:]
    if skip_footer and len(data_lines) > 0 and data_lines[-1].strip().startswith("File Creation"):
        data_lines = data_lines[:-1]
    rows = []
    for line in data_lines:
        parts = line.split("|")
        # Pad to header length
        while len(parts) < len(header):
            parts.append("")
        rows.append(tuple(p.strip() for p in parts[: len(header)]))
    return rows


def _safe_int(s: str) -> Optional[int]:
    if not s or not s.strip():
        return None
    try:
        return int(s.strip())
    except ValueError:
        return None


def upsert_nasdaqlisted(conn: psycopg2.extensions.connection, file_path: Path) -> int:
    """
    Upsert rows from nasdaqlisted.txt into nasdaqlisted.
    Returns number of rows upserted.
    """
    rows = _read_pipe_file(file_path)
    if not rows:
        return 0
    # Columns: Symbol, Security Name, Market Category, Test Issue, Financial Status, Round Lot Size, ETF, NextShares
    cur = conn.cursor()
    values = [(
        r[0], r[1], r[2], r[3], r[4],
        _safe_int(r[5]) if len(r) > 5 else None,
        r[6] if len(r) > 6 else None,
        r[7] if len(r) > 7 else None,
    ) for r in rows]
    execute_values(
        cur,
        """
        INSERT INTO nasdaqlisted (
            symbol, security_name, market_category, test_issue, financial_status,
            round_lot_size, etf, next_shares
        )
        VALUES %s
        ON CONFLICT (symbol) DO UPDATE SET
            security_name = EXCLUDED.security_name,
            market_category = EXCLUDED.market_category,
            test_issue = EXCLUDED.test_issue,
            financial_status = EXCLUDED.financial_status,
            round_lot_size = EXCLUDED.round_lot_size,
            etf = EXCLUDED.etf,
            next_shares = EXCLUDED.next_shares,
            upload_date = CURRENT_TIMESTAMP
        """,
        values,
        page_size=1000,
    )
    conn.commit()
    cur.close()
    return len(values)


def upsert_otherlisted(conn: psycopg2.extensions.connection, file_path: Path) -> int:
    """
    Upsert rows from otherlisted.txt into otherlisted.
    Returns number of rows upserted.
    """
    rows = _read_pipe_file(file_path)
    if not rows:
        return 0
    # ACT Symbol|Security Name|Exchange|CQS Symbol|ETF|Round Lot Size|Test Issue|NASDAQ Symbol
    cur = conn.cursor()
    values = [(
        r[0], r[2], r[1],  # act_symbol, exchange, security_name
        r[3] if len(r) > 3 else None,  # cqs_symbol
        r[4] if len(r) > 4 else None,  # etf
        _safe_int(r[5]) if len(r) > 5 else None,  # round_lot_size
        r[6] if len(r) > 6 else None,  # test_issue
        r[7] if len(r) > 7 else None,  # nasdaq_symbol
    ) for r in rows]
    execute_values(
        cur,
        """
        INSERT INTO otherlisted (
            act_symbol, exchange, security_name, cqs_symbol, etf,
            round_lot_size, test_issue, nasdaq_symbol
        )
        VALUES %s
        ON CONFLICT (act_symbol, exchange) DO UPDATE SET
            security_name = EXCLUDED.security_name,
            cqs_symbol = EXCLUDED.cqs_symbol,
            etf = EXCLUDED.etf,
            round_lot_size = EXCLUDED.round_lot_size,
            test_issue = EXCLUDED.test_issue,
            nasdaq_symbol = EXCLUDED.nasdaq_symbol,
            upload_date = CURRENT_TIMESTAMP
        """,
        values,
        page_size=1000,
    )
    conn.commit()
    cur.close()
    return len(values)


def upload_symbol_directory_to_db(
    output_dir: str | Path,
    *,
    load_env: bool = True,
    project_root: Optional[Path] = None,
) -> tuple[int, int]:
    """
    Upsert nasdaqlisted.txt and otherlisted.txt from output_dir into the nasdaqtrader DB.
    For use from other scripts (e.g. Airflow); not exposed as a CLI switch.

    Returns:
        (nasdaqlisted_count, otherlisted_count)
    """
    output_dir = Path(output_dir)
    nasdaq_path = output_dir / "nasdaqlisted.txt"
    other_path = output_dir / "otherlisted.txt"
    if not nasdaq_path.exists() or not other_path.exists():
        raise FileNotFoundError(
            f"Missing nasdaqlisted.txt or otherlisted.txt in {output_dir}"
        )

    if load_env:
        _project_root = project_root
        if _project_root is None:
            _project_root = Path(__file__).resolve().parent.parent.parent.parent
        try:
            from src.config import load_environment_config
            load_environment_config()
        except Exception:
            if not os.getenv("POSTGRES_PASSWORD"):
                from dotenv import load_dotenv
                load_dotenv(_project_root / ".env.dev", override=True)

    conn = get_postgres_connection()
    init_nasdaqtrader_postgres_tables(conn)
    n1 = upsert_nasdaqlisted(conn, nasdaq_path)
    n2 = upsert_otherlisted(conn, other_path)
    conn.close()
    return (n1, n2)
