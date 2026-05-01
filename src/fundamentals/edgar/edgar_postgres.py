"""
EDGAR PostgreSQL Database Management

This module handles all PostgreSQL operations for EDGAR data storage.
Uses the datalake database, edgar schema.
"""

import logging
import os
import re
from pathlib import Path
from typing import Dict, Optional, List, Any, Tuple
from datetime import datetime
import psycopg2
from tqdm import tqdm

logger = logging.getLogger(__name__)
from psycopg2.extras import execute_values, RealDictCursor
from psycopg2 import sql
import pandas as pd

from src.postgres_connection import get_datalake_connection

_SCHEMA = "edgar"


def get_module_dbname() -> str:
    """Schema name for this module (e.g. edgar). Used for backwards compatibility / display."""
    return _SCHEMA


def get_postgres_connection(dbname: Optional[str] = None, user: Optional[str] = None,
                            host: Optional[str] = None, password: Optional[str] = None,
                            port: Optional[int] = None) -> psycopg2.extensions.connection:
    """
    Get PostgreSQL connection to the datalake edgar schema.
    Uses ENV for host (postgres.{env}.local.info) and user ({env}.user); POSTGRES_PASSWORD, POSTGRES_PORT.
    """
    return get_datalake_connection(
        _SCHEMA,
        user=user,
        host=host,
        password=password,
        port=port,
    )


def populate_companies_exchange(
    conn: psycopg2.extensions.connection,
    json_path: Path,
) -> int:
    """
    Populate companies.exchange from SEC company_tickers_exchange.json.
    CIK is normalized to 10 digits. Returns number of rows updated.
    """
    import json
    path = Path(json_path)
    if not path.exists():
        raise FileNotFoundError(f"company_tickers_exchange.json not found: {path}")
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    # Structure: dict of { "0": {cik_str, ticker, title, exchange}, "1": {...}, ... }
    # or list of dicts
    records = []
    if isinstance(data, dict):
        for v in data.values():
            if isinstance(v, dict) and "cik_str" in v and v.get("exchange"):
                cik = str(v["cik_str"]).strip().zfill(10)
                records.append((cik, str(v["exchange"]).strip()))
    elif isinstance(data, list):
        for v in data:
            if isinstance(v, dict) and "cik_str" in v and v.get("exchange"):
                cik = str(v["cik_str"]).strip().zfill(10)
                records.append((cik, str(v["exchange"]).strip()))
    if not records:
        logger.warning("No exchange records found in %s", path)
        return 0
    cur = conn.cursor()
    try:
        updated = 0
        for cik, exchange in records:
            cur.execute(
                """
                INSERT INTO companies (cik, name, exchange, updated_at)
                VALUES (%s, 'Unknown', %s, CURRENT_TIMESTAMP)
                ON CONFLICT (cik) DO UPDATE SET exchange = EXCLUDED.exchange, updated_at = CURRENT_TIMESTAMP
                """,
                (cik, exchange),
            )
            updated += cur.rowcount if cur.rowcount else 1
        conn.commit()
        logger.info("Upserted exchange for %s companies from %s", len(records), path)
        return len(records)
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()


def init_edgar_postgres_tables(conn: psycopg2.extensions.connection) -> None:
    """
    Initialize PostgreSQL tables for EDGAR data if they don't exist
    
    Args:
        conn: PostgreSQL connection
    """
    logger.info("Initializing EDGAR PostgreSQL tables...")
    cur = conn.cursor()
    
    # Helper function to check if table exists
    def table_exists(table_name: str) -> bool:
        cur.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = %s
            )
        """, (table_name,))
        return cur.fetchone()[0]
    
    # Create companies table
    companies_existed = table_exists('companies')
    cur.execute("""
        CREATE TABLE IF NOT EXISTS companies (
            cik VARCHAR(10) PRIMARY KEY,
            ticker VARCHAR(20),
            name TEXT NOT NULL,
            sic_code VARCHAR(50),
            entity_type VARCHAR(50),
            exchange VARCHAR(50),
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    if not companies_existed:
        logger.info("Created companies table")
    # Add exchange column if table existed without it
    cur.execute("ALTER TABLE companies ADD COLUMN IF NOT EXISTS exchange VARCHAR(50)")
    
    # Create index on ticker for faster lookups
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_companies_ticker 
        ON companies(ticker)
    """)
    
    # Create index on sic_code
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_companies_sic_code 
        ON companies(sic_code)
    """)
    
    # Create company_history table (snapshot of companies at different times)
    company_history_existed = table_exists('company_history')
    cur.execute("""
        CREATE TABLE IF NOT EXISTS company_history (
            id SERIAL PRIMARY KEY,
            cik VARCHAR(10) NOT NULL,
            ticker VARCHAR(20),
            name TEXT NOT NULL,
            sic_code VARCHAR(50),
            entity_type VARCHAR(50),
            snapshot_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (cik) REFERENCES companies(cik) ON DELETE CASCADE
        )
    """)
    if not company_history_existed:
        logger.info("Created company_history table")
    
    # Create index on company_history
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_company_history_cik 
        ON company_history(cik)
    """)
    
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_company_history_snapshot_at 
        ON company_history(snapshot_at)
    """)
    
    # Create metadata table
    metadata_existed = table_exists('metadata')
    cur.execute("""
        CREATE TABLE IF NOT EXISTS metadata (
            key VARCHAR(255) PRIMARY KEY,
            value TEXT,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    if not metadata_existed:
        logger.info("Created metadata table")
    
    # Initialize metadata if empty
    cur.execute("SELECT COUNT(*) FROM metadata")
    if cur.fetchone()[0] == 0:
        now = datetime.now()
        initial_metadata = [
            ('generated_at', now.isoformat(), now),
            ('total_companies', '0', now),
            ('status', 'in_progress', now),
            ('source', 'SEC EDGAR API', now),
        ]
        execute_values(
            cur,
            "INSERT INTO metadata (key, value, updated_at) VALUES %s",
            initial_metadata
        )
    
    # Create master_idx_files table for storing parsed master.idx file data
    master_idx_existed = table_exists('master_idx_files')
    cur.execute("""
        CREATE TABLE IF NOT EXISTS master_idx_files (
            year INTEGER NOT NULL,
            quarter VARCHAR(10) NOT NULL,
            cik VARCHAR(10) NOT NULL,
            company_name TEXT NOT NULL,
            form_type VARCHAR(50) NOT NULL,
            date_filed DATE NOT NULL,
            filename TEXT NOT NULL,
            accession_number VARCHAR(50),
            downloaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (year, quarter, cik, form_type, date_filed, filename)
        )
    """)
    if not master_idx_existed:
        logger.info("Created master_idx_files table")
    
    # Create indexes for faster lookups
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_master_idx_files_year_quarter 
        ON master_idx_files(year, quarter)
    """)
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_master_idx_files_cik 
        ON master_idx_files(cik)
    """)
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_master_idx_files_form_type 
        ON master_idx_files(form_type)
    """)
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_master_idx_files_date_filed 
        ON master_idx_files(date_filed)
    """)
    
    # Create master_idx_download_ledger table for tracking download status
    ledger_existed = table_exists('master_idx_download_ledger')
    cur.execute("""
        CREATE TABLE IF NOT EXISTS master_idx_download_ledger (
            year INTEGER NOT NULL,
            quarter VARCHAR(10) NOT NULL,
            status VARCHAR(20) NOT NULL DEFAULT 'pending',
            downloaded_at TIMESTAMP,
            failed_at TIMESTAMP,
            error_message TEXT,
            retry_count INTEGER DEFAULT 0,
            last_attempt TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (year, quarter),
            CHECK (status IN ('pending', 'success', 'failed'))
        )
    """)
    if not ledger_existed:
        logger.info("Created master_idx_download_ledger table")
    
    # Create index for faster lookups
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_master_idx_ledger_status 
        ON master_idx_download_ledger(status)
    """)

    # Create filings_facts table for adj JSON (balance_sheet, income_stmt, etc.)
    filings_facts_existed = table_exists('filings_facts')
    cur.execute("""
        CREATE TABLE IF NOT EXISTS filings_facts (
            accession_code VARCHAR(50) NOT NULL,
            filing_type VARCHAR(20) NOT NULL,
            ticker VARCHAR(20),
            exchange VARCHAR(100),
            period_date DATE NOT NULL,
            aggregate_key VARCHAR(50) NOT NULL,
            subkey VARCHAR(500) NOT NULL,
            value TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (accession_code, period_date, aggregate_key, subkey)
        )
    """)
    if not filings_facts_existed:
        logger.info("Created filings_facts table")
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_filings_facts_accession 
        ON filings_facts(accession_code)
    """)
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_filings_facts_ticker 
        ON filings_facts(ticker)
    """)
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_filings_facts_period_date 
        ON filings_facts(period_date)
    """)

    # Create filings_facts_integrity_checks table
    integrity_existed = table_exists('filings_facts_integrity_checks')
    cur.execute("""
        CREATE TABLE IF NOT EXISTS filings_facts_integrity_checks (
            accession_code VARCHAR(50) PRIMARY KEY,
            form_type VARCHAR(20) NOT NULL,
            ticker VARCHAR(20),
            exchange VARCHAR(100),
            number_of_errors INTEGER NOT NULL DEFAULT 0,
            number_of_warnings INTEGER NOT NULL DEFAULT 0,
            small_cap_1B_filter BOOLEAN NOT NULL DEFAULT FALSE,
            mkt_cap_mln DOUBLE PRECISION,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    if not integrity_existed:
        logger.info("Created filings_facts_integrity_checks table")
    # Migrate: mkt_cap -> mkt_cap_mln if old column exists
    cur.execute("""
        SELECT column_name FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = 'filings_facts_integrity_checks'
    """)
    cols = {r[0] for r in cur.fetchall()}
    if "mkt_cap" in cols and "mkt_cap_mln" not in cols:
        cur.execute("ALTER TABLE filings_facts_integrity_checks DROP COLUMN mkt_cap")
        cur.execute("ALTER TABLE filings_facts_integrity_checks ADD COLUMN mkt_cap_mln DOUBLE PRECISION")
        logger.info("Migrated filings_facts_integrity_checks: mkt_cap -> mkt_cap_mln")
    elif "mkt_cap" in cols and "mkt_cap_mln" in cols:
        cur.execute("ALTER TABLE filings_facts_integrity_checks DROP COLUMN mkt_cap")
        logger.info("Dropped legacy mkt_cap column from filings_facts_integrity_checks")
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_filings_facts_integrity_ticker
        ON filings_facts_integrity_checks(ticker)
    """)
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_filings_facts_integrity_small_cap
        ON filings_facts_integrity_checks(small_cap_1B_filter)
    """)

    # filings_facts_integrity_checks_errors: one row per scrape error
    cur.execute("""
        CREATE TABLE IF NOT EXISTS filings_facts_integrity_checks_errors (
            accession_code VARCHAR(50) NOT NULL,
            error TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (accession_code, error)
        )
    """)
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_filings_integrity_errors_accession
        ON filings_facts_integrity_checks_errors(accession_code)
    """)

    # filings_facts_integrity_checks_warnings: one row per warning
    cur.execute("""
        CREATE TABLE IF NOT EXISTS filings_facts_integrity_checks_warnings (
            accession_code VARCHAR(50) NOT NULL,
            warning TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (accession_code, warning)
        )
    """)
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_filings_integrity_warnings_accession
        ON filings_facts_integrity_checks_warnings(accession_code)
    """)
    
    # Verify all tables were created before committing
    cur.execute("""
        SELECT table_name 
        FROM information_schema.tables 
                WHERE table_schema = 'public'
        AND table_name IN ('companies', 'company_history', 'metadata', 'master_idx_files', 'master_idx_download_ledger', 'filings_facts', 'filings_facts_integrity_checks', 'filings_facts_integrity_checks_errors', 'filings_facts_integrity_checks_warnings')
        ORDER BY table_name
    """)
    created_tables = [row[0] for row in cur.fetchall()]
    logger.info("Database tables initialized: %s", ", ".join(created_tables))
    
    conn.commit()
    cur.close()
    logger.info("Database initialization complete")


def init_master_idx_tables(conn: psycopg2.extensions.connection) -> None:
    """
    Initialize PostgreSQL tables for master.idx file operations only.
    This is a focused initialization that only creates the tables needed for master_idx DAG.
    
    Args:
        conn: PostgreSQL connection
    """
    logger.info("Initializing master.idx PostgreSQL tables...")
    cur = conn.cursor()
    
    # Helper function to check if table exists
    def table_exists(table_name: str) -> bool:
        cur.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = %s
            )
        """, (table_name,))
        return cur.fetchone()[0]
    
    # Create master_idx_files table for storing parsed master.idx file data
    master_idx_existed = table_exists('master_idx_files')
    cur.execute("""
        CREATE TABLE IF NOT EXISTS master_idx_files (
            year INTEGER NOT NULL,
            quarter VARCHAR(10) NOT NULL,
            cik VARCHAR(10) NOT NULL,
            company_name TEXT NOT NULL,
            form_type VARCHAR(50) NOT NULL,
            date_filed DATE NOT NULL,
            filename TEXT NOT NULL,
            accession_number VARCHAR(50),
            downloaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (year, quarter, cik, form_type, date_filed, filename)
        )
    """)
    if not master_idx_existed:
        logger.info("Created master_idx_files table")
    
    # Create indexes for faster lookups
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_master_idx_files_year_quarter 
        ON master_idx_files(year, quarter)
    """)
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_master_idx_files_cik 
        ON master_idx_files(cik)
    """)
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_master_idx_files_form_type 
        ON master_idx_files(form_type)
    """)
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_master_idx_files_date_filed 
        ON master_idx_files(date_filed)
    """)
    
    # Create master_idx_download_ledger table for tracking download status
    ledger_existed = table_exists('master_idx_download_ledger')
    cur.execute("""
        CREATE TABLE IF NOT EXISTS master_idx_download_ledger (
            year INTEGER NOT NULL,
            quarter VARCHAR(10) NOT NULL,
            status VARCHAR(20) NOT NULL DEFAULT 'pending',
            downloaded_at TIMESTAMP,
            failed_at TIMESTAMP,
            error_message TEXT,
            retry_count INTEGER DEFAULT 0,
            last_attempt TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (year, quarter),
            CHECK (status IN ('pending', 'success', 'failed'))
        )
    """)
    if not ledger_existed:
        logger.info("Created master_idx_download_ledger table")
    
    # Create index for faster lookups
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_master_idx_ledger_status 
        ON master_idx_download_ledger(status)
    """)
    
    conn.commit()
    cur.close()
    logger.info("Master.idx database initialization complete")


def upload_adj_json_to_filings_facts(
    conn: psycopg2.extensions.connection,
    json_path: Path,
    accession_code: str,
    filing_type: str,
) -> int:
    """
    Load adj JSON into filings_facts table.
    Flattens { date: { balance_sheet: { subkey: value }, ... } } into rows.
    Skips minimal adj (ticker=dei:NoTradingSymbolFlag, no period data).
    Returns number of rows inserted/upserted.
    """
    import json
    path = Path(json_path)
    if not path.exists():
        raise FileNotFoundError(f"Adj JSON not found: {path}")
    data = json.loads(path.read_text(encoding="utf-8"))
    ticker = data.get("ticker") or ""
    exchange = data.get("exchange") or ""
    if ticker == "dei:NoTradingSymbolFlag" or not ticker:
        ticker = ticker or "N/A"
    period_keys = [k for k in data if k not in ("ticker", "exchange")]
    if not period_keys:
        return 0

    AGGREGATE_KEYS = ("balance_sheet", "income_stmt", "cashflow", "other", "shares")
    rows: List[tuple] = []
    for period_key in period_keys:
        inner = data.get(period_key)
        if not isinstance(inner, dict):
            continue
        try:
            if "T" in period_key:
                period_date = datetime.fromisoformat(period_key.replace("Z", "+00:00")).date()
            else:
                period_date = datetime.strptime(period_key[:10], "%Y-%m-%d").date()
        except (ValueError, TypeError):
            continue
        for agg_key in AGGREGATE_KEYS:
            agg_data = inner.get(agg_key)
            if not isinstance(agg_data, dict):
                continue
            for subkey, val in agg_data.items():
                if val is None:
                    val_str = None
                else:
                    val_str = str(val)
                rows.append((accession_code, filing_type, ticker, exchange, period_date, agg_key, subkey, val_str))

    if not rows:
        return 0

    cur = conn.cursor()
    try:
        execute_values(
            cur,
            """
            INSERT INTO filings_facts (accession_code, filing_type, ticker, exchange, period_date, aggregate_key, subkey, value)
            VALUES %s
            ON CONFLICT (accession_code, period_date, aggregate_key, subkey)
            DO UPDATE SET filing_type = EXCLUDED.filing_type, ticker = EXCLUDED.ticker, exchange = EXCLUDED.exchange, value = EXCLUDED.value
            """,
            rows,
            template="(%s, %s, %s, %s, %s, %s, %s, %s)",
        )
        conn.commit()
        return len(rows)
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()


def _mkt_cap_str_to_mln(s: str) -> Optional[float]:
    """Parse mkt_cap string (e.g. $17.1M, $1.2B, $1.2K) to float in millions. Returns None if unparseable."""
    if not s or s.strip() in ("N/A", ""):
        return None
    s = s.strip().replace(",", "")
    m = re.match(r"^\$?([\d.]+)\s*([KMB])?$", s, re.IGNORECASE)
    if not m:
        return None
    try:
        val = float(m.group(1))
    except ValueError:
        return None
    suffix = (m.group(2) or "M").upper()
    if suffix == "K":
        return val / 1000.0
    if suffix == "M":
        return val
    if suffix == "B":
        return val * 1000.0
    return val


def _parse_integrity_logs(logs_dir: Path) -> Dict[str, Dict[str, Any]]:
    """
    Parse check_failures.log, check_warnings.log, scrape_errors.log from logs_dir.
    Returns dict: accession -> {ticker, exchange, number_of_errors, number_of_warnings, small_cap_1B_filter, mkt_cap_mln}
    """
    result: Dict[str, Dict[str, Any]] = {}

    def _get_or_init(acc: str) -> Dict[str, Any]:
        if acc not in result:
            result[acc] = {
                "ticker": None,
                "exchange": None,
                "number_of_errors": 0,
                "number_of_warnings": 0,
                "small_cap_1B_filter": False,
                "mkt_cap_mln": None,
            }
        return result[acc]

    # Parse check_failures.log
    cf_path = logs_dir / "check_failures.log"
    if cf_path.exists():
        in_small_cap = False
        # Main with mkt_cap: accession ticker=X exchange=Y mkt_cap=Z: N/14 failed
        main_with_mcap_re = re.compile(
            r"^(\S+)\s+ticker=(\S+)\s+exchange=(.+?)\s+mkt_cap=([^:]+):\s*(\d+)/\d+\s+failed\s*$"
        )
        # Main without mkt_cap: accession ticker=X exchange=Y: N/14 failed
        main_no_mcap_re = re.compile(
            r"^(\S+)\s+ticker=(\S+)\s+exchange=([^:]+):\s*(\d+)/\d+\s+failed\s*$"
        )
        # Small-cap: accession ticker=X exchange=Y: mkt_cap=Z (N/14 failed)
        small_re = re.compile(
            r"^(\S+)\s+ticker=(\S+)\s+exchange=([^:]+):\s*mkt_cap=([^\s]+)\s+\((\d+)/\d+\s+failed\)\s*$"
        )
        with open(cf_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.rstrip()
                if "--- Check failures with market cap < $1B ---" in line:
                    in_small_cap = True
                    continue
                if in_small_cap:
                    m = small_re.match(line)
                    if m:
                        acc, ticker, exchange, mkt_cap, _ = m.groups()
                        d = _get_or_init(acc)
                        d["ticker"] = ticker if ticker != "N/A" else None
                        d["exchange"] = exchange.strip() if exchange.strip() != "N/A" else None
                        d["mkt_cap_mln"] = _mkt_cap_str_to_mln(mkt_cap)
                        d["small_cap_1B_filter"] = True
                else:
                    m = main_with_mcap_re.match(line)
                    if m:
                        acc, ticker, exchange, mkt_cap, _ = m.groups()
                        d = _get_or_init(acc)
                        d["ticker"] = ticker if ticker != "N/A" else None
                        d["exchange"] = exchange.strip() if exchange.strip() != "N/A" else None
                        mc = _mkt_cap_str_to_mln(mkt_cap)
                        if mc is not None:
                            d["mkt_cap_mln"] = mc
                    else:
                        m = main_no_mcap_re.match(line)
                        if m:
                            acc, ticker, exchange, _ = m.groups()
                            d = _get_or_init(acc)
                            d["ticker"] = ticker if ticker != "N/A" else None
                            d["exchange"] = exchange.strip() if exchange.strip() != "N/A" else None

    # Parse check_warnings.log: accession: dei:NoTradingSymbolFlag (no ticker)
    cw_path = logs_dir / "check_warnings.log"
    if cw_path.exists():
        warn_re = re.compile(r"^(\S+):\s+dei:NoTradingSymbolFlag")
        with open(cw_path, "r", encoding="utf-8") as f:
            for line in f:
                m = warn_re.match(line.strip())
                if m:
                    acc = m.group(1)
                    d = _get_or_init(acc)
                    d["number_of_warnings"] += 1

    # Parse scrape_errors.log: accession [stage]: msg
    se_path = logs_dir / "scrape_errors.log"
    if se_path.exists():
        err_re = re.compile(r"^(\S+)\s+\[")
        with open(se_path, "r", encoding="utf-8") as f:
            for line in f:
                m = err_re.match(line.strip())
                if m:
                    acc = m.group(1)
                    d = _get_or_init(acc)
                    d["number_of_errors"] += 1

    return result


def _parse_errors_and_warnings(logs_dir: Path) -> Tuple[List[Tuple[str, str]], List[Tuple[str, str]]]:
    """
    Parse scrape_errors.log and check_warnings.log for detail upload.
    Returns (errors_list, warnings_list) where each is [(accession, text), ...].
    """
    errors: List[Tuple[str, str]] = []
    warnings: List[Tuple[str, str]] = []

    # scrape_errors.log: accession [stage]: msg
    se_path = logs_dir / "scrape_errors.log"
    if se_path.exists():
        err_re = re.compile(r"^(\S+)\s+\[([^\]]+)\]:\s*(.*)$")
        with open(se_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.rstrip()
                m = err_re.match(line)
                if m:
                    acc, stage, msg = m.groups()
                    error_text = f"[{stage}]: {msg.strip()}" if msg else f"[{stage}]"
                    errors.append((acc, error_text))

    # check_warnings.log: accession: warning_text
    cw_path = logs_dir / "check_warnings.log"
    if cw_path.exists():
        warn_re = re.compile(r"^(\S+):\s+(.+)$")
        with open(cw_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.rstrip()
                m = warn_re.match(line)
                if m:
                    acc, warning_text = m.groups()
                    warning_text = warning_text.strip()
                    if warning_text:
                        warnings.append((acc, warning_text))

    return errors, warnings


def upload_integrity_errors_from_logs(
    conn: psycopg2.extensions.connection,
    logs_dir: Path,
) -> int:
    """Upload scrape errors to filings_facts_integrity_checks_errors. Returns rows upserted."""
    errors, _ = _parse_errors_and_warnings(logs_dir)
    if not errors:
        return 0
    cur = conn.cursor()
    BATCH_SIZE = 200
    insert_sql = """
        INSERT INTO filings_facts_integrity_checks_errors (accession_code, error)
        VALUES %s ON CONFLICT (accession_code, error) DO NOTHING
    """
    try:
        for i in tqdm(range(0, len(errors), BATCH_SIZE), desc="Uploading filings_facts_integrity_checks_errors",
                      unit="batch", total=(len(errors) + BATCH_SIZE - 1) // BATCH_SIZE, dynamic_ncols=True):
            batch = errors[i:i + BATCH_SIZE]
            execute_values(cur, insert_sql, batch, template="(%s, %s)")
        conn.commit()
        logger.info("Upserted %s rows into filings_facts_integrity_checks_errors", len(errors))
        return len(errors)
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()


def upload_integrity_warnings_from_logs(
    conn: psycopg2.extensions.connection,
    logs_dir: Path,
) -> int:
    """Upload check warnings to filings_facts_integrity_checks_warnings. Returns rows upserted."""
    _, warnings = _parse_errors_and_warnings(logs_dir)
    if not warnings:
        return 0
    cur = conn.cursor()
    BATCH_SIZE = 200
    insert_sql = """
        INSERT INTO filings_facts_integrity_checks_warnings (accession_code, warning)
        VALUES %s ON CONFLICT (accession_code, warning) DO NOTHING
    """
    try:
        for i in tqdm(range(0, len(warnings), BATCH_SIZE), desc="Uploading filings_facts_integrity_checks_warnings",
                      unit="batch", total=(len(warnings) + BATCH_SIZE - 1) // BATCH_SIZE, dynamic_ncols=True):
            batch = warnings[i:i + BATCH_SIZE]
            execute_values(cur, insert_sql, batch, template="(%s, %s)")
        conn.commit()
        logger.info("Upserted %s rows into filings_facts_integrity_checks_warnings", len(warnings))
        return len(warnings)
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()


def upload_integrity_checks_from_logs(
    conn: psycopg2.extensions.connection,
    logs_dir: Path,
    input_dir: Path,
    form_type: str = "10-Q",
) -> int:
    """
    Parse integrity logs, merge with ticker/exchange from adj JSON where missing,
    and upsert into filings_facts_integrity_checks.
    Returns number of rows upserted.
    """
    data = _parse_integrity_logs(logs_dir)
    if not data:
        logger.info("No integrity log data to upload")
        return 0

    # Enrich with ticker/exchange from adj JSON for accessions not in check_failures
    for jp in input_dir.glob("*.json"):
        if jp.name.endswith(".xbrl.json") or jp.name.endswith(".html.json"):
            continue
        acc = jp.stem
        if acc not in data:
            data[acc] = {
                "ticker": None,
                "exchange": None,
                "number_of_errors": 0,
                "number_of_warnings": 0,
                "small_cap_1B_filter": False,
                "mkt_cap_mln": None,
            }
        d = data[acc]
        if d["ticker"] is None and d["exchange"] is None:
            try:
                import json
                adj = json.loads(jp.read_text(encoding="utf-8"))
                t = adj.get("ticker") or None
                e = adj.get("exchange") or None
                if t and t != "dei:NoTradingSymbolFlag":
                    d["ticker"] = t
                d["exchange"] = e
            except Exception:
                pass

    rows = []
    for acc, d in data.items():
        ticker = d["ticker"]
        exchange = d["exchange"]
        if ticker == "N/A":
            ticker = None
        if exchange == "NONE" or exchange == "N/A":
            exchange = None
        rows.append((
            acc,
            form_type,
            ticker,
            exchange,
            d["number_of_errors"],
            d["number_of_warnings"],
            d["small_cap_1B_filter"],
            d["mkt_cap_mln"],
        ))

    cur = conn.cursor()
    BATCH_SIZE = 200
    insert_sql = """
        INSERT INTO filings_facts_integrity_checks
            (accession_code, form_type, ticker, exchange, number_of_errors, number_of_warnings, small_cap_1B_filter, mkt_cap_mln)
        VALUES %s
        ON CONFLICT (accession_code) DO UPDATE SET
            form_type = EXCLUDED.form_type,
            ticker = EXCLUDED.ticker,
            exchange = EXCLUDED.exchange,
            number_of_errors = EXCLUDED.number_of_errors,
            number_of_warnings = EXCLUDED.number_of_warnings,
            small_cap_1B_filter = EXCLUDED.small_cap_1B_filter,
            mkt_cap_mln = EXCLUDED.mkt_cap_mln
    """
    try:
        for i in tqdm(range(0, len(rows), BATCH_SIZE), desc="Uploading filings_facts_integrity_checks",
                      unit="batch", total=(len(rows) + BATCH_SIZE - 1) // BATCH_SIZE, dynamic_ncols=True):
            batch = rows[i:i + BATCH_SIZE]
            execute_values(cur, insert_sql, batch, template="(%s, %s, %s, %s, %s, %s, %s, %s)")
        conn.commit()
        logger.info("Upserted %s rows into filings_facts_integrity_checks", len(rows))
        return len(rows)
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()


def upload_adj_jsons_from_dir(
    conn: psycopg2.extensions.connection,
    input_dir: Path,
    filing_type: str = "10-Q",
) -> int:
    """
    Upload all accession.json files from a directory to filings_facts,
    then upload integrity checks from logs (check_failures, check_warnings, scrape_errors)
    in input_dir.parent.
    Returns total rows inserted into filings_facts.
    """
    logger.info("Uploading to filings_facts from %s", input_dir)
    json_files = [jp for jp in sorted(input_dir.glob("*.json"))
                  if not (jp.name.endswith(".xbrl.json") or jp.name.endswith(".html.json"))]
    total = 0
    for jp in tqdm(json_files, desc="Uploading filings_facts", unit="file", dynamic_ncols=True):
        accession = jp.stem
        try:
            n = upload_adj_json_to_filings_facts(conn, jp, accession, filing_type)
            total += n
            if n > 0:
                logger.debug("Uploaded %s: %s rows", jp.name, n)
        except Exception as e:
            logger.warning("Failed to upload %s: %s", jp.name, e)
    logger.info("Uploaded %s rows to filings_facts", total)
    logs_dir = input_dir.parent
    try:
        logger.info("Uploading to filings_facts_integrity_checks from %s", logs_dir)
        upload_integrity_checks_from_logs(conn, logs_dir, input_dir, form_type=filing_type)
        logger.info("Uploading to filings_facts_integrity_checks_errors from %s", logs_dir)
        upload_integrity_errors_from_logs(conn, logs_dir)
        logger.info("Uploading to filings_facts_integrity_checks_warnings from %s", logs_dir)
        upload_integrity_warnings_from_logs(conn, logs_dir)
    except Exception as e:
        logger.warning("Failed to upload integrity checks from logs: %s", e)
    return total


