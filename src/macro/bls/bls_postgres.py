"""
BLS PostgreSQL Database Management

This module handles all PostgreSQL operations for BLS data storage.
Uses datalake database, bls schema.
"""

import os
import logging
from typing import Dict, Optional, List
from datetime import datetime
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values, RealDictCursor
from psycopg2 import sql

try:
    from src.postgres_connection import get_datalake_connection
except ImportError:
    # Support direct script execution from src/macro/bls/main.py
    import sys
    from pathlib import Path
    project_root = Path(__file__).resolve().parent.parent.parent.parent
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))
    from src.postgres_connection import get_datalake_connection

_SCHEMA = "bls"
logger = logging.getLogger(__name__)


def get_postgres_connection(dbname: str = "bls", user: Optional[str] = None,
                            host: Optional[str] = None, password: Optional[str] = None,
                            port: Optional[int] = None) -> psycopg2.extensions.connection:
    """
    Get PostgreSQL connection to the datalake bls schema.
    Uses ENV for host (postgres.{env}.local.info) and user ({env}.user); POSTGRES_PASSWORD, POSTGRES_PORT.
    """
    return get_datalake_connection(
        _SCHEMA,
        user=user,
        host=host,
        password=password,
        port=port,
    )


def init_bls_postgres_tables(conn: psycopg2.extensions.connection) -> None:
    """
    Validate PostgreSQL tables for BLS data exist (no runtime schema changes).
    
    Args:
        conn: PostgreSQL connection
    """
    cur = conn.cursor()

    required_tables = ['series', 'metadata', 'time_series', 'survey_sync_status']
    for table in required_tables:
        cur.execute(
            """
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = %s AND table_name = %s
            """,
            (_SCHEMA, table),
        )
        if cur.fetchone() is None:
            raise RuntimeError(
                f"Missing required table {_SCHEMA}.{table}. "
                "BLS schema must be provisioned ahead of time."
            )

    # Initialize metadata if empty (DML only).
    cur.execute("SELECT COUNT(*) FROM metadata")
    if cur.fetchone()[0] == 0:
        now = datetime.now()
        initial_metadata = [
            ('generated_at', now.isoformat(), now),
            ('total_series', '0', now),
            ('status', 'in_progress', now),
            ('source', 'BLS API', now),
        ]
        execute_values(
            cur,
            "INSERT INTO metadata (key, value, updated_at) VALUES %s",
            initial_metadata,
        )

    conn.commit()
    cur.close()


def provision_bls_postgres_schema(conn: psycopg2.extensions.connection) -> None:
    """
    Provision PostgreSQL tables for BLS data (explicit operation; contains DDL).

    This must be invoked explicitly (e.g. via a dedicated CLI flag) to avoid
    runtime schema changes during normal catalogue downloads.
    """
    cur = conn.cursor()

    # Create series table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS series (
            series_id VARCHAR(255) PRIMARY KEY,
            survey_abbreviation VARCHAR(50),
            survey_name TEXT,
            seasonal VARCHAR(50),
            area_code VARCHAR(50),
            area_name VARCHAR(255),
            item_code VARCHAR(50),
            item_name TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Create index on survey_abbreviation for faster queries
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_series_survey_abbreviation
        ON series(survey_abbreviation)
    """)

    # Create metadata table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS metadata (
            key VARCHAR(255) PRIMARY KEY,
            value TEXT,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Create time_series table for storing actual time series data
    cur.execute("""
        CREATE TABLE IF NOT EXISTS time_series (
            series_id VARCHAR(255) NOT NULL,
            date DATE NOT NULL,
            value DOUBLE PRECISION,
            year VARCHAR(10),
            period VARCHAR(10),
            footnotes TEXT,
            PRIMARY KEY (series_id, date),
            FOREIGN KEY (series_id) REFERENCES series(series_id) ON DELETE CASCADE
        )
    """)

    # Create index on date for faster queries
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_time_series_date
        ON time_series(date)
    """)

    # Create index on series_id for faster lookups
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_time_series_series_id
        ON time_series(series_id)
    """)

    # Track per-survey catalog sync status
    cur.execute("""
        CREATE TABLE IF NOT EXISTS survey_sync_status (
            survey_abbreviation VARCHAR(50) PRIMARY KEY,
            survey_name TEXT,
            status VARCHAR(50),
            rows_loaded INTEGER DEFAULT 0,
            last_error TEXT,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Initialize metadata if empty
    cur.execute("SELECT COUNT(*) FROM metadata")
    if cur.fetchone()[0] == 0:
        now = datetime.now()
        initial_metadata = [
            ('generated_at', now.isoformat(), now),
            ('total_series', '0', now),
            ('status', 'in_progress', now),
            ('source', 'BLS API', now),
        ]
        execute_values(
            cur,
            "INSERT INTO metadata (key, value, updated_at) VALUES %s",
            initial_metadata,
        )

    conn.commit()
    cur.close()


def ensure_bls_schema(conn: psycopg2.extensions.connection) -> None:
    """
    Ensure BLS tables exist (provisions schema if missing).

    Note: This contains DDL and should only be called from an explicitly invoked
    operation (e.g. CLI flags like --generate-catalog), not during import/runtime
    background work.
    """
    try:
        init_bls_postgres_tables(conn)
    except RuntimeError as e:
        msg = str(e)
        if "Missing required table" in msg:
            logger.info("BLS schema missing; provisioning tables.")
            provision_bls_postgres_schema(conn)
            return
        raise


def add_bls_series_fast(conn: psycopg2.extensions.connection, new_series: List[Dict]) -> int:
    """
    Add BLS series to PostgreSQL database (upsert - insert or update if exists)
    
    Args:
        conn: PostgreSQL connection
        new_series: List of series dictionaries to add
    
    Returns:
        Number of series successfully added/updated
    """
    if not new_series:
        return 0
    
    cur = conn.cursor()
    
    # Prepare data for bulk insert
    columns = ['series_id', 'survey_abbreviation', 'survey_name', 'seasonal',
               'area_code', 'area_name', 'item_code', 'item_name', 'created_at', 'updated_at']
    
    # Ensure all required fields exist
    for series in new_series:
        for field in columns:
            if field not in series:
                if field in ['created_at', 'updated_at']:
                    series[field] = datetime.now()
                else:
                    series[field] = ''
    
    # Prepare values for upsert
    values = []
    for series in new_series:
        row = tuple(
            str(series.get(col, '')) if col not in ['created_at', 'updated_at'] 
            else (datetime.fromisoformat(series[col]) if isinstance(series.get(col), str) 
                  else series.get(col, datetime.now()))
            for col in columns
        )
        values.append(row)
    
    # Use INSERT ... ON CONFLICT to upsert
    insert_query = sql.SQL("""
        INSERT INTO series ({columns})
        VALUES %s
        ON CONFLICT (series_id) 
        DO UPDATE SET
            survey_abbreviation = EXCLUDED.survey_abbreviation,
            survey_name = EXCLUDED.survey_name,
            seasonal = EXCLUDED.seasonal,
            area_code = EXCLUDED.area_code,
            area_name = EXCLUDED.area_name,
            item_code = EXCLUDED.item_code,
            item_name = EXCLUDED.item_name,
            updated_at = EXCLUDED.updated_at
    """).format(
        columns=sql.SQL(', ').join(map(sql.Identifier, columns))
    )
    
    execute_values(cur, insert_query, values)
    conn.commit()
    
    added_count = cur.rowcount
    cur.close()
    
    return added_count


def load_bls_series_from_postgres(conn: psycopg2.extensions.connection) -> List[Dict]:
    """
    Load BLS series from PostgreSQL database
    
    Args:
        conn: PostgreSQL connection
    
    Returns:
        List of series dictionaries
    """
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("SELECT * FROM series")
    results = cur.fetchall()
    cur.close()
    
    # Convert to list of dictionaries, converting timestamps to strings
    series_list = []
    for row in results:
        series_dict = dict(row)
        # Convert timestamps to ISO format strings
        for key in ['created_at', 'updated_at']:
            if series_dict.get(key) and isinstance(series_dict[key], datetime):
                series_dict[key] = series_dict[key].isoformat()
        series_list.append(series_dict)
    
    return series_list


def load_survey_sync_status(conn: psycopg2.extensions.connection) -> Dict[str, Dict]:
    """
    Load per-survey sync status rows keyed by survey abbreviation.

    Args:
        conn: PostgreSQL connection

    Returns:
        Dict keyed by survey_abbreviation with status details.
    """
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute(
        """
        SELECT survey_abbreviation, survey_name, status, rows_loaded, last_error, updated_at
        FROM survey_sync_status
        """
    )
    rows = cur.fetchall()
    cur.close()

    status_map: Dict[str, Dict] = {}
    for row in rows:
        row_dict = dict(row)
        key = str(row_dict.get("survey_abbreviation", "")).strip().upper()
        if not key:
            continue
        status_map[key] = row_dict
    return status_map


def update_bls_metadata(conn: psycopg2.extensions.connection, key: str, value: str) -> None:
    """
    Update metadata value in PostgreSQL database
    
    Args:
        conn: PostgreSQL connection
        key: Metadata key
        value: Metadata value
    """
    cur = conn.cursor()
    
    cur.execute("""
        INSERT INTO metadata (key, value, updated_at)
        VALUES (%s, %s, %s)
        ON CONFLICT (key)
        DO UPDATE SET value = EXCLUDED.value, updated_at = EXCLUDED.updated_at
    """, (key, value, datetime.now()))
    
    conn.commit()
    cur.close()


def upsert_survey_sync_status(
    conn: psycopg2.extensions.connection,
    survey_abbreviation: str,
    survey_name: str,
    status: str,
    rows_loaded: int = 0,
    last_error: Optional[str] = None,
) -> None:
    """
    Upsert survey sync status row with updated_at timestamp.

    Args:
        conn: PostgreSQL connection
        survey_abbreviation: Survey code (e.g., CU, CE)
        survey_name: Human-readable survey name
        status: Sync status (e.g., in_progress, complete, failed)
        rows_loaded: Number of rows loaded for this survey in current run
        last_error: Optional error message when status is failed
    """
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO survey_sync_status (
            survey_abbreviation, survey_name, status, rows_loaded, last_error, updated_at
        )
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (survey_abbreviation)
        DO UPDATE SET
            survey_name = EXCLUDED.survey_name,
            status = EXCLUDED.status,
            rows_loaded = EXCLUDED.rows_loaded,
            last_error = EXCLUDED.last_error,
            updated_at = EXCLUDED.updated_at
        """,
        (
            survey_abbreviation,
            survey_name,
            status,
            rows_loaded,
            (last_error[:4000] if last_error else None),
            datetime.now(),
        ),
    )
    conn.commit()
    cur.close()


def get_bls_metadata(conn: psycopg2.extensions.connection, key: Optional[str] = None) -> Dict[str, str]:
    """
    Get metadata from PostgreSQL database
    
    Args:
        conn: PostgreSQL connection
        key: Optional specific key to retrieve (if None, returns all metadata)
    
    Returns:
        Dictionary of metadata key-value pairs (or single value if key specified)
    """
    cur = conn.cursor()
    
    if key:
        cur.execute("SELECT value FROM metadata WHERE key = %s", (key,))
        result = cur.fetchone()
        cur.close()
        return {key: result[0]} if result else {}
    else:
        cur.execute("SELECT key, value FROM metadata")
        results = cur.fetchall()
        cur.close()
        return dict(results)


def add_time_series_fast(conn, time_series_list: List[Dict]) -> int:
    """
    Fast batch insert of time series data
    
    Args:
        conn: PostgreSQL connection
        time_series_list: List of dicts with keys: series_id, date, value, year, period, footnotes
        
    Returns:
        Number of records inserted (excluding duplicates)
    """
    if not time_series_list:
        return 0
    
    cur = conn.cursor()
    
    try:
        # Prepare data for batch insert
        columns = ['series_id', 'date', 'value', 'year', 'period', 'footnotes']
        values = []
        for ts in time_series_list:
            # Convert footnotes list to string if needed
            footnotes = ts.get('footnotes', '')
            if isinstance(footnotes, list):
                footnotes = ', '.join(str(f) for f in footnotes)
            
            row = (
                str(ts.get('series_id', '')),
                ts.get('date'),  # Should be date string or date object
                ts.get('value') if ts.get('value') is not None else None,
                str(ts.get('year', '')),
                str(ts.get('period', '')),
                str(footnotes) if footnotes else None
            )
            values.append(row)
        
        # Use INSERT ... ON CONFLICT to upsert (update if exists)
        insert_query = sql.SQL("""
            INSERT INTO time_series ({columns})
            VALUES %s
            ON CONFLICT (series_id, date) 
            DO UPDATE SET
                value = EXCLUDED.value,
                year = EXCLUDED.year,
                period = EXCLUDED.period,
                footnotes = EXCLUDED.footnotes
        """).format(
            columns=sql.SQL(', ').join(map(sql.Identifier, columns))
        )
        
        execute_values(cur, insert_query, values)
        conn.commit()
        
        added = len(values)
        cur.close()
        return added
        
    except Exception as e:
        conn.rollback()
        cur.close()
        raise e


def load_time_series_from_postgres(conn, series_id: Optional[str] = None, 
                                   start_date: Optional[str] = None,
                                   end_date: Optional[str] = None) -> List[Dict]:
    """
    Load time series data from PostgreSQL
    
    Args:
        conn: PostgreSQL connection
        series_id: Optional series ID to filter by
        start_date: Optional start date (YYYY-MM-DD format)
        end_date: Optional end date (YYYY-MM-DD format)
        
    Returns:
        List of dicts with keys: series_id, date, value, year, period, footnotes
    """
    cur = conn.cursor(cursor_factory=RealDictCursor)
    
    query = "SELECT series_id, date, value, year, period, footnotes FROM time_series WHERE 1=1"
    params = []
    
    if series_id:
        query += " AND series_id = %s"
        params.append(str(series_id))
    
    if start_date:
        query += " AND date >= %s"
        params.append(start_date)
    
    if end_date:
        query += " AND date <= %s"
        params.append(end_date)
    
    query += " ORDER BY series_id, date"
    
    cur.execute(query, params)
    results = cur.fetchall()
    cur.close()
    
    return [dict(row) for row in results]

