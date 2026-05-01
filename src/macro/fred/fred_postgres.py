"""
FRED PostgreSQL Database Management

This module handles all PostgreSQL operations for FRED data storage.
Uses datalake database, fred schema.
"""

import os
from pathlib import Path
from typing import Dict, Optional, List
from datetime import datetime
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values, RealDictCursor
from psycopg2 import sql

from src.postgres_connection import get_datalake_connection

_SCHEMA = "fred"


def get_postgres_connection(dbname: Optional[str] = None, user: Optional[str] = None,
                            host: Optional[str] = None, password: Optional[str] = None,
                            port: Optional[int] = None) -> psycopg2.extensions.connection:
    """
    Get PostgreSQL connection to the datalake fred schema.
    Uses ENV for host (postgres.{env}.local.info) and user ({env}.user).
    Requires POSTGRES_PASSWORD; POSTGRES_PORT optional (default 5432).
    """
    password = password if password is not None else os.getenv("POSTGRES_PASSWORD")
    if password is None:
        raise ValueError("POSTGRES_PASSWORD is required (set in .env or environment)")
    return get_datalake_connection(
        _SCHEMA,
        user=user,
        host=host,
        password=password,
        port=port,
    )


def init_fred_postgres_tables(conn: psycopg2.extensions.connection) -> None:
    """
    Initialize PostgreSQL tables for FRED data if they don't exist
    
    Args:
        conn: PostgreSQL connection
    """
    cur = conn.cursor()
    
    # Create series table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS series (
            series_id VARCHAR(255) PRIMARY KEY,
            title TEXT,
            description TEXT,
            frequency VARCHAR(50),
            units VARCHAR(100),
            category_id BIGINT,
            category_name VARCHAR(255),
            observation_start VARCHAR(50),
            observation_end VARCHAR(50),
            country VARCHAR(100),
            last_updated VARCHAR(50),
            popularity VARCHAR(50),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # Create index on category_id for faster queries
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_series_category_id 
        ON series(category_id)
    """)
    
    # Create categories table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS categories (
            category_id BIGINT PRIMARY KEY,
            name TEXT,
            parent_id BIGINT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # Create index on parent_id for tree traversal
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_categories_parent_id 
        ON categories(parent_id)
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
    
    # Initialize metadata if empty
    cur.execute("SELECT COUNT(*) FROM metadata")
    if cur.fetchone()[0] == 0:
        now = datetime.now()
        initial_metadata = [
            ('generated_at', now.isoformat(), now),
            ('total_series', '0', now),
            ('status', 'in_progress', now),
            ('source', 'FRED API', now),
        ]
        execute_values(
            cur,
            "INSERT INTO metadata (key, value, updated_at) VALUES %s",
            initial_metadata
        )
    
    conn.commit()
    cur.close()


_FRED_VIEWS_DIR = Path(__file__).resolve().parent / "categories"
# Order matters: category_paths view first; category_analysis view second (self-contained CTE).
_VIEW_SQL_FILES = ["categories_tree.sql", "category_analysis.sql"]


def apply_fred_views(conn: psycopg2.extensions.connection) -> None:
    """
    Create or replace FRED views in the database.
    Runs view SQL files in a fixed order so dependencies are consistent.
    """
    cur = conn.cursor()
    for name in _VIEW_SQL_FILES:
        path = _FRED_VIEWS_DIR / name
        if not path.exists():
            continue
        try:
            cur.execute(path.read_text())
            conn.commit()
        except Exception as e:
            conn.rollback()
            cur.close()
            raise RuntimeError(f"Failed to apply {name}: {e}") from e
    cur.close()


def add_fred_series_fast(conn: psycopg2.extensions.connection, new_series: List[Dict]) -> int:
    """
    Add FRED series to PostgreSQL database (upsert - insert or update if exists)
    
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
    columns = ['series_id', 'title', 'description', 'frequency', 'units', 'category_id',
               'category_name', 'observation_start', 'observation_end', 'country',
               'last_updated', 'popularity', 'created_at', 'updated_at']
    
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
        row = []
        for col in columns:
            if col in ['created_at', 'updated_at']:
                val = series.get(col)
                if isinstance(val, str):
                    row.append(datetime.fromisoformat(val))
                else:
                    row.append(val if val is not None else datetime.now())
            elif col == 'category_id':
                val = series.get(col)
                row.append(int(val) if val not in (None, '') else None)
            else:
                row.append(str(series.get(col, '')))
        values.append(tuple(row))
    
    # Use INSERT ... ON CONFLICT to upsert
    insert_query = sql.SQL("""
        INSERT INTO series ({columns})
        VALUES %s
        ON CONFLICT (series_id) 
        DO UPDATE SET
            title = EXCLUDED.title,
            description = EXCLUDED.description,
            frequency = EXCLUDED.frequency,
            units = EXCLUDED.units,
            category_id = EXCLUDED.category_id,
            category_name = EXCLUDED.category_name,
            observation_start = EXCLUDED.observation_start,
            observation_end = EXCLUDED.observation_end,
            country = EXCLUDED.country,
            last_updated = EXCLUDED.last_updated,
            popularity = EXCLUDED.popularity,
            updated_at = EXCLUDED.updated_at
    """).format(
        columns=sql.SQL(', ').join(map(sql.Identifier, columns))
    )
    
    execute_values(cur, insert_query, values)
    conn.commit()
    
    added_count = cur.rowcount
    cur.close()
    
    return added_count


def load_fred_series_from_postgres(conn: psycopg2.extensions.connection,
                                   category_ids: Optional[List[int]] = None) -> List[Dict]:
    """
    Load FRED series from PostgreSQL database
    
    Args:
        conn: PostgreSQL connection
        category_ids: Optional list of category IDs to filter by
    
    Returns:
        List of series dictionaries
    """
    cur = conn.cursor(cursor_factory=RealDictCursor)
    
    if category_ids:
        placeholders = ','.join(['%s'] * len(category_ids))
        query = f"""
            SELECT * FROM series 
            WHERE category_id IN ({placeholders})
        """
        cur.execute(query, list(category_ids))
    else:
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


def add_fred_categories_fast(conn: psycopg2.extensions.connection, 
                             new_categories: List[Dict]) -> int:
    """
    Add FRED categories to PostgreSQL database (upsert)
    
    Args:
        conn: PostgreSQL connection
        new_categories: List of category dictionaries with category_id, name, parent_id
    
    Returns:
        Number of categories successfully added/updated
    """
    if not new_categories:
        return 0
    
    cur = conn.cursor()
    
    # Ensure all required fields exist
    for category in new_categories:
        for field in ['category_id', 'name', 'parent_id', 'created_at', 'updated_at']:
            if field not in category:
                if field in ['created_at', 'updated_at']:
                    category[field] = datetime.now()
                elif field in ('category_id', 'parent_id'):
                    category[field] = 0  # Default to 0 for root categories
                else:
                    category[field] = ''
    
    columns = ['category_id', 'name', 'parent_id', 'created_at', 'updated_at']
    values = []
    for category in new_categories:
        row = []
        for col in columns:
            if col in ['created_at', 'updated_at']:
                # Handle timestamp fields
                val = category.get(col)
                if isinstance(val, str):
                    row.append(datetime.fromisoformat(val))
                else:
                    row.append(val if val is not None else datetime.now())
            elif col in ('category_id', 'parent_id'):
                # Handle bigint fields
                val = category.get(col)
                if val is None or val == '':
                    row.append(0)
                else:
                    row.append(int(val))
            else:
                row.append(str(category.get(col, '')))
        values.append(tuple(row))
    
    insert_query = sql.SQL("""
        INSERT INTO categories ({columns})
        VALUES %s
        ON CONFLICT (category_id)
        DO UPDATE SET
            name = EXCLUDED.name,
            parent_id = EXCLUDED.parent_id,
            updated_at = EXCLUDED.updated_at
    """).format(
        columns=sql.SQL(', ').join(map(sql.Identifier, columns))
    )
    
    execute_values(cur, insert_query, values)
    conn.commit()
    
    added_count = cur.rowcount
    cur.close()
    
    return added_count


def load_fred_categories_from_postgres(conn: psycopg2.extensions.connection) -> List[Dict]:
    """
    Load FRED categories from PostgreSQL database
    
    Args:
        conn: PostgreSQL connection
    
    Returns:
        List of category dictionaries
    """
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("SELECT * FROM categories")
    results = cur.fetchall()
    cur.close()
    
    # Convert to list of dictionaries, converting timestamps to strings
    categories_list = []
    for row in results:
        category_dict = dict(row)
        for key in ['created_at', 'updated_at']:
            if category_dict.get(key) and isinstance(category_dict[key], datetime):
                category_dict[key] = category_dict[key].isoformat()
        categories_list.append(category_dict)
    
    return categories_list


def update_fred_metadata(conn: psycopg2.extensions.connection, key: str, value: str) -> None:
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


def get_fred_metadata(conn: psycopg2.extensions.connection, key: Optional[str] = None) -> Dict[str, str]:
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
        time_series_list: List of dicts with keys: series_id, date, value
        
    Returns:
        Number of records inserted (excluding duplicates)
    """
    if not time_series_list:
        return 0
    
    cur = conn.cursor()
    
    try:
        # Prepare data for batch insert
        columns = ['series_id', 'date', 'value']
        values = []
        for ts in time_series_list:
            row = (
                str(ts.get('series_id', '')),
                ts.get('date'),  # Should be date string or date object
                ts.get('value') if ts.get('value') is not None else None
            )
            values.append(row)
        
        # Use INSERT ... ON CONFLICT to upsert (update if exists)
        insert_query = sql.SQL("""
            INSERT INTO time_series ({columns})
            VALUES %s
            ON CONFLICT (series_id, date) 
            DO UPDATE SET
                value = EXCLUDED.value
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
        List of dicts with keys: series_id, date, value
    """
    cur = conn.cursor(cursor_factory=RealDictCursor)
    
    query = "SELECT series_id, date, value, created_at FROM time_series WHERE 1=1"
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


def get_max_date_for_series(conn, series_id: str) -> Optional[str]:
    """
    Get the most recent date for a series in the database.
    Used for incremental downloads: start from day after last stored date.
    """
    cur = conn.cursor()
    cur.execute(
        "SELECT MAX(date)::text FROM time_series WHERE series_id = %s",
        (str(series_id),)
    )
    row = cur.fetchone()
    cur.close()
    return row[0] if row and row[0] else None


def get_series_updated_at(conn: psycopg2.extensions.connection, series_id: str) -> Optional[datetime]:
    """Return updated_at from the series table for this series_id, or None if no row."""
    cur = conn.cursor()
    cur.execute("SELECT updated_at FROM series WHERE series_id = %s", (str(series_id),))
    row = cur.fetchone()
    cur.close()
    if row and row[0]:
        return row[0] if isinstance(row[0], datetime) else datetime.combine(row[0], datetime.min.time())
    return None


def get_series_updated_at_bulk(conn: psycopg2.extensions.connection, series_ids: List[str]) -> Dict[str, Optional[datetime]]:
    """Return mapping series_id -> updated_at for all given series_ids (one query). Missing rows yield None."""
    if not series_ids:
        return {}
    cur = conn.cursor()
    cur.execute(
        "SELECT series_id, updated_at FROM series WHERE series_id = ANY(%s)",
        (list(series_ids),)
    )
    out = {str(sid): None for sid in series_ids}
    for row in cur.fetchall():
        sid, updated_at = row[0], row[1]
        if sid is not None:
            out[str(sid)] = (
                updated_at if isinstance(updated_at, datetime) else datetime.combine(updated_at, datetime.min.time())
                if updated_at else None
            )
    cur.close()
    return out


def update_series_updated_at(conn: psycopg2.extensions.connection, series_id: str) -> None:
    """Set series.updated_at to now for this series_id (after downloading time series)."""
    cur = conn.cursor()
    cur.execute("UPDATE series SET updated_at = %s WHERE series_id = %s", (datetime.now(), str(series_id)))
    conn.commit()
    cur.close()

