"""
Eurostat PostgreSQL Database Management

This module handles all PostgreSQL operations for Eurostat data storage.
Uses datalake database, eurostat schema.
"""

import os
from typing import Dict, Optional, List
from datetime import datetime
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values, RealDictCursor
from psycopg2 import sql

from src.postgres_connection import get_datalake_connection

_SCHEMA = "eurostat"


def get_postgres_connection(dbname: str = "eurostat", user: Optional[str] = None,
                            host: Optional[str] = None, password: Optional[str] = None,
                            port: Optional[int] = None) -> psycopg2.extensions.connection:
    """
    Get PostgreSQL connection to the datalake eurostat schema.
    Uses ENV for host (postgres.{env}.local.info) and user ({env}.user); POSTGRES_PASSWORD, POSTGRES_PORT.
    """
    return get_datalake_connection(
        _SCHEMA,
        user=user,
        host=host,
        password=password,
        port=port,
    )


def init_eurostat_postgres_tables(conn: psycopg2.extensions.connection) -> None:
    """
    Initialize PostgreSQL tables for Eurostat data if they don't exist
    
    Args:
        conn: PostgreSQL connection
    """
    cur = conn.cursor()
    
    # Create datasets table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS datasets (
            dataset_code VARCHAR(255) PRIMARY KEY,
            title TEXT,
            description TEXT,
            last_update VARCHAR(50),
            frequency VARCHAR(50),
            theme VARCHAR(255),
            keywords TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # Create index on theme for faster queries
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_datasets_theme 
        ON datasets(theme)
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
    # Eurostat datasets are multi-dimensional, so we store them with dimension columns
    cur.execute("""
        CREATE TABLE IF NOT EXISTS time_series (
            dataset_code VARCHAR(255) NOT NULL,
            date DATE NOT NULL,
            value DOUBLE PRECISION,
            dimensions JSONB,
            PRIMARY KEY (dataset_code, date, dimensions),
            FOREIGN KEY (dataset_code) REFERENCES datasets(dataset_code) ON DELETE CASCADE
        )
    """)
    
    # Create index on date for faster queries
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_time_series_date 
        ON time_series(date)
    """)
    
    # Create index on dataset_code for faster lookups
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_time_series_dataset_code 
        ON time_series(dataset_code)
    """)
    
    # Create GIN index on dimensions JSONB for faster JSON queries
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_time_series_dimensions 
        ON time_series USING GIN (dimensions)
    """)
    
    # Initialize metadata if empty
    cur.execute("SELECT COUNT(*) FROM metadata")
    if cur.fetchone()[0] == 0:
        now = datetime.now()
        initial_metadata = [
            ('generated_at', now.isoformat(), now),
            ('total_datasets', '0', now),
            ('status', 'in_progress', now),
            ('source', 'Eurostat API', now),
        ]
        execute_values(
            cur,
            "INSERT INTO metadata (key, value, updated_at) VALUES %s",
            initial_metadata
        )
    
    conn.commit()
    cur.close()


def add_eurostat_datasets_fast(conn: psycopg2.extensions.connection, new_datasets: List[Dict]) -> int:
    """
    Add Eurostat datasets to PostgreSQL database (upsert - insert or update if exists)
    
    Args:
        conn: PostgreSQL connection
        new_datasets: List of dataset dictionaries to add
    
    Returns:
        Number of datasets successfully added/updated
    """
    if not new_datasets:
        return 0
    
    cur = conn.cursor()
    
    # Prepare data for bulk insert
    columns = ['dataset_code', 'title', 'description', 'last_update', 'frequency',
               'theme', 'keywords', 'created_at', 'updated_at']
    
    # Ensure all required fields exist
    for dataset in new_datasets:
        for field in columns:
            if field not in dataset:
                if field in ['created_at', 'updated_at']:
                    dataset[field] = datetime.now()
                else:
                    dataset[field] = ''
    
    # Prepare values for upsert
    values = []
    for dataset in new_datasets:
        row = tuple(
            str(dataset.get(col, '')) if col not in ['created_at', 'updated_at'] 
            else (datetime.fromisoformat(dataset[col]) if isinstance(dataset.get(col), str) 
                  else dataset.get(col, datetime.now()))
            for col in columns
        )
        values.append(row)
    
    # Use INSERT ... ON CONFLICT to upsert
    insert_query = sql.SQL("""
        INSERT INTO datasets ({columns})
        VALUES %s
        ON CONFLICT (dataset_code) 
        DO UPDATE SET
            title = EXCLUDED.title,
            description = EXCLUDED.description,
            last_update = EXCLUDED.last_update,
            frequency = EXCLUDED.frequency,
            theme = EXCLUDED.theme,
            keywords = EXCLUDED.keywords,
            updated_at = EXCLUDED.updated_at
    """).format(
        columns=sql.SQL(', ').join(map(sql.Identifier, columns))
    )
    
    execute_values(cur, insert_query, values)
    conn.commit()
    
    added_count = cur.rowcount
    cur.close()
    
    return added_count


def load_eurostat_datasets_from_postgres(conn: psycopg2.extensions.connection) -> List[Dict]:
    """
    Load Eurostat datasets from PostgreSQL database
    
    Args:
        conn: PostgreSQL connection
    
    Returns:
        List of dataset dictionaries
    """
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("SELECT * FROM datasets")
    results = cur.fetchall()
    cur.close()
    
    # Convert to list of dictionaries, converting timestamps to strings
    datasets_list = []
    for row in results:
        dataset_dict = dict(row)
        # Convert timestamps to ISO format strings
        for key in ['created_at', 'updated_at']:
            if dataset_dict.get(key) and isinstance(dataset_dict[key], datetime):
                dataset_dict[key] = dataset_dict[key].isoformat()
        datasets_list.append(dataset_dict)
    
    return datasets_list


def update_eurostat_metadata(conn: psycopg2.extensions.connection, key: str, value: str) -> None:
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


def get_eurostat_metadata(conn: psycopg2.extensions.connection, key: Optional[str] = None) -> Dict[str, str]:
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
    Fast batch insert of time series data for Eurostat datasets
    
    Args:
        conn: PostgreSQL connection
        time_series_list: List of dicts with keys: dataset_code, date, value, dimensions (dict)
        
    Returns:
        Number of records inserted (excluding duplicates)
    """
    if not time_series_list:
        return 0
    
    cur = conn.cursor()
    
    try:
        import json
        
        # Prepare data for batch insert
        columns = ['dataset_code', 'date', 'value', 'dimensions']
        values = []
        for ts in time_series_list:
            # Convert dimensions dict to JSON string for JSONB column
            dimensions = ts.get('dimensions', {})
            if isinstance(dimensions, dict):
                dimensions_json = json.dumps(dimensions)
            elif isinstance(dimensions, str):
                dimensions_json = dimensions
            else:
                dimensions_json = '{}'
            
            row = (
                str(ts.get('dataset_code', '')),
                ts.get('date'),  # Should be date string or date object
                ts.get('value') if ts.get('value') is not None else None,
                dimensions_json
            )
            values.append(row)
        
        # Use INSERT ... ON CONFLICT to upsert (update if exists)
        insert_query = sql.SQL("""
            INSERT INTO time_series ({columns})
            VALUES %s
            ON CONFLICT (dataset_code, date, dimensions) 
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


def load_time_series_from_postgres(conn, dataset_code: Optional[str] = None, 
                                   start_date: Optional[str] = None,
                                   end_date: Optional[str] = None,
                                   dimensions_filter: Optional[Dict] = None) -> List[Dict]:
    """
    Load time series data from PostgreSQL
    
    Args:
        conn: PostgreSQL connection
        dataset_code: Optional dataset code to filter by
        start_date: Optional start date (YYYY-MM-DD format)
        end_date: Optional end date (YYYY-MM-DD format)
        dimensions_filter: Optional dictionary of dimension filters (e.g., {'geo': 'DE'})
        
    Returns:
        List of dicts with keys: dataset_code, date, value, dimensions
    """
    cur = conn.cursor(cursor_factory=RealDictCursor)
    
    query = "SELECT dataset_code, date, value, dimensions FROM time_series WHERE 1=1"
    params = []
    
    if dataset_code:
        query += " AND dataset_code = %s"
        params.append(str(dataset_code))
    
    if start_date:
        query += " AND date >= %s"
        params.append(start_date)
    
    if end_date:
        query += " AND date <= %s"
        params.append(end_date)
    
    if dimensions_filter:
        import json
        # Build JSONB filter query
        for key, value in dimensions_filter.items():
            query += f" AND dimensions @> %s"
            params.append(json.dumps({key: value}))
    
    query += " ORDER BY dataset_code, date"
    
    cur.execute(query, params)
    results = cur.fetchall()
    cur.close()
    
    # Convert results to list of dicts, parsing JSON dimensions
    import json
    time_series_list = []
    for row in results:
        row_dict = dict(row)
        # Parse dimensions JSONB to dict
        if row_dict.get('dimensions'):
            if isinstance(row_dict['dimensions'], str):
                row_dict['dimensions'] = json.loads(row_dict['dimensions'])
        else:
            row_dict['dimensions'] = {}
        time_series_list.append(row_dict)
    
    return time_series_list

