"""
IMF Parquet Database Management

This module handles all DuckDB and Parquet file operations for IMF data storage.
"""

import logging
import os
from typing import Dict, Optional, List
from datetime import datetime
import duckdb

logger = logging.getLogger(__name__)
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd


def init_imf_parquet_tables(parquet_dir: str, conn: Optional[duckdb.DuckDBPyConnection] = None) -> duckdb.DuckDBPyConnection:
    """
    Initialize DuckDB connection and create tables for IMF indicators if they don't exist
    
    Args:
        parquet_dir: Directory where parquet files will be stored
        conn: Existing DuckDB connection (optional)
    
    Returns:
        DuckDB connection
    """
    if conn is None:
        conn = duckdb.connect()
    
    # Create directory if it doesn't exist
    os.makedirs(parquet_dir, exist_ok=True)
    
    indicators_parquet = os.path.join(parquet_dir, 'imf_indicators.parquet')
    metadata_parquet = os.path.join(parquet_dir, 'imf_metadata.parquet')
    
    # Create indicators table schema if file doesn't exist
    if not os.path.exists(indicators_parquet):
        indicators_schema = pa.schema([
            ('indicator_code', pa.string()),
            ('database_id', pa.string()),
            ('indicator_name', pa.string()),
            ('description', pa.string()),
            ('created_at', pa.string()),
            ('updated_at', pa.string()),
        ])
        empty_df = pa.Table.from_pylist([], schema=indicators_schema)
        pq.write_table(empty_df, indicators_parquet)
    
    # Create metadata table schema if file doesn't exist
    if not os.path.exists(metadata_parquet):
        metadata_schema = pa.schema([
            ('key', pa.string()),
            ('value', pa.string()),
            ('updated_at', pa.string()),
        ])
        initial_metadata = [
            {'key': 'generated_at', 'value': datetime.now().isoformat(), 'updated_at': datetime.now().isoformat()},
            {'key': 'total_indicators', 'value': '0', 'updated_at': datetime.now().isoformat()},
            {'key': 'status', 'value': 'in_progress', 'updated_at': datetime.now().isoformat()},
            {'key': 'source', 'value': 'IMF API', 'updated_at': datetime.now().isoformat()},
        ]
        empty_df = pa.Table.from_pylist(initial_metadata, schema=metadata_schema)
        pq.write_table(empty_df, metadata_parquet)
    
    return conn


def get_imf_parquet_paths(parquet_file: str) -> Dict[str, str]:
    """
    Get parquet file paths from base path (handles both file and directory inputs)
    
    Args:
        parquet_file: Path to parquet file or directory
    
    Returns:
        Dictionary with 'indicators', 'metadata', 'base_dir' paths
    """
    if os.path.isdir(parquet_file):
        base_dir = parquet_file
    else:
        base_dir = os.path.dirname(parquet_file) if os.path.dirname(parquet_file) else '.'
    
    return {
        'base_dir': base_dir,
        'indicators': os.path.join(base_dir, 'imf_indicators.parquet'),
        'metadata': os.path.join(base_dir, 'imf_metadata.parquet'),
    }


def add_imf_indicators_fast(indicators_file: str, new_indicators: List[Dict]) -> int:
    """
    Add IMF indicators to parquet file in batch (fast, non-atomic).
    Reads existing file once, adds all new indicators, writes back.
    
    Args:
        indicators_file: Path to the indicators parquet file
        new_indicators: List of indicator dictionaries to add
    
    Returns:
        Number of indicators successfully added
    """
    if not new_indicators:
        return 0
    
    # Get existing schema if file exists
    existing_schema = None
    if os.path.exists(indicators_file):
        try:
            existing_table = pq.read_table(indicators_file)
            existing_schema = existing_table.schema
            existing_df = existing_table.to_pandas()
            # Get existing indicator codes to avoid duplicates
            existing_codes = set(existing_df['indicator_code'].dropna().unique())
        except Exception:
            existing_schema = None
            existing_df = pd.DataFrame()
            existing_codes = set()
    else:
        existing_df = pd.DataFrame()
        existing_codes = set()
    
    # If no schema exists, use the standard schema
    if existing_schema is None:
        existing_schema = pa.schema([
            ('indicator_code', pa.string()),
            ('database_id', pa.string()),
            ('indicator_name', pa.string()),
            ('description', pa.string()),
            ('created_at', pa.string()),
            ('updated_at', pa.string()),
        ])
    
    # Filter out indicators that already exist
    new_indicators_filtered = [
        i for i in new_indicators 
        if i.get('indicator_code') and i.get('indicator_code') not in existing_codes
    ]
    
    if not new_indicators_filtered:
        return 0
    
    # Ensure all required fields exist
    for indicator in new_indicators_filtered:
        for field in ['indicator_code', 'database_id', 'indicator_name', 'description', 'created_at', 'updated_at']:
            if field not in indicator:
                indicator[field] = ''
    
    # Create new indicators table with proper schema
    new_table = pa.Table.from_pylist(new_indicators_filtered, schema=existing_schema)
    new_df = new_table.to_pandas()
    
    # Combine with existing
    if len(existing_df) > 0:
        combined_df = pd.concat([existing_df, new_df], ignore_index=True)
    else:
        combined_df = new_df
    
    # Write directly (non-atomic, but fast)
    combined_table = pa.Table.from_pandas(combined_df, schema=existing_schema)
    pq.write_table(combined_table, indicators_file)
    
    return len(new_indicators_filtered)


def load_imf_indicators_from_parquet(parquet_file: str) -> List[Dict]:
    """
    Load IMF indicators from parquet file
    
    Args:
        parquet_file: Path to parquet directory or file
    
    Returns:
        List of indicator dictionaries
    """
    paths = get_imf_parquet_paths(parquet_file)
    
    if not os.path.exists(paths['indicators']):
        return []
    
    try:
        df = pq.read_table(paths['indicators']).to_pandas()
        return df.to_dict('records')
    except Exception as e:
        logger.exception("Error loading IMF indicators from parquet: %s", e)
        return []


def update_imf_metadata(parquet_file: str, key: str, value: str) -> None:
    """
    Update metadata value in IMF parquet file
    
    Args:
        parquet_file: Path to parquet directory or file
        key: Metadata key
        value: Metadata value
    """
    paths = get_imf_parquet_paths(parquet_file)
    
    if not os.path.exists(paths['metadata']):
        init_imf_parquet_tables(paths['base_dir'])
    
    try:
        df = pq.read_table(paths['metadata']).to_pandas()
        
        if key in df['key'].values:
            df.loc[df['key'] == key, 'value'] = value
            df.loc[df['key'] == key, 'updated_at'] = datetime.now().isoformat()
        else:
            new_row = pd.DataFrame([{
                'key': key,
                'value': value,
                'updated_at': datetime.now().isoformat()
            }])
            df = pd.concat([df, new_row], ignore_index=True)
        
        table = pa.Table.from_pandas(df)
        pq.write_table(table, paths['metadata'])
    except Exception as e:
        logger.exception("Error updating IMF metadata: %s", e)
