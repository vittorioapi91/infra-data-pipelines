"""
BIS Parquet Database Management

This module handles Parquet file operations for BIS data storage.
"""

import os
from typing import Dict, Optional, List
from datetime import datetime
import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd


def init_bis_parquet_tables(parquet_dir: str, conn: Optional[duckdb.DuckDBPyConnection] = None) -> duckdb.DuckDBPyConnection:
    """
    Initialize DuckDB connection and create tables for BIS dataflows and metadata if they don't exist.
    """
    if conn is None:
        conn = duckdb.connect()

    os.makedirs(parquet_dir, exist_ok=True)

    dataflows_parquet = os.path.join(parquet_dir, 'bis_dataflows.parquet')
    metadata_parquet = os.path.join(parquet_dir, 'bis_metadata.parquet')

    if not os.path.exists(dataflows_parquet):
        dataflows_schema = pa.schema([
            ('dataflow_id', pa.string()),
            ('name', pa.string()),
            ('description', pa.string()),
            ('last_updated', pa.string()),
            ('frequency', pa.string()),
            ('updated_at', pa.string()),
        ])
        empty_df = pa.Table.from_pylist([], schema=dataflows_schema)
        pq.write_table(empty_df, dataflows_parquet)

    if not os.path.exists(metadata_parquet):
        metadata_schema = pa.schema([
            ('key', pa.string()),
            ('value', pa.string()),
            ('updated_at', pa.string()),
        ])
        initial_metadata = [
            {'key': 'generated_at', 'value': datetime.now().isoformat(), 'updated_at': datetime.now().isoformat()},
            {'key': 'status', 'value': 'in_progress', 'updated_at': datetime.now().isoformat()},
            {'key': 'source', 'value': 'BIS Stats API', 'updated_at': datetime.now().isoformat()},
        ]
        empty_df = pa.Table.from_pylist(initial_metadata, schema=metadata_schema)
        pq.write_table(empty_df, metadata_parquet)

    return conn


def get_bis_parquet_paths(parquet_file: str) -> Dict[str, str]:
    """Return parquet file paths based on a base path or directory."""
    if os.path.isdir(parquet_file):
        base_dir = parquet_file
    else:
        base_dir = os.path.dirname(parquet_file) if os.path.dirname(parquet_file) else '.'

    return {
        'base_dir': base_dir,
        'dataflows': os.path.join(base_dir, 'bis_dataflows.parquet'),
        'metadata': os.path.join(base_dir, 'bis_metadata.parquet'),
    }


def add_bis_dataflows(dataflows_file: str, new_dataflows: List[Dict]) -> int:
    """
    Add BIS dataflows to parquet. Deduplicates by dataflow_id.
    """
    if not new_dataflows:
        return 0

    existing_schema = None
    if os.path.exists(dataflows_file):
        try:
            existing_table = pq.read_table(dataflows_file)
            existing_schema = existing_table.schema
            existing_df = existing_table.to_pandas()
            existing_ids = set(existing_df['dataflow_id'].dropna().unique())
        except Exception:
            existing_schema = None
            existing_df = pd.DataFrame()
            existing_ids = set()
    else:
        existing_df = pd.DataFrame()
        existing_ids = set()

    if existing_schema is None:
        existing_schema = pa.schema([
            ('dataflow_id', pa.string()),
            ('name', pa.string()),
            ('description', pa.string()),
            ('last_updated', pa.string()),
            ('frequency', pa.string()),
            ('updated_at', pa.string()),
        ])

    new_filtered = [d for d in new_dataflows if d.get('dataflow_id') and d['dataflow_id'] not in existing_ids]
    if not new_filtered:
        return 0

    new_table = pa.Table.from_pylist(new_filtered, schema=existing_schema)
    new_df = new_table.to_pandas()

    if len(existing_df) > 0:
        combined_df = pd.concat([existing_df, new_df], ignore_index=True)
    else:
        combined_df = new_df

    combined_table = pa.Table.from_pandas(combined_df, schema=existing_schema)
    pq.write_table(combined_table, dataflows_file)

    return len(new_filtered)


def load_bis_dataflows_from_parquet(parquet_file: str) -> List[Dict]:
    """Load BIS dataflows metadata from parquet."""
    paths = get_bis_parquet_paths(parquet_file)
    dataflows_file = paths['dataflows']
    if not os.path.exists(dataflows_file):
        return []
    try:
        table = pq.read_table(dataflows_file)
        return table.to_pandas().to_dict(orient='records')
    except Exception:
        return []


def update_bis_metadata(parquet_file: str, key: str, value: str) -> None:
    """Update metadata key/value in metadata parquet."""
    paths = get_bis_parquet_paths(parquet_file)
    metadata_file = paths['metadata']

    if os.path.exists(metadata_file):
        try:
            metadata_df = pq.read_table(metadata_file).to_pandas()
        except Exception:
            metadata_df = pd.DataFrame(columns=['key', 'value', 'updated_at'])
    else:
        metadata_df = pd.DataFrame(columns=['key', 'value', 'updated_at'])

    if key in metadata_df['key'].values:
        metadata_df.loc[metadata_df['key'] == key, 'value'] = value
        metadata_df.loc[metadata_df['key'] == key, 'updated_at'] = datetime.now().isoformat()
    else:
        new_row = pd.DataFrame([{
            'key': key,
            'value': value,
            'updated_at': datetime.now().isoformat()
        }])
        metadata_df = pd.concat([metadata_df, new_row], ignore_index=True)

    metadata_table = pa.Table.from_pandas(metadata_df)
    pq.write_table(metadata_table, metadata_file)
