"""
EDGAR Master Index PostgreSQL Database Management

This module handles PostgreSQL operations specific to master.idx file data storage.
"""

from typing import Dict, Optional, List
import psycopg2
from psycopg2.extras import RealDictCursor


def get_master_idx_download_status(conn: psycopg2.extensions.connection, year: int, quarter: str) -> Optional[Dict]:
    """
    Get download status for a specific year/quarter from the ledger
    
    Args:
        conn: PostgreSQL connection
        year: Year (e.g., 2024)
        quarter: Quarter (e.g., 'QTR1')
        
    Returns:
        Dictionary with status info or None if not found
    """
    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute("""
            SELECT year, quarter, status, downloaded_at, failed_at, error_message, retry_count, last_attempt
            FROM master_idx_download_ledger
            WHERE year = %s AND quarter = %s
        """, (year, quarter))
        result = cur.fetchone()
        if result:
            return dict(result)
        return None
    finally:
        cur.close()


def mark_master_idx_download_success(conn: psycopg2.extensions.connection, year: int, quarter: str) -> None:
    """
    Mark a year/quarter download as successful in the ledger
    
    Args:
        conn: PostgreSQL connection
        year: Year (e.g., 2024)
        quarter: Quarter (e.g., 'QTR1')
    """
    cur = conn.cursor()
    try:
        cur.execute("""
            INSERT INTO master_idx_download_ledger (year, quarter, status, downloaded_at, last_attempt)
            VALUES (%s, %s, 'success', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            ON CONFLICT (year, quarter) 
            DO UPDATE SET 
                status = 'success',
                downloaded_at = CURRENT_TIMESTAMP,
                last_attempt = CURRENT_TIMESTAMP,
                retry_count = 0,
                error_message = NULL
        """, (year, quarter))
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cur.close()


def mark_master_idx_download_failed(conn: psycopg2.extensions.connection, year: int, quarter: str, 
                                     error_message: str) -> None:
    """
    Mark a year/quarter download as failed in the ledger
    
    Args:
        conn: PostgreSQL connection
        year: Year (e.g., 2024)
        quarter: Quarter (e.g., 'QTR1')
        error_message: Error message describing the failure
    """
    cur = conn.cursor()
    try:
        cur.execute("""
            INSERT INTO master_idx_download_ledger (year, quarter, status, failed_at, error_message, retry_count, last_attempt)
            VALUES (%s, %s, 'failed', CURRENT_TIMESTAMP, %s, 1, CURRENT_TIMESTAMP)
            ON CONFLICT (year, quarter) 
            DO UPDATE SET 
                status = 'failed',
                failed_at = CURRENT_TIMESTAMP,
                error_message = %s,
                retry_count = master_idx_download_ledger.retry_count + 1,
                last_attempt = CURRENT_TIMESTAMP
        """, (year, quarter, error_message, error_message))
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cur.close()


def get_quarters_with_data(conn: psycopg2.extensions.connection, start_year: Optional[int] = None) -> List[tuple]:
    """
    Get list of quarters that already have data in master_idx_files table
    
    Args:
        conn: PostgreSQL connection
        start_year: Optional start year filter
        
    Returns:
        List of (year, quarter) tuples that have data
    """
    cur = conn.cursor()
    try:
        query = """
            SELECT DISTINCT year, quarter 
            FROM master_idx_files
        """
        params = []
        if start_year:
            query += " WHERE year >= %s"
            params.append(start_year)
        query += " ORDER BY year, quarter"
        
        cur.execute(query, params)
        return [(row[0], row[1]) for row in cur.fetchall()]
    finally:
        cur.close()


def get_pending_or_failed_quarters(conn: psycopg2.extensions.connection, 
                                   start_year: Optional[int] = None) -> List[tuple]:
    """
    Get list of quarters that are pending or failed (need to be downloaded)
    
    Args:
        conn: PostgreSQL connection
        start_year: Start year to check from (default: 1993)
        
    Returns:
        List of (year, quarter) tuples that need to be downloaded
    """
    start_year = start_year or 1993
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT year, quarter
            FROM master_idx_download_ledger
            WHERE status IN ('pending', 'failed')
            AND year >= %s
            ORDER BY year, quarter
        """, (start_year,))
        return cur.fetchall()
    finally:
        cur.close()
