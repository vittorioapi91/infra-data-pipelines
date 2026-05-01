"""
SEC EDGAR Master Index File Management

This module handles downloading, parsing, and managing SEC EDGAR master.idx files.
"""

import os
import re
import gzip
import logging
import sys
from typing import Optional
from datetime import datetime
from pathlib import Path
import pandas as pd
import requests
import psycopg2
from psycopg2.extras import execute_values
from tqdm import tqdm

# Handle imports for both module import and direct script execution
try:
    from ...download_logger import get_download_logger
    from .master_idx_postgres import (
        get_master_idx_download_status, mark_master_idx_download_success,
        mark_master_idx_download_failed, get_quarters_with_data
    )
    from ..edgar import EDGARDownloader
    from ...config import get_environment
except ImportError:
    # Handle direct script execution - use absolute imports
    file_path = Path(__file__).resolve()
    project_root = file_path.parent.parent.parent.parent.parent
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))
    from src.fundamentals.download_logger import get_download_logger
    from src.fundamentals.edgar.master_idx.master_idx_postgres import (
        get_master_idx_download_status, mark_master_idx_download_success,
        mark_master_idx_download_failed, get_quarters_with_data
    )
    from src.fundamentals.edgar.edgar import EDGARDownloader
    from src.config import get_environment

# Set up logger using download_logger utility with console output
logger = get_download_logger('edgar_master_idx', log_level=logging.INFO, add_console_handler=True)


class MasterIdxManager(EDGARDownloader):
    """Class to manage SEC EDGAR master.idx file downloads and processing"""
    
    def __init__(self, user_agent: str = "VittorioApicella apicellavittorio@hotmail.it", master_dir: Optional[Path] = None):
        """
        Initialize Master Index Manager
        
        Args:
            user_agent: User-Agent string for SEC EDGAR requests (required by SEC)
            master_dir: Optional directory for storing master.idx files.
                       Default: TRADING_AGENT_STORAGE/fundamentals/edgar/master (must be set).
        """
        # Initialize parent class (EDGARDownloader) to get headers and base URLs
        super().__init__(user_agent=user_agent)
        
        # Set up master.idx storage directory.
        # Prefer explicit master_dir; otherwise derive from TRADING_AGENT_STORAGE
        # (as common root without env) + ENV, matching other storage users.
        if master_dir is None:
            storage_root = os.getenv('TRADING_AGENT_STORAGE')
            if not storage_root:
                raise RuntimeError(
                    "TRADING_AGENT_STORAGE environment variable is not set. "
                    "Please configure it in your .env file (e.g. .env.dev) to point to the storage root."
                )
            storage_env = os.getenv("ENV", "dev")
            self.master_dir = Path(storage_root) / storage_env / 'fundamentals' / 'edgar' / 'master'
        else:
            self.master_dir = master_dir
        self.master_dir.mkdir(exist_ok=True)
    
    def save_master_idx_to_disk(self, conn, start_year: Optional[int] = None) -> None:
        """
        Download master.idx files from SEC EDGAR, save to local storage, parse them, and save as CSV
        Only downloads new or previously failed quarters based on the ledger
        
        Args:
            conn: PostgreSQL connection for checking/updating ledger
            start_year: Start year for downloading (default: 1993)
        """
        logger.info("Downloading master.idx files...")
        start_year = start_year or 1993
        current_year = datetime.now().year
        quarters = ['QTR1', 'QTR2', 'QTR3', 'QTR4']
        
        # Get all possible quarters
        all_quarters = []
        for year in range(start_year, current_year + 1):
            for quarter in quarters:
                # Skip future quarters
                if year == current_year:
                    current_quarter = (datetime.now().month - 1) // 3 + 1
                    quarter_num = int(quarter[3])
                    if quarter_num > current_quarter:
                        continue
                all_quarters.append((year, quarter))
        
        # Get quarters that already have data in the database
        quarters_with_data = set(get_quarters_with_data(conn, start_year))
        
        # Filter to only quarters that are missing from database
        # Check both ledger status and actual database content
        total_items = []
        for year, quarter in all_quarters:
            # Skip if data already exists in database
            if (year, quarter) in quarters_with_data:
                continue
            
            # Check ledger status - only download if pending, failed, or not in ledger
            status = get_master_idx_download_status(conn, year, quarter)
            if status is None or status['status'] in ('pending', 'failed'):
                total_items.append((year, quarter))
        
        if not total_items:
            logger.info("No new or failed quarters to download.")
            return
        
        logger.info(f"Found {len(total_items)} quarters to download (new or failed)")
        
        # Progress bar for downloading
        with tqdm(total=len(total_items), desc="Downloading master.idx files", unit="file") as pbar:
            for year, quarter in total_items:
                try:
                    # Try uncompressed first
                    master_url = f"{self.base_url}/Archives/edgar/full-index/{year}/{quarter}/master.idx"
                    response = requests.get(master_url, headers=self.headers, timeout=30)
                    if response.status_code == 200:
                        self._save_master_idx_content(response.content, str(year), quarter, is_compressed=False)
                        mark_master_idx_download_success(conn, year, quarter)
                        pbar.set_postfix_str(f"{year}/{quarter} (uncompressed)")
                        pbar.update(1)
                        continue
                    
                    # Try compressed version
                    master_gz_url = f"{self.data_base_url}/files/edgar/full-index/{year}/{quarter}/master.idx.gz"
                    response = requests.get(master_gz_url, headers=self.data_headers, timeout=30)
                    if response.status_code == 200:
                        self._save_master_idx_content(response.content, str(year), quarter, is_compressed=True)
                        mark_master_idx_download_success(conn, year, quarter)
                        pbar.set_postfix_str(f"{year}/{quarter} (compressed)")
                        pbar.update(1)
                    else:
                        error_msg = f"Failed to download master.idx for {year}/{quarter}: HTTP {response.status_code}"
                        mark_master_idx_download_failed(conn, year, quarter, error_msg)
                        raise Exception(error_msg)
                except Exception as e:
                    error_msg = str(e)
                    mark_master_idx_download_failed(conn, year, quarter, error_msg)
                    raise
    
    def _save_master_idx_content(self, content: bytes, year: str, quarter: str, 
                        is_compressed: bool = False) -> None:
        """
        Save master.idx file content to local storage, parse it, and save as CSV
        
        Args:
            content: File content (bytes)
            year: Year (e.g., '2024')
            quarter: Quarter (e.g., 'QTR1')
            is_compressed: Whether the content is gzipped
        """
        # Save to local file system
        # Create year directory
        year_dir = self.master_dir / year
        year_dir.mkdir(exist_ok=True)
        
        # Determine filename
        filename = f"master.idx.gz" if is_compressed else f"master.idx"
        filepath = year_dir / f"{quarter}_{filename}"
        
        # Save raw file
        with open(filepath, 'wb') as f:
            f.write(content)
        
        # Parse and save as CSV
        df = self._parse_master_idx(content)
        
        csv_filename = f"{quarter}_master_parsed.csv"
        csv_filepath = year_dir / csv_filename
        df.to_csv(csv_filepath, index=False)

    def _parse_master_idx(self, content: bytes) -> pd.DataFrame:
        """
        Parse master.idx file content into a DataFrame
        
        Args:
            content: Content of master.idx file (bytes, may be gzipped)
            
        Returns:
            DataFrame with columns: cik, company_name, form_type, filing_date, filename, accession_number
        """
        rows = []
        
        # Try to decompress if it's gzipped
        try:
            content = gzip.decompress(content)
        except (gzip.BadGzipFile, OSError):
            # Not gzipped, use as-is
            pass
        
        # Decode to string
        try:
            text = content.decode('utf-8', errors='ignore')
        except:
            text = content.decode('latin-1', errors='ignore')
        
        # Parse each line (skip header lines)
        # Expected format: CIK|Company Name|Form Type|Date Filed|Filename
        # Example: 1000045|NICHOLAS FINANCIAL INC|10-Q|2022-11-14|edgar/data/1000045/0000950170-22-024756.txt
        for line in text.split('\n'):
            line = line.strip()
            # Skip empty lines, separator lines, and header lines
            if not line or line.startswith('---') or 'CIK' in line.upper():
                continue
            
            # Format: CIK|Company Name|Form Type|Date Filed|Filename
            parts = line.split('|')
            # Must have exactly 5 parts separated by |
            if len(parts) != 5:
                continue
            
            # Validate pattern: CIK should be numeric, filename should start with 'edgar/data/'
            line_cik = parts[0].strip()
            filename = parts[4].strip()
            
            # Skip if CIK is not numeric or filename doesn't match expected pattern
            if not line_cik.isdigit() or not filename.startswith('edgar/data/'):
                continue
            
            # Extract all parts
            company_name = parts[1].strip()
            form_type = parts[2].strip()
            date_filed_str = parts[3].strip()
            
            # Normalize CIK to 10 digits
            cik_int = int(line_cik)  # Remove leading zeros if any
            cik_normalized = str(cik_int).zfill(10)
        
            
            # Parse date (format: YYYYMMDD or YYYY-MM-DD)
            filing_date = None
            if len(date_filed_str) == 8 and date_filed_str.isdigit():
                # YYYYMMDD format
                filing_date = f"{date_filed_str[0:4]}-{date_filed_str[4:6]}-{date_filed_str[6:8]}"
            elif len(date_filed_str) == 10 and date_filed_str[4] == '-' and date_filed_str[7] == '-':
                # YYYY-MM-DD format
                filing_date = date_filed_str
            else:
                continue  # Skip invalid dates
            
            # Extract accession number from filename
            # Format: edgar/data/{cik}/{accession_number}.txt
            # Example: edgar/data/926688/9999999997-05-015654.txt
            accession_number = None
            if filename:
                path_parts = filename.split('/')
                if len(path_parts) >= 4:
                    # Get the filename part (last element)
                    filename_part = path_parts[-1]
                    # Remove .txt extension to get accession number
                    if filename_part.endswith('.txt'):
                        accession_number = filename_part[:-4]  # Remove '.txt'
                    else:
                        accession_number = filename_part
            
            if not accession_number:
                continue  # Skip if we can't extract accession number
            
            rows.append({
                'cik': cik_normalized,
                'company_name': company_name,
                'form_type': form_type,
                'filing_date': filing_date,
                'filename': filename,
                'accession_number': accession_number
            })
            
        
        # Create DataFrame
        if rows:
            df = pd.DataFrame(rows)
            return df
        else:
            return pd.DataFrame(columns=['cik', 'company_name', 'form_type', 'filing_date', 'filename', 'accession_number'])

    def save_master_idx_to_db(self, conn) -> None:
        """
        Load all parsed CSV files and save them to database
        
        Args:
            conn: PostgreSQL connection
        """
        if not self.master_dir.exists():
            return
        
        # Get quarters that already have data in the database
        quarters_with_data = set(get_quarters_with_data(conn))
        
        # Collect all CSV files first, but only for quarters missing from database
        csv_files = []
        for year_dir in sorted(self.master_dir.iterdir()):
            if not year_dir.is_dir():
                continue
            
            year = year_dir.name
            try:
                year_int = int(year)
            except ValueError:
                continue
            
            # Find all CSV files in this year directory
            for filepath in sorted(year_dir.iterdir()):
                if not filepath.is_file():
                    continue
                
                # Check if it's a parsed CSV file (QTR1_master_parsed.csv, etc.)
                filename = filepath.name
                if not filename.endswith('_master_parsed.csv'):
                    continue
                
                # Extract quarter from filename (QTR1, QTR2, QTR3, QTR4)
                quarter_match = re.match(r'QTR[1-4]', filename)
                if not quarter_match:
                    continue
                quarter = quarter_match.group(0)
                
                # Only process if data doesn't already exist in database
                if (year_int, quarter) not in quarters_with_data:
                    csv_files.append((year_int, quarter, filepath))
                else:
                    logger.debug(f"Skipping {year_int}/{quarter} - data already exists in database")
        
        # Progress bar for database saving
        with tqdm(total=len(csv_files), desc="Saving to database", unit="file") as pbar:
            for year_int, quarter, filepath in csv_files:
                # Read CSV file
                df = pd.read_csv(filepath)
                
                # Save to database if DataFrame is not empty
                if df is not None and not df.empty:
                    cur = conn.cursor()
                    try:
                        # Prepare data for bulk insert
                        records = []
                        for _, row in df.iterrows():
                            records.append((
                                year_int,
                                quarter,
                                row['cik'],                    # CIK
                                row['company_name'],           # Company Name
                                row['form_type'],              # Form Type
                                row['filing_date'],            # Date Filed
                                row['filename'],               # Filename
                                row.get('accession_number', '')  # Additional: accession_number
                            ))
                        
                        # Bulk insert with conflict handling (skip duplicates)
                        execute_values(
                            cur,
                            """
                            INSERT INTO master_idx_files 
                                (year, quarter, cik, company_name, form_type, date_filed, filename, accession_number)
                            VALUES %s
                            ON CONFLICT (year, quarter, cik, form_type, date_filed, filename) DO NOTHING
                            """,
                            records
                        )
                        conn.commit()
                        pbar.set_postfix_str(f"{year_int}/{quarter} ({len(df)} records)")
                    except Exception as e:
                        conn.rollback()
                        raise e
                    finally:
                        cur.close()
                else:
                    pbar.set_postfix_str(f"{year_int}/{quarter} (empty)")
                pbar.update(1)
