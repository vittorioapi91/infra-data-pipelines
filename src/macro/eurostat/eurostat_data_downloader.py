"""
Eurostat Data Downloader

This module provides functionality to download time series data from Eurostat
using the SDMX API via the sdmx1 library.
"""

import logging
import os
import time
from typing import List, Dict, Optional
import pandas as pd
from datetime import datetime

# Import Eurostat PostgreSQL utilities
try:
    from .eurostat_postgres import (
        get_postgres_connection, init_eurostat_postgres_tables,
        add_eurostat_datasets_fast, load_eurostat_datasets_from_postgres,
        update_eurostat_metadata, add_time_series_fast,
        load_time_series_from_postgres
    )
except ImportError:
    from src.macro.eurostat.eurostat_postgres import (
        get_postgres_connection, init_eurostat_postgres_tables,
        add_eurostat_datasets_fast, load_eurostat_datasets_from_postgres,
        update_eurostat_metadata, add_time_series_fast,
        load_time_series_from_postgres
    )
import warnings

import sdmx
from tqdm import tqdm

warnings.filterwarnings('ignore')
sdmx.log.setLevel(logging.WARNING)  # suppress per-request debug logs

logger = logging.getLogger(__name__)


class EurostatDataDownloader:
    """Class to download economic time series data from Eurostat via SDMX API"""

    def __init__(self, api_version: str = '1.0', format_type: str = 'json', language: str = 'en', timeout: int = 120):
        """
        Initialize Eurostat data downloader.

        Uses sdmx1 to connect to Eurostat's SDMX-REST web service.
        PostgreSQL credentials are loaded from .env (POSTGRES_USER, POSTGRES_HOST, POSTGRES_PASSWORD, POSTGRES_PORT).
        dbname is 'eurostat' (module name).

        Args:
            api_version: API version (default: '1.0', kept for compatibility)
            format_type: Response format (default: 'json', kept for compatibility)
            language: Language for metadata (default: 'en')
            timeout: HTTP request timeout in seconds (default: 120, Eurostat can be slow)
        """
        self.client = sdmx.Client("ESTAT", timeout=timeout)
        self.api_version = api_version
        self.format_type = format_type
        self.language = language
        
        # PostgreSQL: dbname equals module name (eurostat), credentials from .env
        self.dbname = 'eurostat'
        self.user = os.getenv('POSTGRES_USER')
        self.host = os.getenv('POSTGRES_HOST')
        self.password = os.getenv('POSTGRES_PASSWORD')
        _port = os.getenv('POSTGRES_PORT')
        if not self.user:
            raise ValueError("POSTGRES_USER is required (set in .env or environment)")
        if not self.host:
            raise ValueError("POSTGRES_HOST is required (set in .env or environment)")
        if self.password is None:
            raise ValueError("POSTGRES_PASSWORD is required (set in .env or environment)")
        if not _port:
            raise ValueError("POSTGRES_PORT is required (set in .env or environment)")
        self.port = int(_port)
        self._pg_conn = None
    
    def _get_pg_connection(self):
        """Get or create PostgreSQL connection"""
        if self._pg_conn is None or self._pg_conn.closed:
            self._pg_conn = get_postgres_connection(
                dbname=self.dbname,
                user=self.user,
                host=self.host,
                password=self.password,
                port=self.port
            )
            init_eurostat_postgres_tables(self._pg_conn)
        return self._pg_conn
    
    def _parse_eurostat_date(self, date_val) -> Optional[str]:
        """
        Parse Eurostat date format to YYYY-MM-DD
        
        Eurostat uses formats like:
        - 2020Q1 (quarterly)
        - 2020M01 (monthly)
        - 2020 (annual)
        - 2020-W01 (weekly)
        
        Args:
            date_val: Date value (can be string, datetime, or other)
            
        Returns:
            Date string in YYYY-MM-DD format or None if parsing fails
        """
        if date_val is None:
            return None
        
        # If already a datetime object
        if isinstance(date_val, pd.Timestamp):
            return date_val.strftime('%Y-%m-%d')
        
        if isinstance(date_val, datetime):
            return date_val.strftime('%Y-%m-%d')
        
        # Convert to string
        date_str = str(date_val).strip()
        
        # Try standard date parsing first
        try:
            from dateutil.parser import parse
            date_obj = parse(date_str)
            return date_obj.strftime('%Y-%m-%d')
        except:
            pass
        
        # Handle Eurostat-specific formats
        import re
        
        # Quarterly: 2020Q1 -> 2020-01-01 (first month of quarter)
        q_match = re.match(r'(\d{4})Q([1-4])', date_str, re.IGNORECASE)
        if q_match:
            year = int(q_match.group(1))
            quarter = int(q_match.group(2))
            month = (quarter - 1) * 3 + 1
            return f"{year}-{month:02d}-01"
        
        # Monthly: 2020M01 -> 2020-01-01
        m_match = re.match(r'(\d{4})M(\d{2})', date_str, re.IGNORECASE)
        if m_match:
            year = int(m_match.group(1))
            month = int(m_match.group(2))
            return f"{year}-{month:02d}-01"
        
        # Annual: 2020 -> 2020-01-01
        y_match = re.match(r'(\d{4})$', date_str)
        if y_match:
            year = int(y_match.group(1))
            return f"{year}-01-01"
        
        # Weekly: 2020-W01 -> approximate to first day of week
        w_match = re.match(r'(\d{4})-W(\d{2})', date_str, re.IGNORECASE)
        if w_match:
            year = int(w_match.group(1))
            week = int(w_match.group(2))
            # Approximate: first week starts around Jan 1
            return f"{year}-01-01"
        
        # Try to extract year-month-day pattern
        ymd_match = re.match(r'(\d{4})-(\d{2})-(\d{2})', date_str)
        if ymd_match:
            return date_str
        
        return None
        
    def get_all_datasets(self, limit: Optional[int] = None) -> List[Dict[str, str]]:
        """
        Get all available Eurostat datasets from the SDMX dataflow catalogue.

        Args:
            limit: Maximum number of datasets to return (None for all)

        Returns:
            List of dataset dictionaries with code, title, description, download_link, and metadata
        """
        logger.info("Fetching Eurostat datasets from SDMX dataflow catalogue...")

        try:
            flow_msg = self.client.dataflow()
            datasets: List[Dict[str, str]] = []

            for df_id, df_def in flow_msg.dataflow.items():
                code = str(df_id)
                name_obj = getattr(df_def, 'name', None)
                title = (getattr(name_obj, self.language, None) or
                         (name_obj.get(self.language) if hasattr(name_obj, 'get') else None) or
                         str(name_obj) if name_obj else code)
                desc_obj = getattr(df_def, 'description', None)
                description = (getattr(desc_obj, self.language, None) or
                              (desc_obj.get(self.language) if hasattr(desc_obj, 'get') else None) or
                              str(desc_obj) if desc_obj else '') or ''
                ver = getattr(df_def, 'version', None)
                version = str(ver) if ver is not None else '1.0'

                datasets.append({
                    'code': code,
                    'title': title or code,
                    'description': description or '',
                    'version': version or '1.0',
                    'download_link': '',
                })

                if limit is not None and len(datasets) >= limit:
                    break

            logger.info("Found %d datasets in Eurostat SDMX catalogue", len(datasets))
            return datasets

        except Exception as e:
            logger.error("Error fetching Eurostat SDMX catalogue: %s", e)
            import traceback
            traceback.print_exc()
            return []
    
    def get_dataset_structure(self, dataset_code: str) -> Optional[Dict]:
        """
        Get structure (dimensions, codes) for a dataset via SDMX datastructure.

        Args:
            dataset_code: Dataset code (e.g., 'tps00001')

        Returns:
            Dictionary with dataset structure information
        """
        try:
            # ESTAT uses references="descendants" (not "all")
            sm = self.client.datastructure(dataset_code, params={"references": "descendants"})
            dimensions = []

            for dsd_id, dsd in sm.structure.items():
                if not hasattr(dsd, 'dimensions') or not dsd.dimensions:
                    continue
                for dim in dsd.dimensions.components:
                    ci = getattr(dim, 'concept_identity', None)
                    if ci is None:
                        continue
                    dim_id = getattr(ci, 'id', str(dim))
                    dim_name = getattr(getattr(ci, 'name', None), self.language, None) or dim_id
                    codes = []
                    try:
                        lr = getattr(dim, 'local_representation', None)
                        if lr is not None:
                            cl_ref = getattr(lr, 'enumerated', None) or getattr(lr, 'enumeration', None)
                            if cl_ref is not None:
                                cl = sm.get(str(cl_ref))
                                if cl is not None and hasattr(cl, 'items'):
                                    for item_id, item in cl.items.items():
                                        item_name = getattr(getattr(item, 'name', None), self.language, None) or str(getattr(item, 'name', item_id))
                                        codes.append({'code': item_id, 'name': item_name})
                    except Exception:
                        pass
                    dimensions.append({'id': dim_id, 'name': dim_name, 'codes': codes})

            return {'dataset_code': dataset_code, 'dimensions': dimensions}

        except Exception as e:
            logger.warning("Could not get full structure for %s: %s", dataset_code, e)
            return {'dataset_code': dataset_code, 'dimensions': []}
    
    def download_dataset(self, dataset_code: str,
                        filters: Optional[Dict[str, str]] = None,
                        params: Optional[Dict] = None,
                        save_to_db: bool = True) -> Optional[pd.DataFrame]:
        """
        Download a specific Eurostat dataset and optionally save to PostgreSQL.

        Args:
            dataset_code: Dataset code (e.g., 'tps00001')
            filters: Dictionary of dimension filters (e.g., {'geo': 'DE'})
            params: Additional query parameters (e.g. startPeriod, endPeriod)
            save_to_db: Whether to save to PostgreSQL database (default: True)

        Returns:
            DataFrame with the dataset or None if error
        """
        try:
            # Build key from filters; ESTAT expects dimension IDs
            key = dict(filters) if filters else None
            req_params = dict(params) if params else {}

            dm = self.client.data(dataset_code, key=key, params=req_params)
            result = sdmx.to_pandas(dm)

            # sdmx.to_pandas returns Series with MultiIndex; convert to DataFrame
            if isinstance(result, pd.Series):
                df = result.reset_index()
                # Ensure value column has a consistent name
                if df.columns[-1] != 'value' and 'value' not in df.columns:
                    df = df.rename(columns={df.columns[-1]: 'value'})
            else:
                df = result

            if df is not None and not df.empty:
                if save_to_db:
                    # Save to PostgreSQL
                    conn = self._get_pg_connection()
                    time_series_list = []
                    
                    # Eurostat DataFrames are multi-dimensional
                    # Typically: index = time, columns = other dimensions (can be multi-index)
                    # Reset index to make time a column if it's in the index
                    df_to_save = df.copy()
                    
                    # Check if index is time-based
                    time_in_index = False
                    if isinstance(df_to_save.index, pd.DatetimeIndex):
                        time_in_index = True
                        df_to_save = df_to_save.reset_index()
                        time_col = df_to_save.columns[0]
                    elif df_to_save.index.name and 'time' in str(df_to_save.index.name).lower():
                        time_in_index = True
                        df_to_save = df_to_save.reset_index()
                        time_col = df_to_save.columns[0]
                    else:
                        # Try to find time column (SDMX uses TIME_PERIOD)
                        time_col = None
                        for col in df_to_save.columns:
                            if 'time' in str(col).lower() or 'period' in str(col).lower():
                                time_col = col
                                break

                    # Find value column
                    value_col = 'value' if 'value' in df_to_save.columns else df_to_save.columns[-1]

                    if time_in_index or time_col:
                        # SDMX long format: columns are (dim1, dim2, ..., time, value)
                        # or wide format needing melt
                        is_long_format = time_col and value_col in df_to_save.columns and time_col != value_col

                        if is_long_format:
                            dim_cols = [c for c in df_to_save.columns if c not in (time_col, value_col)]
                            for _, row in df_to_save.iterrows():
                                date_val = row[time_col]
                                date_str = self._parse_eurostat_date(date_val)
                                if not date_str:
                                    continue
                                val = row.get(value_col)
                                if pd.isna(val):
                                    continue
                                dimensions = {str(c): str(row[c]) for c in dim_cols}
                                time_series_list.append({
                                    'dataset_code': dataset_code,
                                    'date': date_str,
                                    'value': float(val),
                                    'dimensions': dimensions
                                })
                        else:
                            # Wide format: melt
                            id_vars = [time_col] if time_col else []
                            df_melted = df_to_save.melt(id_vars=id_vars, var_name='dimensions', value_name='value')
                            for _, row in df_melted.iterrows():
                                date_str = self._parse_eurostat_date(row.get(time_col)) if time_col else None
                                if not date_str:
                                    continue
                                dim_col = row.get('dimensions', '')
                                dimensions = {}
                                if isinstance(dim_col, tuple):
                                    for i, dim_val in enumerate(dim_col):
                                        dimensions[f'dim_{i}'] = str(dim_val)
                                else:
                                    dimensions['dimension'] = str(dim_col)
                                value = row.get('value')
                                if pd.notna(value):
                                    time_series_list.append({
                                        'dataset_code': dataset_code,
                                        'date': date_str,
                                        'value': float(value),
                                        'dimensions': dimensions
                                    })
                    else:
                        # Fallback: treat index as time, columns as dimensions
                        for idx, row in df.iterrows():
                            date_str = self._parse_eurostat_date(idx)
                            if not date_str:
                                continue
                            
                            for col in df.columns:
                                value = row[col]
                                if pd.notna(value):
                                    dimensions = {}
                                    if isinstance(col, tuple):
                                        for i, dim_val in enumerate(col):
                                            dimensions[f'dim_{i}'] = str(dim_val)
                                    else:
                                        dimensions['dimension'] = str(col)
                                    
                                    time_series_list.append({
                                        'dataset_code': dataset_code,
                                        'date': date_str,
                                        'value': float(value),
                                        'dimensions': dimensions
                                    })
                    
                    if time_series_list:
                        add_time_series_fast(conn, time_series_list)
                        logger.info("  Saved %d data points to database for %s", len(time_series_list), dataset_code)
                
                return df
            else:
                logger.warning("No data returned for %s", dataset_code)
                return None
                
        except Exception as e:
            logger.error("Error downloading %s: %s", dataset_code, e)
            import traceback
            traceback.print_exc()
            return None
    
    def get_all_downloadable_series(self, limit: Optional[int] = None,
                                    write_interval: int = 50) -> List[Dict[str, str]]:
        """
        Retrieve all downloadable Eurostat datasets and save to PostgreSQL database incrementally
        
        Args:
            limit: Maximum number of datasets to retrieve (None for all)
            write_interval: Number of datasets to collect before writing to PostgreSQL database
            
        Returns:
            List of dictionaries with dataset information
        """
        logger.info("=" * 60)
        logger.info("Retrieving all downloadable Eurostat datasets...")
        logger.info("=" * 60)
        
        all_datasets_info = []
        dataset_count_at_last_write = 0
        
        # Initialize PostgreSQL connection
        conn = self._get_pg_connection()
        update_eurostat_metadata(conn, 'status', 'in_progress')
        update_eurostat_metadata(conn, 'generated_at', datetime.now().isoformat())
        logger.info("Connected to PostgreSQL database: %s", self.dbname)
        
        def write_to_db_incrementally(show_status=True):
            """Helper function to write current state to PostgreSQL database"""
            if all_datasets_info:
                try:
                    # Get only new datasets since last write
                    new_datasets = all_datasets_info[dataset_count_at_last_write:]
                    if not new_datasets:
                        return
                    
                    # Convert dataset info to database format
                    db_datasets = []
                    for dataset in new_datasets:
                        # Convert dimensions list to string for keywords
                        dimensions = dataset.get('dimensions', [])
                        keywords = ', '.join([d.get('id', '') for d in dimensions]) if dimensions else ''
                        
                        db_datasets.append({
                            'dataset_code': dataset.get('code', ''),
                            'title': dataset.get('title', ''),
                            'description': dataset.get('description', ''),
                            'last_update': dataset.get('version', ''),
                            'frequency': '',  # Eurostat doesn't always provide this in catalog
                            'theme': '',
                            'keywords': keywords,
                            'created_at': datetime.now().isoformat(),
                            'updated_at': datetime.now().isoformat(),
                        })
                    
                    if db_datasets:
                        add_eurostat_datasets_fast(conn, db_datasets)
                        update_eurostat_metadata(conn, 'total_datasets', str(len(all_datasets_info)))
                    
                    if show_status:
                        status_msg = f"  [{len(all_datasets_info)} datasets] Progress saved to database"
                        if limit:
                            status_msg += f" ({len(all_datasets_info)}/{limit})"
                        logger.info("%s", status_msg)
                except Exception as e:
                    logger.warning("Error writing to database: %s", e)
        
        try:
            # Get all datasets
            logger.info("Fetching dataset catalog from Eurostat...")
            datasets = self.get_all_datasets(limit=limit)
            
            if not datasets:
                logger.warning("No datasets found")
                return []
            
            logger.info("Processing %d datasets...", len(datasets))
            
            # Progress bar
            pbar = tqdm(datasets, desc="Cataloging datasets", unit="dataset", leave=True)
            for dataset in pbar:
                code = dataset.get('code', '')
                title = dataset.get('title', '')
                
                pbar.set_postfix({'code': code[:20], 'datasets': len(all_datasets_info)})
                
                # Try to get additional metadata
                try:
                    structure = self.get_dataset_structure(code)
                    dimensions = structure.get('dimensions', []) if structure else []
                except:
                    dimensions = []
                    time.sleep(0.1)  # Rate limiting
                
                dataset_info = {
                    'code': code,
                    'title': title,
                    'description': dataset.get('description', ''),
                    'version': dataset.get('version', '1.0'),
                    'dimensions': dimensions,
                }
                
                all_datasets_info.append(dataset_info)
                
                # Write to database incrementally
                if len(all_datasets_info) - dataset_count_at_last_write >= write_interval:
                    write_to_db_incrementally()
                    dataset_count_at_last_write = len(all_datasets_info)
                
                time.sleep(0.1)  # Rate limiting
            
            pbar.close()
            
            # Final save to database (mark as complete)
            if all_datasets_info:
                new_datasets = all_datasets_info[dataset_count_at_last_write:]
                if new_datasets:
                    db_datasets = []
                    for dataset in new_datasets:
                        dimensions = dataset.get('dimensions', [])
                        keywords = ', '.join([d.get('id', '') for d in dimensions]) if dimensions else ''
                        db_datasets.append({
                            'dataset_code': dataset.get('code', ''),
                            'title': dataset.get('title', ''),
                            'description': dataset.get('description', ''),
                            'last_update': dataset.get('version', ''),
                            'frequency': '',
                            'theme': '',
                            'keywords': keywords,
                            'created_at': datetime.now().isoformat(),
                            'updated_at': datetime.now().isoformat(),
                        })
                    if db_datasets:
                        add_eurostat_datasets_fast(conn, db_datasets)
            
            update_eurostat_metadata(conn, 'total_datasets', str(len(all_datasets_info)))
            update_eurostat_metadata(conn, 'status', 'complete')
            logger.info("Eurostat database finalized: %s", self.dbname)
            logger.info("  Total datasets: %d", len(all_datasets_info))
            
            return all_datasets_info
            
        except Exception as e:
            logger.error("Error retrieving datasets: %s", e)
            import traceback
            traceback.print_exc()
            return []
    
    def load_datasets_from_db(self) -> List[str]:
        """
        Load dataset codes from PostgreSQL database
            
        Returns:
            List of dataset codes
        """
        try:
            conn = self._get_pg_connection()
            datasets_list = load_eurostat_datasets_from_postgres(conn)
            dataset_codes = [d.get('dataset_code') for d in datasets_list if d.get('dataset_code')]
            logger.info("Loaded %d dataset codes from PostgreSQL database", len(dataset_codes))
            return dataset_codes
        except Exception as e:
            logger.error("Error loading datasets from database: %s", e)
            return []
    
    def download_datasets_from_db(self,
                                   dataset_codes: Optional[List[str]] = None,
                                   filters: Optional[Dict[str, Dict[str, str]]] = None,
                                   save_to_db: bool = True) -> Dict[str, pd.DataFrame]:
        """
        Download datasets from PostgreSQL database
        
        Args:
            dataset_codes: Optional list of dataset codes to download (if None, downloads all from DB)
            filters: Optional dictionary mapping dataset codes to filter dictionaries
            save_to_db: Whether to save time series data to PostgreSQL (default: True)
            
        Returns:
            Dictionary mapping dataset codes to DataFrames
        """
        if dataset_codes is None:
            dataset_codes = self.load_datasets_from_db()
        
        if not dataset_codes:
            logger.warning("No dataset codes found in database")
            return {}
        
        logger.info("Downloading %d datasets and saving to database...", len(dataset_codes))
        
        downloaded_dfs = {}
        pbar = tqdm(dataset_codes, desc="Downloading datasets", unit="dataset", leave=True)
        
        for dataset_code in pbar:
            pbar.set_description(f"Downloading {dataset_code}")
            
            # Get filters for this dataset if provided
            dataset_filters = filters.get(dataset_code) if filters else None
            
            try:
                data = self.download_dataset(dataset_code, filters=dataset_filters, save_to_db=save_to_db)
                
                if data is not None and not data.empty:
                    downloaded_dfs[dataset_code] = data
                
                time.sleep(0.5)  # Rate limiting
                
            except Exception as e:
                pbar.write(f"Error downloading {dataset_code}: {e}")
                time.sleep(0.5)
        
        pbar.close()
        
        logger.info("Successfully downloaded %d/%d datasets.", len(downloaded_dfs), len(dataset_codes))
        logger.info("All data saved to PostgreSQL database")
        return downloaded_dfs

