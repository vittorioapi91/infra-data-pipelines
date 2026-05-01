"""
FRED Economic Data Downloader

This module provides functionality to download time series data from FRED (Federal Reserve Economic Data).
"""

import csv
import logging
import os
import time
import ssl
from pathlib import Path
from typing import Callable, List, Dict, Optional, Set, Union
import pandas as pd
from fredapi import Fred
import requests
import urllib.request
import urllib.error
import warnings
from datetime import datetime, timedelta

from tqdm import tqdm

# Import FRED PostgreSQL utilities
try:
    from .fred_postgres import (
        get_postgres_connection, init_fred_postgres_tables, apply_fred_views,
        add_fred_series_fast, load_fred_series_from_postgres,
        update_fred_metadata, add_fred_categories_fast,
        load_fred_categories_from_postgres, add_time_series_fast,
        load_time_series_from_postgres, get_max_date_for_series,
        get_series_updated_at, get_series_updated_at_bulk, update_series_updated_at,
    )
except ImportError:
    from src.macro.fred.fred_postgres import (
        get_postgres_connection, init_fred_postgres_tables, apply_fred_views,
        add_fred_series_fast, load_fred_series_from_postgres,
        update_fred_metadata, add_fred_categories_fast,
        load_fred_categories_from_postgres, add_time_series_fast,
        load_time_series_from_postgres, get_max_date_for_series,
        get_series_updated_at, get_series_updated_at_bulk, update_series_updated_at,
    )

# Try to import certifi for proper SSL certificates
try:
    import certifi
    HAS_CERTIFI = True
except ImportError:
    HAS_CERTIFI = False
    certifi = None

warnings.filterwarnings('ignore')

logger = logging.getLogger(__name__)


class FREDDataDownloader:
    """Class to download economic time series data from FRED"""
    
    def __init__(self, api_key: Optional[str] = None, verify_ssl: bool = None):
        """
        Initialize FRED data downloader

        Connection details and API key are loaded from environment variables.
        Requires: FRED_API_KEY, POSTGRES_USER, POSTGRES_HOST, POSTGRES_PASSWORD, POSTGRES_PORT.

        Args:
            api_key: FRED API key. If not provided, uses FRED_API_KEY env var (required).
            verify_ssl: Whether to verify SSL certificates. If None, tries certifi if available.
        """
        self.api_key = api_key or os.getenv('FRED_API_KEY')
        if not self.api_key:
            raise ValueError("FRED_API_KEY is required (set in .env or environment)")

        # Load PostgreSQL connection from env (no defaults)
        self.dbname = 'fred'
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

        # Handle SSL verification
        if verify_ssl is None:
            # Auto-detect: use certifi if available, otherwise disable verification
            verify_ssl = HAS_CERTIFI
        
        self.verify_ssl = verify_ssl
        
        # Set up SSL context
        if verify_ssl and HAS_CERTIFI:
            try:
                # Create SSL context with certifi certificates
                ssl_context = ssl.create_default_context(cafile=certifi.where())
                # Create HTTPS handler with the SSL context
                https_handler = urllib.request.HTTPSHandler(context=ssl_context)
                opener = urllib.request.build_opener(https_handler)
                urllib.request.install_opener(opener)
            except Exception as e:
                logger.warning("Could not set up SSL context with certifi: %s", e)
                logger.warning("Falling back to unverified SSL context")
                verify_ssl = False
        
        if not verify_ssl:
            # Disable SSL verification by creating an unverified context
            try:
                ssl_context = ssl.create_default_context()
                ssl_context.check_hostname = False
                ssl_context.verify_mode = ssl.CERT_NONE
                https_handler = urllib.request.HTTPSHandler(context=ssl_context)
                opener = urllib.request.build_opener(https_handler)
                urllib.request.install_opener(opener)
                # Also disable SSL warnings for urllib3 (used by requests)
                import urllib3
                urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
            except Exception as e:
                logger.warning("Could not set up unverified SSL context: %s", e)
        
        # Initialize Fred client
        self.fred = Fred(api_key=self.api_key)
        self._pg_conn = None
        self._last_fred_request_time = 0.0
        self._fred_min_interval = 0.6  # 120 req/min = 2/sec, use 0.6s to stay under limit

    def _fred_request(self, url: str, params: Dict, timeout: int = 10, max_retries: int = 3) -> requests.Response:
        """Make FRED API request with rate limiting and 429 retry."""
        last_backoff = 0
        for attempt in range(max_retries + 1):
            elapsed = time.monotonic() - self._last_fred_request_time
            if elapsed < self._fred_min_interval:
                time.sleep(self._fred_min_interval - elapsed)
            self._last_fred_request_time = time.monotonic()

            response = requests.get(url, params=params, timeout=timeout, verify=self.verify_ssl)
            if response.status_code != 429:
                return response
            if attempt < max_retries:
                backoff = 60 + (attempt * 30)  # 60s, 90s, 120s
                logger.warning("Rate limited (429). Waiting %ds before retry %d/%d...", backoff, attempt + 1, max_retries)
                time.sleep(backoff)
        return response

    def _get_pg_connection(self):
        """Get or create PostgreSQL connection"""
        if self._pg_conn is None or self._pg_conn.closed:
            self._pg_conn = get_postgres_connection(
                user=self.user,
                host=self.host,
                password=self.password,
                port=self.port
            )
            init_fred_postgres_tables(self._pg_conn)
        return self._pg_conn
        
    def get_all_series_ids(self, limit: Optional[int] = None, search_text: str = '') -> List[str]:
        """
        Get all available FRED series IDs
        
        Args:
            limit: Maximum number of series to return (None for all)
            search_text: Optional search text to filter series
            
        Returns:
            List of series IDs
        """
        logger.info("Fetching FRED series IDs (search: '%s')...", search_text)
        
        try:
            series_ids = []
            
            # Use FRED's search functionality
            if search_text:
                # Search for series with specific text
                try:
                    search_results = self.fred.search(search_text, order_by='popularity', limit=limit or 1000)
                    if isinstance(search_results, pd.DataFrame) and not search_results.empty:
                        series_ids = search_results.index.tolist()
                except Exception as e:
                    logger.error("Error searching: %s", e)
            else:
                # To get "all" series, we need to explore multiple approaches
                # FRED doesn't provide a direct "get all" endpoint
                # Strategy: Search with common economic terms and explore categories
                
                # Approach 1: Search with common economic keywords to discover series
                # This is the most reliable method as FRED's search API is well-supported
                search_terms = [
                    '',  # Empty search returns popular series
                    'GDP', 'unemployment', 'inflation', 'interest rate',
                    'employment', 'production', 'consumer', 'price',
                    'money', 'banking', 'trade', 'exchange rate',
                    'industrial', 'retail', 'housing', 'manufacturing',
                    'income', 'sales', 'index', 'rate', 'percent',
                    'billion', 'million', 'thousand', 'dollar'
                ]
                
                logger.info("Searching FRED with common economic terms...")
                for i, term in enumerate(search_terms):
                    try:
                        search_limit = min(1000, limit - len(set(series_ids)) if limit else 1000)
                        if search_limit <= 0:
                            break
                            
                        results = self.fred.search(term, order_by='popularity', limit=search_limit)
                        if isinstance(results, pd.DataFrame) and not results.empty:
                            new_series = results.index.tolist()
                            series_ids.extend(new_series)
                            unique_count = len(set(series_ids))
                            logger.info("  Term '%s': +%d series (total: %d)", term, len(new_series), unique_count)
                        
                        time.sleep(0.2)  # Rate limiting (FRED allows 120 req/min)
                    except Exception as e:
                        logger.warning("Error searching with term '%s': %s", term, e)
                        continue
                
                # Approach 2: Try to get popular series without search terms
                # Some fredapi versions support getting popular series directly
                try:
                    popular_results = self.fred.search('', order_by='popularity', limit=5000)
                    if isinstance(popular_results, pd.DataFrame) and not popular_results.empty:
                        series_ids.extend(popular_results.index.tolist())
                except:
                    pass  # Not all versions support this
            
            # Remove duplicates and apply limit
            series_ids = list(set(series_ids))
            if limit:
                series_ids = series_ids[:limit]
            
            logger.info("Found %d unique series", len(series_ids))
            return series_ids
            
        except Exception as e:
            logger.error("Error fetching series IDs: %s", e)
            return []
    
    def _get_known_categories(self) -> List[int]:
        """
        Get known major FRED category IDs (fast - no API calls)
        
        Returns:
            List of known major category IDs
        """
        # FRED's major economic categories - these are well-known and don't require API calls
        major_categories = [
            1,      # National Accounts
            3,      # Production & Business Activity
            5,      # Additional category
            6,      # Additional category
            9,      # Additional category
            10,     # Money, Banking, & Finance
            11,     # Additional category
            12,     # Additional category
            13,     # Population, Employment, & Labor Markets
            15,     # Additional category
            18,     # Additional category
            22,     # Additional category
            24,     # Additional category
            31,     # Additional category
            32,     # Prices
            46,     # Additional category
            50,     # International Data
            94,     # Additional category
            95,     # Additional category
            106,    # Academic Data
            115,    # Additional category
            120,    # Additional category
            398,    # Additional category
            32217,  # Additional category
            32255,  # US Rig Count
            32263,  # Additional category
            32446,  # Additional category
            32455,  # Additional category
            33060,  # Additional category
            33061,  # Additional category
            33939,  # Additional category
            33951,  # Additional category
        ]
        return sorted(major_categories)  # Sort for easier reading
    
    def _get_all_categories(self) -> List[int]:
        """
        Recursively get all FRED category IDs starting from root category (0)
        Saves category tree to PostgreSQL database
        
        Returns:
            List of all category IDs
        """
        all_cats: Set[int] = set()
        visited: Set[int] = set()
        categories_info: List[Dict] = []
        categories_saved_count = 0
        base_url = "https://api.stlouisfed.org/fred/category/children"
        category_info_url = "https://api.stlouisfed.org/fred/category"
        batch_save_interval = 50  # Save every 50 categories
        conn = self._get_pg_connection()
        pbar_cats = tqdm(desc="Discovering categories", unit="cat", leave=True)
        
        def save_categories_batch() -> None:
            """Helper to save categories in batch"""
            nonlocal categories_saved_count
            if categories_info:
                try:
                    # Get only new categories since last save
                    new_categories = categories_info[categories_saved_count:]
                    if new_categories:
                        added = add_fred_categories_fast(conn, new_categories)
                        categories_saved_count = len(categories_info)
                except Exception as e:
                    pass  # Will save at end
        
        def get_category_children_recursive(category_id: int, parent_id: Optional[int] = None,
                                            category_name: Optional[str] = None) -> None:
            """
            Recursively fetch all child categories starting from a parent category.
            Saves the ENTIRE category tree including all parent and child categories.
            category_name: if provided (from parent's children response), skip the category info API call.
            """
            # Avoid infinite loops
            if category_id in visited:
                return
            visited.add(category_id)
            all_cats.add(category_id)
            
            try:
                # Only call category info API when we don't have the name (root, or first visit).
                # Children response already includes id, name, parent_id per child.
                if category_name is None:
                    cat_params = {
                        'category_id': category_id,
                        'api_key': self.api_key,
                        'file_type': 'json'
                    }
                    cat_response = self._fred_request(category_info_url, cat_params, timeout=10)
                    category_name = f'Category {category_id}'
                    if cat_response.status_code == 200:
                        cat_data = cat_response.json()
                        if 'categories' in cat_data and cat_data['categories']:
                            category_name = cat_data['categories'][0].get('name', category_name)
                else:
                    category_name = category_name or f'Category {category_id}'
                
                # Create category info dict - this saves the current category
                cat_info = {
                    'category_id': str(category_id),
                    'name': category_name,
                    'parent_id': int(parent_id) if parent_id is not None and parent_id != '' else 0,
                    'created_at': datetime.now().isoformat(),
                    'updated_at': datetime.now().isoformat(),
                }
                
                # Add to list (will be saved in batch)
                categories_info.append(cat_info)
                pbar_cats.update(1)
                pbar_cats.set_postfix(n=len(all_cats), refresh=False)

                parent_str = str(parent_id) if parent_id is not None else 'None'
                logger.debug("  Discovered category %s (parent %s): %s", category_id, parent_str, category_name)
                
                # Batch save periodically
                if len(categories_info) - categories_saved_count >= batch_save_interval:
                    save_categories_batch()
                
                # Use FRED API REST endpoint to get category children
                params = {
                    'category_id': category_id,
                    'api_key': self.api_key,
                    'file_type': 'json'
                }
                
                response = self._fred_request(base_url, params, timeout=10)
                response.raise_for_status()
                
                data = response.json()
                
                # Check if we have categories in the response
                if 'categories' in data and data['categories']:
                    # For each child category, recursively process it. Pass child name from response
                    # to avoid an extra API call per category (children already include name).
                    for category in data['categories']:
                        child_id = category.get('id')
                        if child_id:
                            child_name = category.get('name') or f'Category {child_id}'
                            get_category_children_recursive(child_id, parent_id=category_id, category_name=child_name)
                            
            except requests.exceptions.RequestException as e:
                logger.warning("Error fetching children of category %s: %s", category_id, e)
            except Exception as e:
                logger.warning("Unexpected error for category %s: %s", category_id, e)
        
        logger.info("Fetching all FRED categories recursively...")
        # Start from root category (0) which contains all top-level categories
        get_category_children_recursive(0, parent_id=None)
        pbar_cats.close()
        logger.info("Found %d total categories", len(all_cats))
        
        # Final batch save to ensure all categories are saved
        if categories_info:
            try:
                save_categories_batch()  # Save any remaining
                # Also do a final full save to ensure nothing is missed
                add_fred_categories_fast(conn, categories_info)
                logger.info("  Saved %d categories to PostgreSQL (complete tree)", len(categories_info))
            except Exception as e:
                logger.warning("Error saving categories to PostgreSQL: %s", e)
        
        return sorted(list(all_cats))
    
    def _extract_country_from_series(self, series_id: str, series_data: Dict, geography: str = '') -> str:
        """
        Extract country information from series ID or metadata
        
        Args:
            series_id: Series ID
            series_data: Series data dictionary
            geography: Geography field from series data
            
        Returns:
            Country name
        """
        # Common country codes in FRED series IDs
        country_codes = {
            'US': 'United States',
            'USA': 'United States',
            'AUS': 'Australia',
            'CAN': 'Canada',
            'CHN': 'China',
            'DEU': 'Germany',
            'FRA': 'France',
            'GBR': 'United Kingdom',
            'ITA': 'Italy',
            'JPN': 'Japan',
            'MEX': 'Mexico',
            'BRA': 'Brazil',
            'IND': 'India',
            'KOR': 'South Korea',
            'RUS': 'Russia',
            'ZAF': 'South Africa',
        }
        
        # Try to get from geography field
        if geography:
            geography_upper = geography.upper()
            for code, country in country_codes.items():
                if code in geography_upper:
                    return country
        
        # Try to extract from series ID (common patterns: country code at end)
        series_id_upper = series_id.upper()
        for code, country in country_codes.items():
            if series_id_upper.endswith(code) or code in series_id_upper:
                return country
        
        # Check if it's a US state code (2-letter codes at end)
        if len(series_id) >= 2:
            last_two = series_id[-2:].upper()
            # Common US state codes
            us_states = ['AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA',
                        'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD',
                        'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ',
                        'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC',
                        'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY']
            if last_two in us_states:
                return 'United States'
        
        # Default to United States for most FRED series
        return 'United States'
    
    def _is_rate_limit_error(self, e: Exception) -> bool:
        """True if the exception indicates FRED API rate limit (429)."""
        msg = str(e).lower()
        return '429' in msg or 'too many' in msg or 'rate limit' in msg or 'exceeded' in msg

    def download_series(self, series_id: str, start_date: Optional[str] = None,
                       end_date: Optional[str] = None, save_to_db: bool = True,
                       run_log_cb: Optional[Callable[[str, int], None]] = None,
                       max_rate_limit_retries: int = 5) -> Optional[pd.Series]:
        """
        Download a single FRED time series and optionally save to PostgreSQL.

        When start_date and end_date are None (default):
        - start: first available from FRED, or first date after existing DB data (incremental)
        - end: last available on FRED (current date for active series)

        On 429 / rate limit, waits with backoff and retries until success or max_rate_limit_retries.

        Args:
            series_id: FRED series ID
            start_date: Start date (YYYY-MM-DD). None = first available or first after DB.
            end_date: End date (YYYY-MM-DD). None = last available (e.g. today for active).
            save_to_db: Whether to save to PostgreSQL database (default: True)
            run_log_cb: Optional callback (series_id, n_saved) when data is saved.
            max_rate_limit_retries: Max retries on rate limit (default 5).

        Returns:
            pandas Series with the time series data, or None on failure.
        """
        last_error = None
        for attempt in range(max_rate_limit_retries + 1):
            try:
                _start = start_date
                _end = end_date
                if _start is None and save_to_db:
                    conn = self._get_pg_connection()
                    max_date = get_max_date_for_series(conn, series_id)
                    if max_date:
                        dt = datetime.strptime(max_date, '%Y-%m-%d') + timedelta(days=1)
                        _start = dt.strftime('%Y-%m-%d')
                data = self.fred.get_series(series_id, start=_start, end=_end)
                if data is not None and not data.empty:
                    data = pd.to_numeric(
                        data.replace(r'^\s*$', pd.NA, regex=True),
                        errors='coerce',
                    )

                time_series_list = []
                if save_to_db and data is not None and not data.empty:
                    # Save to PostgreSQL
                    conn = self._get_pg_connection()
                    for date, value in data.items():
                        time_series_list.append({
                            'series_id': series_id,
                            'date': date.strftime('%Y-%m-%d') if hasattr(date, 'strftime') else str(date),
                            'value': float(value) if pd.notna(value) else None
                        })
                    
                if time_series_list:
                    add_time_series_fast(conn, time_series_list)
                    update_series_updated_at(conn, series_id)
                    n_saved = len(time_series_list)
                    logger.debug("  Saved %d data points to database for %s", n_saved, series_id)
                    if run_log_cb:
                        run_log_cb(series_id, n_saved)
                
                return data
            except Exception as e:
                last_error = e
                if self._is_rate_limit_error(e) and attempt < max_rate_limit_retries:
                    backoff = 60 + (attempt * 30)  # 60s, 90s, 120s, ...
                    logger.warning("Rate limited for %s. Waiting %ds before retry %d/%d...", series_id, backoff, attempt + 1, max_rate_limit_retries)
                    time.sleep(backoff)
                else:
                    logger.error("Error downloading series %s: %s", series_id, e)
                    return None
        logger.error("Error downloading series %s (max retries): %s", series_id, last_error)
        return None
    
    def download_multiple_series(self, series_ids: List[str], 
                                 start_date: Optional[str] = None,
                                 end_date: Optional[str] = None,
                                 save_to_db: bool = True) -> pd.DataFrame:
        """
        Download multiple FRED time series and combine into a DataFrame
        
        Args:
            series_ids: List of FRED series IDs to download
            start_date: Start date (YYYY-MM-DD format)
            end_date: End date (YYYY-MM-DD format)
            save_to_db: Whether to save to PostgreSQL database (default: True)
            
        Returns:
            DataFrame with all series as columns
        """
        logger.info("Downloading %d series...", len(series_ids))
        
        all_data = {}
        failed_series = []
        
        for i, series_id in enumerate(series_ids):
            try:
                logger.info("Downloading %d/%d: %s", i+1, len(series_ids), series_id)
                data = self.download_series(series_id, start_date, end_date, save_to_db=save_to_db)
                
                if data is not None and not data.empty:
                    all_data[series_id] = data
                else:
                    failed_series.append(series_id)
                
                # Respect API rate limits (FRED allows 120 requests per minute)
                time.sleep(0.5)
                
            except Exception as e:
                logger.error("Error downloading %s: %s", series_id, e)
                failed_series.append(series_id)
                continue
        
        logger.info("Successfully downloaded %d/%d series", len(all_data), len(series_ids))
        if failed_series:
            logger.warning("Failed to download %d series: %s...", len(failed_series), failed_series[:10])
        
        # Combine into DataFrame
        if all_data:
            df = pd.DataFrame(all_data)
            df.index.name = 'date'
            
            return df
        else:
            logger.warning("No data was successfully downloaded")
            return pd.DataFrame()
    
    def download_all_available_series(self, start_date: Optional[str] = None,
                                      end_date: Optional[str] = None,
                                      limit: Optional[int] = None) -> Dict[str, pd.DataFrame]:
        """
        Download all available FRED economic variables and save to PostgreSQL
        
        Args:
            start_date: Start date for time series (YYYY-MM-DD format)
            end_date: End date for time series (YYYY-MM-DD format)
            limit: Maximum number of series to download (None for all)
            
        Returns:
            Dictionary mapping series IDs to DataFrames
        """
        logger.info("=" * 60)
        logger.info("FRED Economic Data Downloader")
        logger.info("=" * 60)
        
        # Get all series IDs
        series_ids = self.get_all_series_ids(limit=limit)
        
        if not series_ids:
            logger.warning("No series found to download")
            return {}
        
        logger.info("Starting download of %d series...", len(series_ids))
        logger.info("All data will be saved to PostgreSQL database")
        
        all_data = {}
        successful = 0
        failed = 0
        
        # Download all series
        for i, series_id in enumerate(series_ids):
            try:
                logger.info("Downloading %d/%d: %s", i+1, len(series_ids), series_id)
                data = self.download_series(series_id, start_date=start_date, end_date=end_date, save_to_db=True)
                
                if data is not None and not data.empty:
                    df = data.to_frame(name=series_id)
                    df.index.name = 'date'
                    all_data[series_id] = df
                    successful += 1
                else:
                    failed += 1
                
                time.sleep(0.5)  # Rate limiting
                
            except Exception as e:
                logger.error("Error downloading %s: %s", series_id, e)
                failed += 1
                continue
        
        logger.info("Download complete!")
        logger.info("  Successfully downloaded: %d series", successful)
        logger.info("  Failed: %d series", failed)
        logger.info("  All data saved to PostgreSQL database")
        
        return all_data
    
    def get_all_downloadable_series(self, limit: Optional[int] = None,
                                     use_categories: bool = True,
                                     use_known_categories_only: bool = True,
                                     use_search_terms: bool = False,
                                     write_interval: int = 50,
                                     category_roots: Optional[List[int]] = None,
                                     catalog_output_dir: Optional[Union[str, Path]] = None) -> List[Dict[str, str]]:
        """
        Retrieve all downloadable FRED series and save to PostgreSQL database incrementally
        
        Args:
            limit: Maximum number of series to retrieve (None for all)
            use_categories: If True, explore categories to find series. If False, use search terms only.
            use_known_categories_only: If True, use a limited set of root categories (fast). If False, recursively explore all categories (slow).
            use_search_terms: If True, also search using economic terms to find additional series (slower but more comprehensive).
            write_interval: Number of series to collect before writing to database
            category_roots: Optional list of FRED category IDs to use as roots when use_known_categories_only=True.
                            If provided, they replace the built-in "known categories" list.
            
        catalog_output_dir: If set, save catalog to disk as CSV at each incremental write (replace).
                           Path: {catalog_output_dir}/fred_series_master.csv

        Returns:
            List of dictionaries with series information (id, title, etc.)
        """
        catalog_path = Path(catalog_output_dir) if catalog_output_dir else None

        logger.info("=" * 60)
        logger.info("Retrieving all downloadable FRED series...")
        logger.info("=" * 60)
        
        all_series_info = []
        series_ids_set: Set[str] = set()
        series_count_at_last_write = 0
        
        # Initialize PostgreSQL connection and refresh views first
        conn = self._get_pg_connection()
        apply_fred_views(conn)
        logger.info("Refreshed FRED views (category_paths, category_analysis)")
        update_fred_metadata(conn, 'status', 'in_progress')
        update_fred_metadata(conn, 'generated_at', datetime.now().isoformat())
        logger.info("Connected to PostgreSQL database: %s", self.dbname)
        if catalog_path:
            logger.info("Catalog will be saved to: %s", catalog_path / 'fred_series_master.csv')

        def _save_catalog_to_csv(series_info: List[Dict], output_dir: Path) -> None:
            """Save catalog to CSV, replacing the file."""
            output_dir = Path(output_dir)
            output_dir.mkdir(parents=True, exist_ok=True)
            path = output_dir / 'fred_series_master.csv'
            columns = [
                'id', 'title', 'description', 'units', 'frequency', 'seasonal_adjustment',
                'observation_start', 'observation_end', 'country', 'last_updated',
                'popularity', 'category_id', 'category_name'
            ]
            with open(path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=columns, extrasaction='ignore')
                writer.writeheader()
                for s in series_info:
                    row = {k: (s.get(k) or '') for k in columns}
                    writer.writerow(row)

        def write_to_db_incrementally(show_status=True):
            """Helper function to write current state to PostgreSQL database"""
            if all_series_info:
                try:
                    # Get only new series since last write
                    new_series = all_series_info[series_count_at_last_write:]
                    if not new_series:
                        return
                    
                    # Convert series info to database format
                    db_series = []
                    for series in new_series:
                        db_series.append({
                            'series_id': series.get('id', ''),
                            'title': series.get('title', ''),
                            'description': series.get('description', ''),
                            'frequency': series.get('frequency', ''),
                            'units': series.get('units', ''),
                            'category_id': str(series.get('category_id', '')),
                            'category_name': series.get('category_name', ''),
                            'observation_start': series.get('observation_start', ''),
                            'observation_end': series.get('observation_end', ''),
                            'country': series.get('country', ''),
                            'last_updated': series.get('last_updated', ''),
                            'popularity': str(series.get('popularity', '')),
                            'created_at': datetime.now().isoformat(),
                            'updated_at': datetime.now().isoformat(),
                        })
                    
                    if db_series:
                        add_fred_series_fast(conn, db_series)
                        update_fred_metadata(conn, 'total_series', str(len(all_series_info)))
                    if catalog_path:
                        _save_catalog_to_csv(all_series_info, catalog_path)

                    if show_status:
                        status_msg = f"  [{len(all_series_info)} series] Progress saved to database"
                        if limit:
                            status_msg += f" ({len(all_series_info)}/{limit})"
                        logger.debug("%s", status_msg)
                except Exception as e:
                    logger.warning("Error writing to database: %s", e)
        
        try:
            if use_categories:
                # Approach 1: Get series from categories
                if use_known_categories_only:
                    # If user provided explicit category roots, use those instead of the built-in list
                    if category_roots:
                        categories = sorted(set(category_roots))
                        logger.info("Exploring user-specified category roots to find series (fast mode)...")
                        logger.info("Using %d root categories: %s", len(categories), categories)
                    else:
                        logger.info("Exploring known major categories to find series (fast mode)...")
                        categories = self._get_known_categories()
                        logger.info("Using %d known major categories. Exploring for series...", len(categories))
                else:
                    logger.info("Exploring ALL categories to find series (this may take a while)...")
                    categories = self._get_all_categories()
                    logger.info("Found %d categories. Exploring for series...", len(categories))
                
                base_url = "https://api.stlouisfed.org/fred/category/series"
                category_info_url = "https://api.stlouisfed.org/fred/category"
                
                # Cache category names to avoid repeated API calls
                category_names_cache = {}
                
                pbar = tqdm(categories, desc="Categories", unit="cat")
                for i, cat_id in enumerate(pbar):
                    try:
                        pbar.set_postfix(series=len(series_ids_set), refresh=False)
                        # Get category name if not in cache
                        if cat_id not in category_names_cache:
                            try:
                                cat_params = {
                                    'category_id': cat_id,
                                    'api_key': self.api_key,
                                    'file_type': 'json'
                                }
                                cat_response = self._fred_request(category_info_url, cat_params, timeout=10)
                                if cat_response.status_code == 200:
                                    cat_data = cat_response.json()
                                    if 'categories' in cat_data and cat_data['categories']:
                                        category_names_cache[cat_id] = cat_data['categories'][0].get('name', f'Category {cat_id}')
                                    else:
                                        category_names_cache[cat_id] = f'Category {cat_id}'
                                else:
                                    category_names_cache[cat_id] = f'Category {cat_id}'
                            except:
                                category_names_cache[cat_id] = f'Category {cat_id}'
                        
                        category_name = category_names_cache[cat_id]
                        logger.debug("Exploring category %s (%s) [%d/%d]", cat_id, category_name, i + 1, len(categories))
                        
                        # Paginate: FRED returns max 1000 per request; use offset to get all series in this category
                        offset = 0
                        page_size = 1000
                        while True:
                            params = {
                                'category_id': cat_id,
                                'api_key': self.api_key,
                                'file_type': 'json',
                                'limit': page_size,
                                'offset': offset
                            }
                            
                            response = self._fred_request(base_url, params, timeout=10)
                            response.raise_for_status()
                            data = response.json()
                            
                            seriess = data.get('seriess') or []
                            if not seriess:
                                break
                            
                            for series in seriess:
                                series_id = series.get('id')
                                if series_id and series_id not in series_ids_set:
                                    series_ids_set.add(series_id)
                                    
                                    # Extract country information
                                    country = self._extract_country_from_series(series_id, series, '')
                                    
                                    # Get description if available
                                    description = series.get('notes', '') or series.get('description', '')
                                    
                                    all_series_info.append({
                                        'id': series_id,
                                        'title': series.get('title', ''),
                                        'description': description,
                                        'units': series.get('units', ''),
                                        'frequency': series.get('frequency', ''),
                                        'seasonal_adjustment': series.get('seasonal_adjustment', ''),
                                        'observation_start': series.get('observation_start', ''),
                                        'observation_end': series.get('observation_end', ''),
                                        'country': country,
                                        'last_updated': series.get('last_updated', ''),
                                        'popularity': series.get('popularity', ''),
                                        'category_id': cat_id,
                                        'category_name': category_name,
                                    })
                                    
                                    # Write to database incrementally
                                    if len(all_series_info) - series_count_at_last_write >= write_interval:
                                        write_to_db_incrementally()
                                        series_count_at_last_write = len(all_series_info)
                                    
                                    if limit and len(series_ids_set) >= limit:
                                        break
                            
                            if limit and len(series_ids_set) >= limit:
                                break
                            if len(seriess) < page_size:
                                break
                            offset += page_size
                        
                        if (i + 1) % 50 == 0:
                            # Also write to database at category checkpoints
                            if len(all_series_info) > series_count_at_last_write:
                                write_to_db_incrementally()
                                series_count_at_last_write = len(all_series_info)
                        
                        if limit and len(series_ids_set) >= limit:
                            break

                    except Exception as e:
                        continue
                
                logger.info("Found %d series from categories", len(series_ids_set))
            
            # Approach 2: Use search terms to find additional series (only if enabled)
            if use_search_terms and (not limit or len(series_ids_set) < limit):
                logger.info("Searching with economic terms to find additional series...")
                search_terms = [
                    '', 'GDP', 'unemployment', 'inflation', 'interest rate',
                    'employment', 'production', 'consumer', 'price',
                    'money', 'banking', 'trade', 'exchange rate',
                    'industrial', 'retail', 'housing', 'manufacturing'
                ]
                
                remaining_limit = (limit - len(series_ids_set)) if limit else None
                
                for term in search_terms:
                    if limit and len(series_ids_set) >= limit:
                        break
                    
                    try:
                        search_limit = min(1000, remaining_limit if remaining_limit else 1000)
                        results = self.fred.search(term, order_by='popularity', limit=search_limit)
                        
                        if isinstance(results, pd.DataFrame) and not results.empty:
                            for series_id in results.index:
                                if series_id not in series_ids_set:
                                    series_ids_set.add(series_id)
                                    # Try to get series info
                                    try:
                                        info = self.fred.get_series_info(series_id)
                                        if isinstance(info, pd.Series):
                                            info_dict = info.to_dict() if hasattr(info, 'to_dict') else {}
                                            country = self._extract_country_from_series(series_id, info_dict, '')
                                            
                                            # Get category_id if available
                                            category_id = info_dict.get('category_id')
                                            category_name = 'Found via search'
                                            if category_id:
                                                # Try to get category name
                                                try:
                                                    cat_params = {
                                                        'category_id': category_id,
                                                        'api_key': self.api_key,
                                                        'file_type': 'json'
                                                    }
                                                    cat_response = self._fred_request("https://api.stlouisfed.org/fred/category", cat_params, timeout=10)
                                                    if cat_response.status_code == 200:
                                                        cat_data = cat_response.json()
                                                        if 'categories' in cat_data and cat_data['categories']:
                                                            category_name = cat_data['categories'][0].get('name', f'Category {category_id}')
                                                except:
                                                    category_name = f'Category {category_id}'
                                            
                                            description = info_dict.get('notes', '') or info_dict.get('description', '')
                                            
                                            all_series_info.append({
                                                'id': series_id,
                                                'title': info_dict.get('title', ''),
                                                'description': description,
                                                'units': info_dict.get('units', ''),
                                                'frequency': info_dict.get('frequency', ''),
                                                'seasonal_adjustment': info_dict.get('seasonal_adjustment', ''),
                                                'observation_start': info_dict.get('observation_start', ''),
                                                'observation_end': info_dict.get('observation_end', ''),
                                                'country': country,
                                                'last_updated': info_dict.get('last_updated', ''),
                                                'popularity': info_dict.get('popularity', ''),
                                                'category_id': str(category_id) if category_id else '',
                                                'category_name': category_name,
                                            })
                                    except:
                                        # Fallback with minimal info
                                        country = self._extract_country_from_series(series_id, {}, '')
                                        all_series_info.append({
                                            'id': series_id,
                                            'title': '',
                                            'description': '',
                                            'units': '',
                                            'frequency': '',
                                            'seasonal_adjustment': '',
                                            'observation_start': '',
                                            'observation_end': '',
                                            'country': country,
                                            'last_updated': '',
                                            'popularity': '',
                                            'category_id': '',
                                            'category_name': 'Found via search',
                                        })
                                    
                                    # Write to database incrementally
                                    if len(all_series_info) - series_count_at_last_write >= write_interval:
                                        write_to_db_incrementally()
                                        series_count_at_last_write = len(all_series_info)
                                    
                                    if limit and len(series_ids_set) >= limit:
                                        break
                        
                        time.sleep(0.2)
                    except Exception as e:
                        continue
            
            logger.info("=" * 60)
            logger.info("Total unique series found: %d", len(all_series_info))
            if limit:
                logger.info("  Limit: %s | Found: %d", limit, len(all_series_info))
            logger.info("=" * 60)
            
            # Final save to database (mark as complete)
            # Write final batch of series
            if all_series_info:
                new_series = all_series_info[series_count_at_last_write:]
                if new_series:
                    db_series = []
                    for series in new_series:
                        db_series.append({
                            'series_id': series.get('id', ''),
                            'title': series.get('title', ''),
                            'description': series.get('description', ''),
                            'frequency': series.get('frequency', ''),
                            'units': series.get('units', ''),
                            'category_id': str(series.get('category_id', '')),
                            'category_name': series.get('category_name', ''),
                            'observation_start': series.get('observation_start', ''),
                            'observation_end': series.get('observation_end', ''),
                            'country': series.get('country', ''),
                            'last_updated': series.get('last_updated', ''),
                            'popularity': str(series.get('popularity', '')),
                            'created_at': datetime.now().isoformat(),
                            'updated_at': datetime.now().isoformat(),
                        })
                    if db_series:
                        add_fred_series_fast(conn, db_series)
                if catalog_path:
                    _save_catalog_to_csv(all_series_info, catalog_path)

            update_fred_metadata(conn, 'total_series', str(len(all_series_info)))
            update_fred_metadata(conn, 'status', 'complete')
            logger.info("FRED database finalized: %s", self.dbname)
            logger.info("  Total series saved: %d", len(all_series_info))
            
            return all_series_info
            
        except Exception as e:
            logger.error("Error retrieving downloadable series: %s", e)
            import traceback
            traceback.print_exc()
            return all_series_info
    
    def load_series_from_db(self, category_ids: Optional[List[int]] = None) -> List[str]:
        """
        Load series IDs from PostgreSQL database
        
        Args:
            category_ids: Optional list of category IDs to filter by
            
        Returns:
            List of series IDs
        """
        try:
            conn = self._get_pg_connection()
            series_list = load_fred_series_from_postgres(conn, category_ids=category_ids)
            series_ids = [s.get('series_id') for s in series_list if s.get('series_id')]
            logger.info("Loaded %d series IDs from PostgreSQL database", len(series_ids))
            return series_ids
        except Exception as e:
            logger.error("Error loading series from database: %s", e)
            return []
    
    def download_series_from_db(self,
                                   start_date: Optional[str] = None,
                                   end_date: Optional[str] = None,
                                   series_ids: Optional[List[str]] = None,
                                   run_log_path: Optional[Union[str, Path]] = None,
                                   command_line: Optional[str] = None) -> Dict[str, pd.DataFrame]:
        """
        Download time series for specified series IDs and save to PostgreSQL
        
        Args:
            start_date: Start date for time series (YYYY-MM-DD format)
            end_date: End date for time series (YYYY-MM-DD format)
            series_ids: List of series IDs to download (required)
            run_log_path: If set, write "Saved X data points" lines to this file.
            command_line: If run_log_path set, write this at the top of the log (e.g. sys.argv).
            
        Returns:
            Dictionary mapping series IDs to DataFrames
        """
        if not series_ids:
            logger.error("series_ids is required")
            return {}
        
        logger.info("Downloading %d series and saving to database...", len(series_ids))
        
        all_data = {}
        successful_downloads = 0
        failed_downloads = 0
        skipped_same_day = 0
        run_log_file = None
        if run_log_path:
            run_log_path = Path(run_log_path)
            run_log_path.parent.mkdir(parents=True, exist_ok=True)
            run_log_file = open(run_log_path, 'w', encoding='utf-8')
            if command_line:
                run_log_file.write(command_line + '\n')
            run_log_file.flush()

        conn = self._get_pg_connection()
        today = datetime.now().date()

        # One query: which series need update vs skip (updated_at already today)
        updated_at_map = get_series_updated_at_bulk(conn, series_ids)
        to_download = []
        for sid in series_ids:
            updated_at = updated_at_map.get(sid)
            if updated_at is not None and updated_at.date() == today:
                skipped_same_day += 1
                if run_log_file:
                    ts = updated_at.isoformat() if hasattr(updated_at, 'isoformat') else str(updated_at)
                    run_log_file.write("  skipped because updated to current day-%s %s\n" % (ts, sid))
            else:
                to_download.append(sid)
        if run_log_file:
            run_log_file.flush()

        try:
            for series_id in tqdm(to_download, desc="Download series", unit="series", dynamic_ncols=True):
                try:
                    def _log_saved(sid: str, n: int) -> None:
                        if run_log_file:
                            run_log_file.write("  Saved %d data points to database for %s\n" % (n, sid))
                            run_log_file.flush()

                    data = self.download_series(
                        series_id, start_date=start_date, end_date=end_date, save_to_db=True,
                        run_log_cb=_log_saved if run_log_file else None
                    )
                
                    if data is not None and not data.empty:
                        # Convert to DataFrame for return value
                        df = data.to_frame(name=series_id)
                        df.index.name = 'date'
                        all_data[series_id] = df
                        
                        successful_downloads += 1
                    else:
                        failed_downloads += 1
                    
                    # Respect API rate limits (FRED allows 120 requests per minute)
                    time.sleep(0.5)
                
                except Exception as e:
                    logger.error("Error downloading %s: %s", series_id, e)
                    failed_downloads += 1
                    continue
        finally:
            if run_log_file:
                run_log_file.close()
        
        logger.info("Download complete!")
        logger.info("  Successfully downloaded: %d series", successful_downloads)
        logger.info("  Skipped (already updated today): %d series", skipped_same_day)
        logger.info("  Failed: %d series", failed_downloads)
        logger.info("  All data saved to PostgreSQL database")
        if run_log_path:
            logger.info("  Run log: %s", run_log_path)
        
        return all_data

