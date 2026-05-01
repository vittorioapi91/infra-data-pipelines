"""
BLS Economic Data Downloader

This module provides functionality to download time series data from BLS (Bureau of Labor Statistics).
BLS API key is optional but recommended. Get one at: https://www.bls.gov/developers/api_signature.htm
"""

import logging
import os
import time
import csv
import io
from pathlib import Path
from typing import List, Dict, Optional
import pandas as pd
import requests
import warnings
from datetime import datetime
try:
    from curl_cffi import requests as curl_requests
except ImportError:
    curl_requests = None

logger = logging.getLogger(__name__)

# Import BLS PostgreSQL utilities
try:
    from .bls_postgres import (
        get_postgres_connection, init_bls_postgres_tables, 
        add_bls_series_fast, load_bls_series_from_postgres, 
        update_bls_metadata, load_survey_sync_status, upsert_survey_sync_status, add_time_series_fast,
        load_time_series_from_postgres
    )
except ImportError:
    from src.macro.bls.bls_postgres import (
        get_postgres_connection, init_bls_postgres_tables, 
        add_bls_series_fast, load_bls_series_from_postgres, 
        update_bls_metadata, load_survey_sync_status, upsert_survey_sync_status, add_time_series_fast,
        load_time_series_from_postgres
    )

warnings.filterwarnings('ignore')


class BLSDataDownloader:
    """Class to download economic time series data from BLS"""
    
    def __init__(self, api_key: Optional[str] = None,
                 user: str = "tradingAgent", host: str = "localhost",
                 password: Optional[str] = None, port: int = 5432):
        """
        Initialize BLS data downloader
        
        Args:
            api_key: BLS API key (optional but recommended for higher rate limits).
                     If not provided, will use default key or try to get from environment variable BLS_API_KEY
            user: PostgreSQL user (default: 'tradingAgent')
            host: PostgreSQL host (default: 'localhost')
            password: PostgreSQL password (optional, can use POSTGRES_PASSWORD env var)
            port: PostgreSQL port (default: 5432)
        """
        # Default API key
        default_api_key = "e2b2d9ddb16a4437bc8747c59dda4eac"
        self.api_key = api_key or os.getenv('BLS_API_KEY') or default_api_key
        self.base_url = 'https://api.bls.gov/publicAPI/v2/timeseries/data/'
        # BLS v2 "All Surveys" endpoint: returns a list of surveys
        self.surveys_url = 'https://api.bls.gov/publicAPI/v2/surveys'
        
        # Schema name for logging (datalake.bls)
        self.dbname = 'bls'
        self.user = user
        self.host = host
        self.password = password or os.getenv('POSTGRES_PASSWORD', '')
        self.port = port
        self._pg_conn = None
    
    def _get_pg_connection(self):
        """Get or create PostgreSQL connection"""
        if self._pg_conn is None or self._pg_conn.closed:
            self._pg_conn = get_postgres_connection(
                user=self.user,
                host=self.host,
                password=self.password,
                port=self.port
            )
            init_bls_postgres_tables(self._pg_conn)
        return self._pg_conn
        
    def download_series(self, series_ids: List[str], start_year: int, end_year: int, 
                      save_to_db: bool = True) -> Optional[pd.DataFrame]:
        """
        Download BLS time series data for given series IDs
        
        Args:
            series_ids: List of BLS series IDs (e.g., ['CUUR0000SA0', 'SUUR0000SA0'])
            start_year: Start year for data
            end_year: End year for data
            
        Returns:
            DataFrame with time series data, or None if error
        """
        try:
            headers = {'Content-Type': 'application/json'}
            data = {
                "seriesid": series_ids,
                "startyear": str(start_year),
                "endyear": str(end_year)
            }
            
            # Add API key if available
            if self.api_key:
                data["registrationkey"] = self.api_key
            
            response = requests.post(self.base_url, headers=headers, json=data, timeout=30)
            response.raise_for_status()
            
            json_data = response.json()
            
            # Check for errors in BLS response
            if json_data.get('status') == 'REQUEST_SUCCEEDED':
                df = self._parse_bls_data(json_data)
                if df is not None and not df.empty:
                    df['value'] = pd.to_numeric(
                        df['value'].replace(r'^\s*$', pd.NA, regex=True),
                        errors='coerce',
                    )

                if save_to_db and df is not None and not df.empty:
                    # Save to PostgreSQL
                    conn = self._get_pg_connection()
                    time_series_list = []
                    
                    for _, row in df.iterrows():
                        time_series_list.append({
                            'series_id': row['series_id'],
                            'date': row['date'].strftime('%Y-%m-%d') if hasattr(row['date'], 'strftime') else str(row['date']),
                            'value': float(row['value']) if pd.notna(row['value']) else None,
                            'year': str(row.get('year', '')),
                            'period': str(row.get('period', '')),
                            'footnotes': row.get('footnotes', [])
                        })
                    
                    if time_series_list:
                        add_time_series_fast(conn, time_series_list)
                        logger.info("Saved %s data points to database for %s series", len(time_series_list), len(series_ids))
                
                return df
            else:
                error_msg = json_data.get('message', ['Unknown error'])
                logger.error("BLS API error: %s", error_msg)
                return None
                
        except Exception as e:
            logger.exception("Error downloading BLS series %s: %s", series_ids, e)
            return None
    
    def _parse_bls_data(self, json_data: Dict) -> pd.DataFrame:
        """
        Parse BLS API response into DataFrame
        
        Args:
            json_data: JSON response from BLS API
            
        Returns:
            DataFrame with parsed data
        """
        all_data = []
        
        if 'Results' in json_data and 'series' in json_data['Results']:
            for series in json_data['Results']['series']:
                series_id = series.get('seriesID', '')
                
                # Get series attributes
                catalog_data = series.get('catalogData', {})
                
                for item in series.get('data', []):
                    year = item.get('year', '')
                    period = item.get('period', '')
                    value = item.get('value', '')
                    
                    # Convert period to date (e.g., "M01" -> January)
                    date_str = self._period_to_date(year, period)
                    
                    all_data.append({
                        'series_id': series_id,
                        'date': date_str,
                        'year': year,
                        'period': period,
                        'value': value,
                        'footnotes': item.get('footnotes', [])
                    })
        
        if all_data:
            df = pd.DataFrame(all_data)
            df['date'] = pd.to_datetime(df['date'], errors='coerce')
            df = df.sort_values('date').reset_index(drop=True)
            return df
        else:
            return pd.DataFrame()
    
    def _period_to_date(self, year: str, period: str) -> str:
        """
        Convert BLS period code to date string
        
        Args:
            year: Year as string
            period: Period code (e.g., "M01" for January, "M13" for annual average, "Q01" for Q1)
            
        Returns:
            Date string in YYYY-MM-DD format
        """
        try:
            year_int = int(year)
            
            if period.startswith('M'):  # Monthly
                month = int(period[1:])
                if 1 <= month <= 12:
                    return f"{year_int}-{month:02d}-01"
            elif period.startswith('Q'):  # Quarterly
                quarter = int(period[1:])
                month = (quarter - 1) * 3 + 1
                return f"{year_int}-{month:02d}-01"
            elif period == 'M13' or period == 'A00':  # Annual average
                return f"{year_int}-01-01"
            elif period.startswith('S'):  # Semi-annual
                half = int(period[1:])
                month = (half - 1) * 6 + 1
                return f"{year_int}-{month:02d}-01"
            
            # Default to start of year if unknown format
            return f"{year_int}-01-01"
        except Exception as e:
            logger.debug("Could not parse period %s for year %s: %s", period, year, e)
            return f"{year}-01-01"
    
    def get_series_info(self, series_id: str) -> Optional[Dict]:
        """
        Get metadata for a BLS series
        
        Args:
            series_id: BLS series ID
            
        Returns:
            Dictionary with series metadata
        """
        try:
            # BLS API v2 doesn't have a direct series info endpoint
            # We can infer from series ID structure or use catalog
            # For now, return basic structure
            return {
                'series_id': series_id,
                'source': 'BLS',
                'description': f'BLS Series {series_id}'
            }
        except Exception as e:
            logger.warning("Error getting series info for %s: %s", series_id, e)
            return None
    
    def get_all_surveys(self) -> List[Dict]:
        """
        Get list of all available BLS surveys
        
        Returns:
            List of survey dictionaries
        """
        try:
            response = requests.get(self.surveys_url, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            if data.get('status') == 'REQUEST_SUCCEEDED' and 'Results' in data:
                return data['Results'].get('survey', [])
            return []
        except Exception as e:
            logger.warning("Error fetching surveys: %s", e)
            return []
    
    def get_available_series_from_survey(self, survey_abbreviation: str) -> List[str]:
        """
        Get available series IDs from a survey
        
        Args:
            survey_abbreviation: Survey abbreviation (e.g., 'CU', 'SU', 'CE')
            
        Returns:
            List of series IDs
        """
        # Note: BLS API doesn't provide a direct endpoint for this
        # This would require additional API calls or manual catalog lookup
        # For now, return empty list - users need to know series IDs
        logger.info("Note: BLS API doesn't provide direct series listing for survey %s", survey_abbreviation)
        logger.info("You'll need to specify series IDs directly or use BLS data tools to find them")
        return []

    def _build_survey_lookup(self, surveys: List[Dict]) -> Dict[str, str]:
        """Build {survey_abbreviation: survey_name} lookup from BLS survey metadata."""
        lookup: Dict[str, str] = {}
        for survey in surveys:
            abbr = (survey.get('survey_abbreviation') or '').strip().upper()
            name = (survey.get('survey_name') or '').strip()
            if abbr:
                lookup[abbr] = name
        return lookup

    def _download_survey_series_rows(
        self,
        survey_abbreviation: str,
        survey_name: str,
        series_files_dir: Optional[str] = None,
    ) -> List[Dict[str, str]]:
        """
        Download and parse one BLS bulk survey file: /pub/time.series/{xx}/{xx}.series
        """
        survey_code = survey_abbreviation.lower()
        if series_files_dir:
            base = Path(series_files_dir).expanduser().resolve()
            candidate_paths = [
                base / survey_code / f"{survey_code}.series",
                base / f"{survey_code}.series",
            ]
            series_path = next((p for p in candidate_paths if p.exists() and p.is_file()), None)
            if series_path is None:
                raise RuntimeError(
                    f"Local .series file not found for survey {survey_abbreviation}. "
                    f"Tried: {candidate_paths}"
                )
            text = series_path.read_text(encoding="utf-8", errors="replace")
        else:
            url = f"https://download.bls.gov/pub/time.series/{survey_code}/{survey_code}.series"
            if curl_requests is None:
                raise RuntimeError(
                    "curl_cffi is required for BLS bulk catalogue download route, but it is not installed."
                )

            headers = {
                "User-Agent": os.getenv(
                    "BLS_BULK_USER_AGENT",
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0.0.0 Safari/537.36",
                ),
                "Accept": os.getenv(
                    "BLS_BULK_ACCEPT",
                    "text/plain,text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                ),
                "Accept-Language": os.getenv("BLS_BULK_ACCEPT_LANGUAGE", "en-US,en;q=0.9"),
                "Referer": os.getenv("BLS_BULK_REFERER", "https://www.bls.gov/"),
                "Connection": "keep-alive",
            }
            bulk_cookie = os.getenv("BLS_BULK_COOKIE")
            if bulk_cookie:
                headers["Cookie"] = bulk_cookie

            # Large survey files can be very slow; retry transient network timeouts.
            # Keep this non-configurable by design (per user request).
            last_exc: Optional[Exception] = None
            for attempt in range(1, 4):
                try:
                    response = curl_requests.get(
                        url,
                        headers={
                            **headers,
                            "Cache-Control": "no-cache",
                            "Pragma": "no-cache",
                            "Sec-Fetch-Dest": "document",
                            "Sec-Fetch-Mode": "navigate",
                            "Sec-Fetch-Site": "none",
                            "Sec-Fetch-User": "?1",
                            "Upgrade-Insecure-Requests": "1",
                        },
                        impersonate=os.getenv("BLS_BULK_IMPERSONATE", "chrome124"),
                        timeout=600,
                    )
                    break
                except Exception as e:
                    last_exc = e
                    if attempt == 3:
                        raise
                    logger.warning(
                        "Retrying survey %s download (attempt %s/3): %s",
                        survey_abbreviation,
                        attempt,
                        e.__class__.__name__,
                    )
                    time.sleep(2 * attempt)

            if last_exc is not None and 'response' not in locals():
                raise last_exc
            response.raise_for_status()

            content_type = (response.headers.get("content-type") or "").lower()
            if "text/html" in content_type:
                raise RuntimeError(
                    f"Unexpected HTML response for {survey_abbreviation} at {url} "
                    "(expected plain text series file)."
                )
            text = response.text

        if not text.strip():
            raise RuntimeError(f"Empty .series file for survey {survey_abbreviation}.")

        raw_reader = csv.DictReader(io.StringIO(text), delimiter="\t")
        if not raw_reader.fieldnames:
            raise RuntimeError(f"No header found in .series file for survey {survey_abbreviation}.")

        # Some BLS files pad header names (e.g. "series_id        ").
        normalized_fieldnames = [str(name).strip() for name in raw_reader.fieldnames]
        reader = csv.DictReader(io.StringIO(text), delimiter="\t", fieldnames=normalized_fieldnames)
        # Skip original header row (we injected our own normalized names).
        next(reader, None)

        def get_row_value(row: Dict[str, str], *keys: str) -> str:
            for key in keys:
                value = row.get(key)
                if value is not None and str(value).strip():
                    return str(value).strip()
            return ""

        rows: List[Dict[str, str]] = []
        for row in reader:
            series_id = get_row_value(row, "series_id", "seriesid")
            if not series_id:
                continue

            rows.append(
                {
                    "series_id": series_id,
                    "survey_abbreviation": survey_abbreviation,
                    "survey_name": survey_name,
                    "seasonal": get_row_value(row, "seasonal", "seasonality", "seasonality_code"),
                    "area_code": get_row_value(row, "area_code"),
                    "area_name": get_row_value(row, "area_name", "area_text", "area"),
                    "item_code": get_row_value(row, "item_code"),
                    "item_name": get_row_value(row, "item_name", "item_text", "item"),
                }
            )

        return rows
    
    def download_multiple_series(self, series_ids: List[str], 
                                 start_year: int, 
                                 end_year: int,
                                 save_to_db: bool = True) -> pd.DataFrame:
        """
        Download multiple BLS series and combine into a DataFrame
        
        Args:
            series_ids: List of BLS series IDs
            start_year: Start year
            end_year: End year
            save_to_file: Optional path to save DataFrame as CSV
            
        Returns:
            DataFrame with all series data
        """
        logger.info("Downloading %s BLS series from %s to %s...", len(series_ids), start_year, end_year)
        
        # BLS API can handle multiple series in one request
        df = self.download_series(series_ids, start_year, end_year, save_to_db=save_to_db)
        
        if df is not None and not df.empty:
            # Pivot to have series as columns
            df_pivot = df.pivot_table(
                index='date',
                columns='series_id',
                values='value',
                aggfunc='first'
            )
            df_pivot.index.name = 'date'
            
            return df_pivot
        else:
            logger.warning("No data was successfully downloaded")
            return pd.DataFrame()

    def download_series_catalog_batch(
        self,
        series_ids: List[str],
        start_year: int,
        end_year: int,
    ) -> List[Dict]:
        """
        Download BLS v2 catalog metadata for a batch of series IDs.

        Uses the "One or More Series with Optional Parameters" signature with:
        - catalog=true
        - registrationkey=<BLS_API_KEY>
        """
        if not series_ids:
            return []

        headers = {'Content-Type': 'application/json'}
        payload: Dict[str, object] = {
            'seriesid': series_ids,
            'startyear': str(start_year),
            'endyear': str(end_year),
            'catalog': 'true',
        }
        if self.api_key:
            payload['registrationkey'] = self.api_key

        response = requests.post(
            self.base_url,
            headers=headers,
            json=payload,
            timeout=30,
        )
        response.raise_for_status()
        json_data = response.json()

        if json_data.get('status') != 'REQUEST_SUCCEEDED':
            raise RuntimeError(f"BLS API error: {json_data.get('message', [])}")

        # For invalid series IDs, BLS still returns REQUEST_SUCCEEDED but includes
        # per-series errors in "message", and catalog is missing/false.
        messages = json_data.get('message') or []
        if messages:
            logger.info("BLS catalog batch returned %s message(s)", len(messages))

        series = json_data.get('Results', {}).get('series', []) or []
        catalogs: List[Dict] = []
        for s in series:
            catalog = s.get('catalog')
            if isinstance(catalog, dict) and catalog.get('series_id'):
                catalogs.append(catalog)

        return catalogs
    
    def get_all_downloadable_series(
        self,
        test_patterns: bool = True,
        write_interval: int = 5000,
        series_files_dir: Optional[str] = None,
    ) -> int:
        """
        Build a full catalogue of available BLS series and save it to PostgreSQL.

        This implementation is survey-driven:
        1) fetch all surveys from BLS API v2
        2) for each survey, download the official BLS bulk file
           /pub/time.series/{survey}/{survey}.series
           (or read it from local --series-files-dir)
        3) ingest all rows into bls.series
        """
        logger.info("Retrieving BLS series catalogue...")

        conn = self._get_pg_connection()
        update_bls_metadata(conn, 'status', 'in_progress')
        update_bls_metadata(conn, 'generated_at', datetime.now().isoformat())
        logger.info("Connected to PostgreSQL database: %s", self.dbname)

        surveys = self.get_all_surveys()
        logger.info("Found %s BLS surveys", len(surveys))

        existing_sync_status = load_survey_sync_status(conn)
        survey_lookup = self._build_survey_lookup(surveys)
        if not survey_lookup and existing_sync_status:
            # If BLS surveys API returns nothing, fallback to known surveys already tracked
            # in DB so failed/non-completed surveys can still be retried.
            survey_lookup = {
                abbr: (row.get("survey_name") or "")
                for abbr, row in existing_sync_status.items()
            }
            logger.warning(
                "BLS surveys API returned no surveys; falling back to %s tracked surveys from DB.",
                len(survey_lookup),
            )

        run_date = datetime.now().date()
        pending_rows: List[Dict[str, str]] = []
        processed_surveys = 0
        skipped_surveys = 0
        failed_surveys: List[str] = []
        total_written = 0

        for survey_abbreviation in sorted(survey_lookup.keys()):
            survey_name = survey_lookup[survey_abbreviation]
            survey_code = survey_abbreviation.lower()
            survey_url = f"https://download.bls.gov/pub/time.series/{survey_code}/{survey_code}.series"

            existing_status = existing_sync_status.get(survey_abbreviation)
            existing_updated_at = existing_status.get("updated_at") if existing_status else None
            if (
                existing_status
                and str(existing_status.get("status", "")).lower() == "completed"
                and existing_updated_at is not None
                and hasattr(existing_updated_at, "date")
                and existing_updated_at.date() == run_date
            ):
                skipped_surveys += 1
                logger.info(
                    "Skipping survey %s (already completed today at %s)",
                    survey_abbreviation,
                    existing_updated_at.isoformat(),
                )
                continue

            try:
                upsert_survey_sync_status(
                    conn,
                    survey_abbreviation=survey_abbreviation,
                    survey_name=survey_name,
                    status="in_progress",
                    rows_loaded=0,
                    last_error=None,
                )
                rows = self._download_survey_series_rows(
                    survey_abbreviation,
                    survey_name,
                    series_files_dir=series_files_dir,
                )
            except Exception as e:
                failed_surveys.append(f"{survey_abbreviation} ({survey_url})")
                message = str(e)
                # Prefer transport-level curl code (e.g. CURL_28 timeout), which is
                # more informative than an attached HTTP status for this failure mode.
                if "curl: (" in message:
                    try:
                        error_code = f"CURL_{message.split('curl: (', 1)[1].split(')', 1)[0].strip()}"
                    except Exception:
                        error_code = e.__class__.__name__.upper()
                else:
                    response = getattr(e, "response", None)
                    status_code = getattr(response, "status_code", None) if response is not None else None
                    if isinstance(status_code, int) and status_code >= 400:
                        error_code = f"HTTP_{status_code}"
                    else:
                        error_code = e.__class__.__name__.upper()
                upsert_survey_sync_status(
                    conn,
                    survey_abbreviation=survey_abbreviation,
                    survey_name=survey_name,
                    status=f"failed:{error_code}",
                    rows_loaded=0,
                    last_error=str(e),
                )
                logger.warning(
                    "Failed survey %s: %s | error_code=%s",
                    survey_abbreviation,
                    survey_url,
                    error_code,
                )
                continue

            pending_rows.extend(rows)
            upsert_survey_sync_status(
                conn,
                survey_abbreviation=survey_abbreviation,
                survey_name=survey_name,
                status="completed",
                rows_loaded=len(rows),
                last_error=None,
            )
            processed_surveys += 1
            logger.info(
                "Loaded %s rows from survey %s (%s/%s)",
                len(rows),
                survey_abbreviation,
                processed_surveys,
                len(survey_lookup),
            )

            if len(pending_rows) >= write_interval:
                written = add_bls_series_fast(conn, pending_rows)
                total_written += written
                pending_rows = []
                update_bls_metadata(conn, 'total_series', str(total_written))
                logger.info("Progress saved: %s series rows written", total_written)

            time.sleep(0.05)

        if pending_rows:
            written = add_bls_series_fast(conn, pending_rows)
            total_written += written

        if failed_surveys:
            update_bls_metadata(conn, 'status', 'complete_with_errors')
            update_bls_metadata(conn, 'failed_surveys_count', str(len(failed_surveys)))
            logger.warning(
                "Catalogue completed with errors: %s surveys failed. "
                "See per-survey warning logs above for details.",
                len(failed_surveys),
            )
        else:
            update_bls_metadata(conn, 'status', 'complete')
            update_bls_metadata(conn, 'failed_surveys_count', '0')

        update_bls_metadata(conn, 'total_series', str(total_written))
        logger.info(
            "BLS series catalogue completed: %s series rows written (%s surveys processed, %s skipped)",
            total_written,
            processed_surveys,
            skipped_surveys,
        )

        return total_written
    


