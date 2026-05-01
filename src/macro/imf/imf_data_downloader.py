"""
IMF Economic Data Downloader

This module provides functionality to download time series data from IMF (International Monetary Fund).
Uses the imfp package to interact with IMF's JSON RESTful API.
"""

import logging
import os
import time
import json
from typing import List, Dict, Optional, Set
import pandas as pd
import requests
import warnings
from datetime import datetime

logger = logging.getLogger(__name__)

# Import IMF parquet utilities
try:
    from .imf_parquet import init_imf_parquet_tables, get_imf_parquet_paths, add_imf_indicators_fast, load_imf_indicators_from_parquet, update_imf_metadata
except ImportError:
    from src.macro.imf.imf_parquet import init_imf_parquet_tables, get_imf_parquet_paths, add_imf_indicators_fast, load_imf_indicators_from_parquet, update_imf_metadata

warnings.filterwarnings('ignore')

# Try to import imfp, but make it optional
try:
    import imfp
    IMF_AVAILABLE = True
except ImportError:
    IMF_AVAILABLE = False
    logger.warning("imfp package not installed. Install with: pip install imfp")


class IMFDataDownloader:
    """Class to download economic time series data from IMF"""
    
    def __init__(self):
        """
        Initialize IMF data downloader
        
        Note: IMF API doesn't require an API key for most endpoints
        """
        if not IMF_AVAILABLE:
            raise ImportError("imfp package is required. Install with: pip install imfp")
        
        self.base_url = "https://api.imf.org"
        
    def get_databases(self) -> pd.DataFrame:
        """
        Get list of all available IMF databases
        
        Returns:
            DataFrame with database_id and description columns
        """
        try:
            databases = imfp.imf_databases()
            return databases
        except Exception as e:
            logger.warning("Error fetching IMF databases: %s", e)
            return pd.DataFrame()
    
    def get_database_parameters(self, database_id: str) -> Optional[pd.DataFrame]:
        """
        Get parameters (dimensions) for a specific database
        
        Args:
            database_id: Database ID (e.g., 'IFS', 'BOP', 'WEO')
            
        Returns:
            DataFrame with parameter information
        """
        try:
            params = imfp.imf_parameters(database_id)
            return params
        except Exception as e:
            logger.warning("Error fetching parameters for %s: %s", database_id, e)
            return None
    
    def search_series(self, database_id: str, search_term: str = '', 
                     indicator: Optional[str] = None,
                     country: Optional[str] = None) -> List[Dict]:
        """
        Search for series in a database by getting parameters and filtering
        
        Args:
            database_id: Database ID (e.g., 'IFS', 'BOP', 'WEO')
            search_term: Search term to filter series
            indicator: Filter by indicator code
            country: Filter by country code
            
        Returns:
            List of series dictionaries
        """
        try:
            # Get parameters for the database
            params_df = self.get_database_parameters(database_id)
            
            if params_df is None or params_df.empty:
                return []
            
            # Filter by search term if provided
            if search_term:
                search_term_lower = search_term.lower()
                params_df = params_df[
                    params_df.astype(str).apply(lambda x: x.str.lower().str.contains(search_term_lower, na=False)).any(axis=1)
                ]
            
            # Convert to list of dicts
            series_list = params_df.to_dict('records')
            return series_list
        except Exception as e:
            logger.warning("Error searching series in %s: %s", database_id, e)
            return []
    
    def download_series(self, database_id: str, 
                      parameters: Optional[Dict] = None,
                      start_date: Optional[str] = None,
                      end_date: Optional[str] = None,
                      countries: Optional[List[str]] = None,
                      indicators: Optional[List[str]] = None) -> Optional[pd.DataFrame]:
        """
        Download time series data from a database
        
        Args:
            database_id: Database ID (e.g., 'IFS', 'BOP', 'WEO')
            parameters: Dictionary of parameter filters (e.g., {'indicator': 'NGDP_RPCH', 'country': 'USA'})
            start_date: Start date (YYYY-MM-DD format)
            end_date: End date (YYYY-MM-DD format)
            countries: List of country codes (ISO 3-letter codes) - alternative to parameters
            indicators: List of indicator codes - alternative to parameters
            
        Returns:
            DataFrame with time series data, or None if error
        """
        try:
            # Build parameters dict
            if parameters is None:
                parameters = {}
            
            # Add countries and indicators if provided
            if countries:
                parameters['country'] = countries if len(countries) > 1 else countries[0]
            if indicators:
                parameters['indicator'] = indicators if len(indicators) > 1 else indicators[0]
            
            # Download data using imf_dataset
            df = imfp.imf_dataset(database_id, **parameters)
            
            if df is not None and not df.empty:
                # Filter by date range if provided
                if start_date or end_date:
                    # Try to find date column
                    date_col = None
                    for col in df.columns:
                        if 'date' in col.lower() or col.lower() in ['date', 'time', 'period']:
                            date_col = col
                            break
                    
                    if date_col:
                        df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
                        if start_date:
                            df = df[df[date_col] >= pd.to_datetime(start_date)]
                        if end_date:
                            df = df[df[date_col] <= pd.to_datetime(end_date)]
                
                return df
            else:
                return None
                
        except Exception as e:
            logger.warning("Error downloading IMF data from %s: %s", database_id, e)
            import traceback
            traceback.print_exc()
            return None
    
    def download_multiple_series(self, database_id: str, indicators: List[str],
                                start_date: Optional[str] = None,
                                end_date: Optional[str] = None,
                                countries: Optional[List[str]] = None) -> pd.DataFrame:
        """
        Download multiple indicators and combine into a single DataFrame
        
        Args:
            database_id: Database ID
            indicators: List of indicator codes to download
            start_date: Start date (YYYY-MM-DD format)
            end_date: End date (YYYY-MM-DD format)
            countries: List of country codes
            
        Returns:
            Combined DataFrame with all series
        """
        try:
            # Download all indicators at once if possible
            df = self.download_series(
                database_id,
                indicators=indicators,
                start_date=start_date,
                end_date=end_date,
                countries=countries
            )
            
            if df is not None and not df.empty:
                return df
            else:
                return pd.DataFrame()
        except Exception as e:
            logger.warning("Error downloading multiple series: %s", e)
            # Fallback: download one by one
            all_data = []
            
            for indicator in indicators:
                logger.info("Downloading %s...", indicator)
                df = self.download_series(
                    database_id,
                    indicators=[indicator],
                    start_date=start_date,
                    end_date=end_date,
                    countries=countries
                )
                
                if df is not None and not df.empty:
                    all_data.append(df)
                
                # Rate limiting
                time.sleep(0.2)
            
            if all_data:
                # Combine all DataFrames
                combined_df = pd.concat(all_data, axis=0, sort=True, ignore_index=True)
                return combined_df
            else:
                return pd.DataFrame()
    
    def get_indicators(self, database_id: str) -> List[Dict]:
        """
        Get list of available indicators for a database
        
        Args:
            database_id: Database ID
            
        Returns:
            List of indicator dictionaries with code and name
        """
        try:
            params_df = self.get_database_parameters(database_id)
            if params_df is None or params_df.empty:
                return []
            
            # Look for indicator parameter
            if 'indicator' in params_df.columns or 'parameter' in params_df.columns:
                # Get unique indicators
                indicator_col = 'indicator' if 'indicator' in params_df.columns else 'parameter'
                indicators = params_df[indicator_col].dropna().unique().tolist()
                return [{'code': ind, 'name': ind} for ind in indicators]
            
            # Try to get from parameter values
            params = imfp.imf_parameters(database_id)
            if 'indicator' in params.columns:
                return params[['indicator']].drop_duplicates().to_dict('records')
            
            return []
        except Exception as e:
            logger.warning("Error fetching indicators for %s: %s", database_id, e)
            return []
    
    def get_countries(self, database_id: str) -> List[Dict]:
        """
        Get list of available countries for a database
        
        Args:
            database_id: Database ID
            
        Returns:
            List of country dictionaries with code and name
        """
        try:
            params_df = self.get_database_parameters(database_id)
            if params_df is None or params_df.empty:
                return []
            
            # Look for country parameter
            if 'country' in params_df.columns:
                countries = params_df['country'].dropna().unique().tolist()
                return [{'code': c, 'name': c} for c in countries]
            
            # Try to get from parameter values
            params = imfp.imf_parameters(database_id)
            if 'country' in params.columns:
                return params[['country']].drop_duplicates().to_dict('records')
            
            return []
        except Exception as e:
            logger.warning("Error fetching countries for %s: %s", database_id, e)
            return []
    
    def get_all_downloadable_series(self, database_id: str = 'IFS',
                                   parquet_file: Optional[str] = None,
                                   limit: Optional[int] = None) -> List[Dict]:
        """
        Get all downloadable indicators for a database and optionally save to Parquet
        
        Args:
            database_id: Database ID (default: 'IFS' for International Financial Statistics)
            parquet_file: Optional path to parquet directory or file to save indicator metadata
            limit: Optional limit on number of indicators to return
            
        Returns:
            List of indicator dictionaries
        """
        logger.info("Fetching all indicators from IMF database: %s", database_id)
        
        # Initialize Parquet file if requested
        paths = None
        if parquet_file:
            paths = get_imf_parquet_paths(parquet_file)
            init_imf_parquet_tables(paths['base_dir'])
            update_imf_metadata(parquet_file, 'status', 'in_progress')
            update_imf_metadata(parquet_file, 'generated_at', datetime.now().isoformat())
            logger.info("Initialized IMF Parquet database: %s", paths['base_dir'])
        
        try:
            # Get all indicators
            indicators = self.get_indicators(database_id)
            logger.info("Found %s indicators", len(indicators))
            
            # Build series list
            series_list = []
            
            # For each indicator, create series entries
            for indicator in (indicators[:limit] if limit else indicators):
                indicator_code = indicator.get('code', '') or indicator.get('indicator', '')
                indicator_name = indicator.get('name', indicator_code)
                
                series_list.append({
                    'database_id': database_id,
                    'indicator_code': indicator_code,
                    'indicator_name': indicator_name,
                    'description': f"{indicator_name} ({indicator_code})"
                })
            
            # Save to Parquet if requested
            if parquet_file and paths and series_list:
                parquet_indicators = []
                for series in series_list:
                    parquet_indicators.append({
                        'indicator_code': series.get('indicator_code', ''),
                        'database_id': series.get('database_id', ''),
                        'indicator_name': series.get('indicator_name', ''),
                        'description': series.get('description', ''),
                        'created_at': datetime.now().isoformat(),
                        'updated_at': datetime.now().isoformat(),
                    })
                
                if parquet_indicators:
                    add_imf_indicators_fast(paths['indicators'], parquet_indicators)
                    update_imf_metadata(parquet_file, 'total_indicators', str(len(series_list)))
                    update_imf_metadata(parquet_file, 'status', 'complete')
                logger.info("Saved %s indicators to Parquet", len(series_list))
            
            return series_list
            
        except Exception as e:
            logger.warning("Error fetching downloadable indicators: %s", e)
            import traceback
            traceback.print_exc()
            return []
    
    def download_series_from_parquet(self, parquet_file: str,
                                 start_date: Optional[str] = None,
                                 end_date: Optional[str] = None,
                                 output_dir: str = 'imf_data') -> Dict:
        """
        Download series from a Parquet file
        
        Args:
            parquet_file: Path to parquet directory or file with indicator definitions
            start_date: Start date (YYYY-MM-DD format)
            end_date: End date (YYYY-MM-DD format)
            output_dir: Output directory for saved files (parquet only)
            
        Returns:
            Dictionary with download statistics
        """
        try:
            series_list = load_imf_indicators_from_parquet(parquet_file)
            
            if not series_list:
                logger.warning("No indicators found in Parquet file")
                return {'downloaded': 0, 'errors': 0}
            
            os.makedirs(output_dir, exist_ok=True)
            
            downloaded = 0
            errors = 0
            
            for series_def in series_list:
                database_id = series_def.get('database_id', 'IFS')
                indicator_code = series_def.get('indicator_code', '')
                indicator_name = series_def.get('indicator_name', indicator_code)
                
                if not indicator_code:
                    continue
                
                try:
                    logger.info("Downloading %s (%s)...", indicator_name, indicator_code)
                    df = self.download_series(
                        database_id,
                        indicators=[indicator_code],
                        start_date=start_date,
                        end_date=end_date
                    )
                    
                    if df is not None and not df.empty:
                        # Save file
                        safe_name = indicator_code.replace('/', '_').replace('\\', '_')
                        
                        parquet_path = os.path.join(output_dir, f"{safe_name}.parquet")
                        df.to_parquet(parquet_path)
                        
                        downloaded += 1
                    else:
                        logger.info("No data available for %s", indicator_code)
                        errors += 1
                    
                    time.sleep(0.2)  # Rate limiting
                    
                except Exception as e:
                    logger.warning("Error downloading %s: %s", indicator_code, e)
                    errors += 1
            
            logger.info("Download complete: %s series downloaded, %s errors", downloaded, errors)
            return {'downloaded': downloaded, 'errors': errors}
            
        except Exception as e:
            logger.exception("Error reading Parquet file: %s", e)
            return {'downloaded': 0, 'errors': 1}
