"""
BIS Data Downloader using BIS Stats API (SDMX REST).
"""

import logging
import os
import json
import time
from typing import List, Dict, Optional
import requests
from datetime import datetime

logger = logging.getLogger(__name__)

# Import BIS PostgreSQL utilities
try:
    from .bis_postgres import (
        get_postgres_connection, init_bis_postgres_tables, 
        add_bis_dataflows_fast, load_bis_dataflows_from_postgres, 
        update_bis_metadata, add_time_series_fast,
        load_time_series_from_postgres
    )
except ImportError:
    from src.macro.bis.bis_postgres import (
        get_postgres_connection, init_bis_postgres_tables, 
        add_bis_dataflows_fast, load_bis_dataflows_from_postgres, 
        update_bis_metadata, add_time_series_fast,
        load_time_series_from_postgres
)


class BISDataDownloader:
    def __init__(self, user_agent: str = "VittorioApicella apicellavittorio@hotmail.it",
                 user: str = "tradingAgent", host: str = "localhost",
                 password: Optional[str] = None, port: int = 5432):
        self.base_url = "https://stats.bis.org/api/v1"
        self.headers = {
            "User-Agent": user_agent,
            "Accept": "application/json",
        }
        
        # Store PostgreSQL connection parameters (dbname is fixed to 'bis')
        self.dbname = 'bis'
        self.user = user
        self.host = host
        self.password = password or os.getenv('POSTGRES_PASSWORD', '')
        self.port = port
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
            init_bis_postgres_tables(self._pg_conn)
        return self._pg_conn

    def _get(self, endpoint: str, params: Optional[Dict] = None) -> Optional[Dict]:
        try:
            response = requests.get(f"{self.base_url}{endpoint}", headers=self.headers, params=params, timeout=30)
            if response.status_code == 200:
                return response.json()
            else:
                logger.warning("Request failed (%s): %s", response.status_code, response.text[:200])
                return None
        except Exception as e:
            logger.warning("Request error: %s", e)
            return None

    def list_dataflows(self) -> List[Dict]:
        """List available dataflows (datasets) from BIS."""
        data = self._get("/dataflow")
        if not data or "data" not in data:
            return []
        flows = data.get("data", [])
        results = []
        for flow in flows:
            results.append({
                "dataflow_id": flow.get("id") or flow.get("dataflowid") or flow.get("name"),
                "name": flow.get("name"),
                "description": flow.get("description", ""),
                "last_updated": flow.get("lastUpdate", ""),
                "frequency": flow.get("frequency", ""),
                "updated_at": datetime.now().isoformat(),
            })
        return results

    def get_structure(self, dataflow_id: str) -> Optional[Dict]:
        """Get data structure (dimensions) for a dataflow."""
        return self._get(f"/datastructure/{dataflow_id}")

    def download_dataset(self, dataflow_id: str, key: str = "", params: Optional[Dict] = None) -> Optional[Dict]:
        """
        Download dataset for a given dataflow and optional key (dimension filters).
        key: SDMX key, e.g., ALL or country.dims...
        params: additional query params (e.g., startPeriod, endPeriod)
        """
        if params is None:
            params = {}
        params = params.copy()
        params.setdefault("format", "json")
        endpoint = f"/data/{dataflow_id}/{key}" if key else f"/data/{dataflow_id}"
        return self._get(endpoint, params=params)

    def save_dataset(self, data: Dict, output_path: str) -> None:
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        with open(output_path, "w") as f:
            json.dump(data, f, indent=2)

    def generate_db(self) -> List[Dict]:
        """Generate database with all BIS dataflows"""
        conn = self._get_pg_connection()
        dataflows = self.list_dataflows()
        added = add_bis_dataflows_fast(conn, dataflows)
        update_bis_metadata(conn, 'total_dataflows', str(len(dataflows)))
        update_bis_metadata(conn, 'generated_at', datetime.now().isoformat())
        update_bis_metadata(conn, 'status', 'complete')
        logger.info("Dataflows found: %s, added: %s", len(dataflows), added)
        logger.info("Database: %s", self.dbname)
        return dataflows

    def download_from_db(self, dataflow_id: str, key: str = "", 
                        params: Optional[Dict] = None, save_to_db: bool = True) -> Optional[Dict]:
        """
        Download dataset and optionally save to PostgreSQL
        
        Args:
            dataflow_id: Dataflow ID
            key: SDMX key (dimension filters)
            params: Additional query parameters
            save_to_db: Whether to save time series data to PostgreSQL (default: True)
            
        Returns:
            Dataset dictionary or None
        """
        data = self.download_dataset(dataflow_id, key=key, params=params)
        if not data:
            logger.warning("No data returned")
            return None
        
        if save_to_db and data:
            # Save time series data to PostgreSQL
            conn = self._get_pg_connection()
            time_series_list = []
            
            # Parse BIS SDMX JSON response
            if 'dataSets' in data and len(data['dataSets']) > 0:
                dataset = data['dataSets'][0]
                observations = dataset.get('observations', {})
                
                # Get structure information
                structure = data.get('structure', {})
                dimensions = structure.get('dimensions', {}).get('observation', [])
                dimension_ids = [dim.get('id') for dim in dimensions]
                
                # Parse observations
                for obs_key, obs_value in observations.items():
                    # obs_key is typically a list of dimension values
                    # obs_value is typically a list of values
                    if isinstance(obs_key, list):
                        # Extract dimensions from key
                        dim_dict = {}
                        for i, dim_id in enumerate(dimension_ids):
                            if i < len(obs_key):
                                dim_dict[dim_id] = str(obs_key[i])
                        
                        # Extract time dimension (usually first or last)
                        date_str = None
                        for dim_val in obs_key:
                            # Try to parse as date
                            date_str = self._parse_bis_date(str(dim_val))
                            if date_str:
                                break
                        
                        if not date_str:
                            continue
                        
                        # Get value (usually first element of obs_value list)
                        value = obs_value[0] if isinstance(obs_value, list) and len(obs_value) > 0 else obs_value
                        
                        if value is not None:
                            try:
                                time_series_list.append({
                                    'dataflow_id': dataflow_id,
                                    'date': date_str,
                                    'value': float(value),
                                    'dimensions': dim_dict
                                })
                            except (ValueError, TypeError):
                                pass
                    else:
                        # Simple key-value format
                        date_str = self._parse_bis_date(str(obs_key))
                        if date_str:
                            value = obs_value[0] if isinstance(obs_value, list) and len(obs_value) > 0 else obs_value
                            if value is not None:
                                try:
                                    time_series_list.append({
                                        'dataflow_id': dataflow_id,
                                        'date': date_str,
                                        'value': float(value),
                                        'dimensions': {}
                                    })
                                except (ValueError, TypeError):
                                    pass
            
            if time_series_list:
                add_time_series_fast(conn, time_series_list)
                logger.info("Saved %s data points to database for %s", len(time_series_list), dataflow_id)
        
        return data
    
    def _parse_bis_date(self, date_val) -> Optional[str]:
        """
        Parse BIS date format to YYYY-MM-DD
        
        Args:
            date_val: Date value (can be string, datetime, or other)
            
        Returns:
            Date string in YYYY-MM-DD format or None if parsing fails
        """
        if date_val is None:
            return None
        
        # If already a datetime object
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
        
        # Handle BIS-specific formats (similar to Eurostat)
        import re
        
        # Quarterly: 2020-Q1 -> 2020-01-01
        q_match = re.match(r'(\d{4})-Q([1-4])', date_str, re.IGNORECASE)
        if q_match:
            year = int(q_match.group(1))
            quarter = int(q_match.group(2))
            month = (quarter - 1) * 3 + 1
            return f"{year}-{month:02d}-01"
        
        # Monthly: 2020-01 -> 2020-01-01
        m_match = re.match(r'(\d{4})-(\d{2})', date_str)
        if m_match:
            year = int(m_match.group(1))
            month = int(m_match.group(2))
            if 1 <= month <= 12:
                return f"{year}-{month:02d}-01"
        
        # Annual: 2020 -> 2020-01-01
        y_match = re.match(r'(\d{4})$', date_str)
        if y_match:
            year = int(y_match.group(1))
            return f"{year}-01-01"
        
        # Try to extract year-month-day pattern
        ymd_match = re.match(r'(\d{4})-(\d{2})-(\d{2})', date_str)
        if ymd_match:
            return date_str
        
        return None
