"""
Airflow DAG for FRED catalog generation

This DAG generates the FRED (Federal Reserve Economic Data) catalog of all
downloadable time series. It:
1. Retrieves all series metadata from the FRED API
2. Saves to PostgreSQL database
3. Saves to disk as CSV at {TRADING_AGENT_STORAGE}/{env}/macro/fred/master/fred_series_master.csv

Uses the same in-process pattern as edgar DAGs (PythonOperator callable
that imports and runs trading_agent code directly).
"""

import logging
from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import os
import sys
from pathlib import Path

logger = logging.getLogger(__name__)

# Default arguments
default_args = {
    'owner': 'trading_agent',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=15),
    'start_date': days_ago(1),
}

# Get environment
AIRFLOW_ENV = os.getenv('AIRFLOW_ENV', 'dev')
STORAGE_ENV = 'test' if AIRFLOW_ENV == 'staging' else AIRFLOW_ENV

# Environment-specific storage for catalog (same pattern as edgar DAGs)
# In Airflow: TRADING_AGENT_STORAGE is a common root (without env),
# so effective path is TRADING_AGENT_STORAGE/{env}/macro/fred/master.
# Locally: TradingPythonAgent/storage/{env}/macro/fred/master.
storage_root = os.getenv('TRADING_AGENT_STORAGE')
if storage_root:
    CATALOG_OUTPUT_DIR = Path(storage_root) / STORAGE_ENV / 'macro' / 'fred' / 'master'
else:
    project_root = Path(__file__).resolve().parent.parent.parent.parent
    CATALOG_OUTPUT_DIR = project_root / 'storage' / STORAGE_ENV / 'macro' / 'fred' / 'master'
CATALOG_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

ENV_CONFIG = {
    'dev': {'schedule': '0 3 1 */3 *'},   # 3 AM on 1st day of every 3rd month (quarterly)
    'staging': {'schedule': '0 3 1 */3 *'},
    'prod': {'schedule': '0 3 1 */3 *'},
}
env_config = ENV_CONFIG.get(AIRFLOW_ENV, ENV_CONFIG['dev'])


def generate_fred_catalog(**context):
    """Generate FRED catalog (saves to PostgreSQL and disk as CSV)."""
    src_path = '/opt/airflow/src'
    if os.path.exists(src_path) and src_path not in sys.path:
        sys.path.insert(0, src_path)
    dag_dir = os.path.dirname(__file__)
    project_root_from_dag = os.path.abspath(os.path.join(dag_dir, '..', '..'))
    if os.path.exists(project_root_from_dag) and project_root_from_dag not in sys.path:
        sys.path.insert(0, project_root_from_dag)

    from trading_agent.macro.fred.fred_data_downloader import FREDDataDownloader

    api_key = os.getenv('FRED_API_KEY')
    if not api_key:
        raise ValueError("FRED_API_KEY environment variable is required")

    downloader = FREDDataDownloader(api_key=api_key)
    series_list = downloader.get_all_downloadable_series(
        use_categories=True,
        use_known_categories_only=False,  # full-category-search
        use_search_terms=False,
        catalog_output_dir=str(CATALOG_OUTPUT_DIR),
    )
    logger.info("Generated catalog with %s series", len(series_list))
    return len(series_list)


dag = DAG(
    'fred_catalog_generation',
    default_args=default_args,
    description='Generate FRED catalog of downloadable time series',
    schedule_interval=env_config['schedule'],
    catchup=False,
    tags=['fred', 'macro', 'catalog', 'data-download', AIRFLOW_ENV],
    max_active_runs=1,
)

generate_catalog = PythonOperator(
    task_id='generate_fred_catalog',
    python_callable=generate_fred_catalog,
    dag=dag,
)

generate_catalog
