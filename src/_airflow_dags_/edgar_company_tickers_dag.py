"""
Airflow DAG for EDGAR company_tickers.json download

This DAG orchestrates downloading the company_tickers.json file from SEC EDGAR.
This file contains a mapping of all companies with their CIK, ticker, and name.

The file is updated periodically by the SEC, so this DAG runs daily to ensure
we have the latest company data.
"""

import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
try:
    from airflow.sdk.timezone import datetime as tz_datetime
except ImportError:
    from airflow.utils.timezone import datetime as tz_datetime
import os
import sys
from pathlib import Path

logger = logging.getLogger(__name__)

# Get environment from Airflow environment variable (set in container)
AIRFLOW_ENV = os.getenv('AIRFLOW_ENV', 'dev')
# Map staging to 'test' for storage directory naming
STORAGE_ENV = 'test' if AIRFLOW_ENV == 'staging' else AIRFLOW_ENV

# Add project root to path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Environment-specific storage directory for company_tickers.json
STORAGE_DIR = Path(project_root) / 'storage' / STORAGE_ENV / 'edgar'
STORAGE_DIR.mkdir(parents=True, exist_ok=True)

default_args = {
    'owner': 'trading_agent',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=15),
    'start_date': tz_datetime(2024, 1, 1),
}

dag = DAG(
    'edgar_company_tickers_download',
    default_args=default_args,
    description='Download SEC EDGAR company_tickers.json file',
    schedule='0 2 * * *',  # Run daily at 2 AM to catch updates
    catchup=False,
    tags=['edgar', 'fundamentals', 'company-tickers', 'data-download'],
)


def download_company_tickers(**context):
    """Download company_tickers.json from SEC EDGAR"""
    # The src directory is mounted at /opt/airflow/src and added to PYTHONPATH
    # So we can import directly. If that doesn't work, add the path explicitly.
    import sys
    import os
    
    # Ensure /opt/airflow/src is in path (where src is mounted in the container)
    src_path = '/opt/airflow/src'
    if os.path.exists(src_path) and src_path not in sys.path:
        sys.path.insert(0, src_path)
    # Also try adding from DAG directory (fallback)
    dag_dir = os.path.dirname(__file__)  # /opt/airflow/dags
    project_root_from_dag = os.path.abspath(os.path.join(dag_dir, '..', 'src'))
    if os.path.exists(project_root_from_dag) and project_root_from_dag not in sys.path:
        sys.path.insert(0, project_root_from_dag)
    
    # Import here to avoid import errors at DAG parse time
    # Note: /opt/airflow/src contains trading_agent/ directly (not src/trading_agent/)
    # because the mount is ../../src:/opt/airflow/src
    from trading_agent.fundamentals.edgar.company_tickers import CompaniesDownloader
    
    # Get user agent from environment or use default
    user_agent = os.getenv('EDGAR_USER_AGENT', 'VittorioApicella apicellavittorio@hotmail.it')
    
    # Initialize downloader
    downloader = CompaniesDownloader(user_agent=user_agent)
    
    # Download company_tickers.json to environment-specific storage directory
    companies_file = downloader.download_company_tickers_json(output_dir=STORAGE_DIR)
    
    logger.info("Successfully downloaded company_tickers.json to %s", companies_file)
    return str(companies_file)


# Task: Download company_tickers.json
download_task = PythonOperator(
    task_id='download_company_tickers',
    python_callable=download_company_tickers,
    dag=dag,
)
