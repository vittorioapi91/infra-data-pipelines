"""
Airflow DAG for downloading SEC EDGAR filings by quarter

This DAG automatically downloads filings for the most recent completed quarter.
It runs monthly and downloads filings from the previous quarter.

Example: If run in January, it downloads Q4 filings from the previous year.

Uses the same in-process pattern as edgar_master_idx_download (PythonOperator
callable that imports and runs trading_agent code directly, no subprocess).
"""

import logging
from datetime import datetime, timedelta
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
    'retry_delay': timedelta(minutes=5),
    'start_date': days_ago(1),
}

# Get environment
AIRFLOW_ENV = os.getenv('AIRFLOW_ENV', 'dev')
# Map staging to 'test' for storage directory naming
STORAGE_ENV = 'test' if AIRFLOW_ENV == 'staging' else AIRFLOW_ENV

# Environment-specific storage for filings (same pattern as edgar_master_idx_dag)
# In Airflow: TRADING_AGENT_STORAGE -> storage-other-data/ta/{env}/edgar/filings
# Locally: TradingPythonAgent/storage/{env}/edgar/filings
storage_root = os.getenv('TRADING_AGENT_STORAGE')
if storage_root:
    OUTPUT_DIR = Path(storage_root) / 'edgar' / 'filings'
else:
    project_root = Path(__file__).resolve().parent.parent.parent.parent
    OUTPUT_DIR = project_root / 'storage' / STORAGE_ENV / 'edgar' / 'filings'
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# dbname from module; user/password from env only
ENV_CONFIG = {
    'dev': {'schedule': '0 2 1 * *'},  # 2 AM on 1st of month
    'staging': {'schedule': '0 2 1 * *'},
    'prod': {'schedule': '0 2 1 * *'},
}

env_config = ENV_CONFIG.get(AIRFLOW_ENV, ENV_CONFIG['dev'])


def get_previous_quarter(context):
    """
    Calculate the previous quarter based on execution date.

    Returns:
        tuple: (year, quarter) where quarter is QTR1, QTR2, QTR3, or QTR4
    """
    execution_date = context['execution_date']
    if isinstance(execution_date, str):
        execution_date = datetime.fromisoformat(execution_date.replace('Z', '+00:00'))

    # Get previous month (subtract 1 month)
    if execution_date.month == 1:
        prev_month = 12
        year = execution_date.year - 1
    else:
        prev_month = execution_date.month - 1
        year = execution_date.year

    # Determine quarter
    if prev_month in [1, 2, 3]:
        quarter = 'QTR1'
    elif prev_month in [4, 5, 6]:
        quarter = 'QTR2'
    elif prev_month in [7, 8, 9]:
        quarter = 'QTR3'
    else:
        quarter = 'QTR4'

    return year, quarter


def download_quarterly_filings(**context):
    """Download filings for the previous quarter (in-process, same pattern as edgar_master_idx_download)."""
    # Ensure path so we can import trading_agent (same as edgar_master_idx_dag)
    src_path = '/opt/airflow/src'
    if os.path.exists(src_path) and src_path not in sys.path:
        sys.path.insert(0, src_path)
    dag_dir = os.path.dirname(__file__)
    project_root_from_dag = os.path.abspath(os.path.join(dag_dir, '..', '..'))
    if os.path.exists(project_root_from_dag) and project_root_from_dag not in sys.path:
        sys.path.insert(0, project_root_from_dag)

    from trading_agent.fundamentals.edgar.filings.filings_downloader import FilingDownloader

    year, quarter = get_previous_quarter(context)
    logger.info("Downloading filings for %s %s", year, quarter)

    user_agent = os.getenv('EDGAR_USER_AGENT', 'VittorioApicella apicellavittorio@hotmail.it')
    filing_downloader = FilingDownloader(user_agent=user_agent)

    limit = None if AIRFLOW_ENV == 'prod' else 1000
    filters = {
        'year': year,
        'quarter': quarter,
        'form_type': '10-K',
    }

    downloaded_files = filing_downloader.download_filings(
        dbname=None,  # use module-derived (e.g. edgar)
        output_dir=str(OUTPUT_DIR),
        limit=limit,
        **filters
    )

    logger.info("Successfully downloaded %s filing(s)", len(downloaded_files))
    return str(len(downloaded_files))


dag = DAG(
    'edgar_filings_quarterly_download',
    default_args=default_args,
    description='Download EDGAR filings for previous quarter',
    schedule_interval=env_config['schedule'],
    catchup=False,
    tags=['edgar', 'filings', 'quarterly', AIRFLOW_ENV],
    max_active_runs=1,
)

download_quarterly = PythonOperator(
    task_id='download_quarterly_filings',
    python_callable=download_quarterly_filings,
    dag=dag,
)

download_quarterly
