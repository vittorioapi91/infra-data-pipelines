"""
Airflow DAG for Eurostat catalog generation

This DAG generates the Eurostat catalog of all downloadable datasets. It:
1. Retrieves all dataflows from the Eurostat SDMX API
2. Fetches structure (dimensions) for each dataset
3. Saves to PostgreSQL database (eurostat schema)

Uses the same in-process pattern as edgar/FRED DAGs (PythonOperator callable
that imports and runs trading_agent code directly).

Requires POSTGRES_* env vars for the eurostat database.
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

ENV_CONFIG = {
    'dev': {'schedule': '0 4 1 */3 *'},   # 4 AM on 1st day of every 3rd month (quarterly)
    'staging': {'schedule': '0 4 1 */3 *'},
    'prod': {'schedule': '0 4 1 */3 *'},
}
env_config = ENV_CONFIG.get(AIRFLOW_ENV, ENV_CONFIG['dev'])


def generate_eurostat_catalog(**context):
    """Generate Eurostat catalog (saves to PostgreSQL)."""
    src_path = '/opt/airflow/src'
    if os.path.exists(src_path) and src_path not in sys.path:
        sys.path.insert(0, src_path)
    dag_dir = os.path.dirname(__file__)
    project_root_from_dag = os.path.abspath(os.path.join(dag_dir, '..', '..'))
    if os.path.exists(project_root_from_dag) and project_root_from_dag not in sys.path:
        sys.path.insert(0, project_root_from_dag)

    from trading_agent.macro.eurostat.eurostat_data_downloader import EurostatDataDownloader

    # Optional limit for testing (e.g. EUROSTAT_LIMIT=10)
    limit = os.getenv('EUROSTAT_LIMIT')
    limit = int(limit) if limit else None

    downloader = EurostatDataDownloader()
    datasets_list = downloader.get_all_downloadable_series(limit=limit)

    logger.info("Generated catalog with %s datasets", len(datasets_list))
    return len(datasets_list)


dag = DAG(
    'eurostat_catalog_generation',
    default_args=default_args,
    description='Generate Eurostat catalog of downloadable datasets (SDMX dataflows)',
    schedule_interval=env_config['schedule'],
    catchup=False,
    tags=['eurostat', 'macro', 'catalog', 'data-download', 'sdmx', AIRFLOW_ENV],
    max_active_runs=1,
)

generate_catalog = PythonOperator(
    task_id='generate_eurostat_catalog',
    python_callable=generate_eurostat_catalog,
    dag=dag,
)

generate_catalog
