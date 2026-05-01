"""
Airflow DAG for EDGAR master.idx files generation

This DAG orchestrates:
1. Downloading master.idx files from SEC EDGAR (only new/failed quarters)
2. Parsing and saving to CSV files
3. Loading parsed data into PostgreSQL database

The DAG uses a ledger table to track download status and only processes
quarters that are new or have previously failed.
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

# Environment-specific storage directory for master.idx files
# In Airflow: use TRADING_AGENT_STORAGE as common root (without env),
# so effective path is TRADING_AGENT_STORAGE/{env}/edgar/master.
# Locally: use TradingPythonAgent/storage/{env}/edgar/master.
storage_root = os.getenv('TRADING_AGENT_STORAGE')
if storage_root:
    # Running in Airflow: use storage-other-data/ta/{env}/edgar/master
    MASTER_DIR = Path(storage_root) / STORAGE_ENV / 'edgar' / 'master'
else:
    # Running locally: use TradingPythonAgent/storage/{env}/edgar/master
    MASTER_DIR = Path(project_root) / 'storage' / STORAGE_ENV / 'edgar' / 'master'

MASTER_DIR.mkdir(parents=True, exist_ok=True)

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
    'edgar_master_idx_download',
    default_args=default_args,
    description='Download and process SEC EDGAR master.idx files',
    schedule='0 0 1 */3 *',  # Run quarterly (1st day of every 3rd month) to catch new quarters
    catchup=False,
    tags=['edgar', 'fundamentals', 'master-idx', 'data-download'],
)


def download_master_idx_files(**context):
    """Download master.idx files (only new/failed quarters)"""
    # The src directory is mounted at /opt/airflow/src and added to PYTHONPATH
    # So we can import directly. If that doesn't work, add the path explicitly.
    import sys
    import os
    import json
    import time
    
    # #region agent log
    log_path = '/Users/Snake91/CursorProjects/TradingPythonAgent/.cursor/debug.log'
    def log_debug(location, message, data, hypothesis_id):
        try:
            with open(log_path, 'a') as f:
                f.write(json.dumps({
                    "sessionId": "debug-session",
                    "runId": "run1",
                    "hypothesisId": hypothesis_id,
                    "location": location,
                    "message": message,
                    "data": data,
                    "timestamp": int(time.time() * 1000)
                }) + '\n')
        except: pass
    # #endregion
    
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
    from trading_agent.fundamentals.edgar.master_idx.master_idx import MasterIdxManager
    from trading_agent.fundamentals.edgar.edgar_postgres import (
        get_postgres_connection,
        get_module_dbname,
        init_master_idx_tables
    )
    
    # Datalake connection: ENV drives host (postgres.{env}.local.info) and user ({env}.user)
    schema = get_module_dbname()
    env = os.getenv('ENV', os.getenv('AIRFLOW_ENV', 'dev'))
    dbpassword = os.getenv('POSTGRES_PASSWORD', '')
    dbport = int(os.getenv('POSTGRES_PORT', '5432'))
    
    # #region agent log
    log_debug("edgar_master_idx_dag.py:download_master_idx_files", "DB connection params (datalake)", {
        "schema": schema,
        "ENV": env,
        "POSTGRES_PORT": dbport,
        "POSTGRES_PASSWORD_set": bool(dbpassword),
    }, "C")
    # #endregion
    
    # Get start year from context or use default
    start_year = context.get('dag_run').conf.get('start_year') if context.get('dag_run') else None
    
    # Initialize master index manager with environment-specific storage directory
    user_agent = os.getenv('EDGAR_USER_AGENT', 'VittorioApicella apicellavittorio@hotmail.it')
    master_idx_manager = MasterIdxManager(user_agent=user_agent, master_dir=MASTER_DIR)
    
    # Connect to database (datalake, edgar schema)
    conn = get_postgres_connection(
        password=dbpassword or None,
        port=dbport,
    )
    
    # #region agent log
    cur = conn.cursor()
    cur.execute("SELECT current_database(), current_user, current_setting('search_path');")
    db_info = cur.fetchone()
    cur.close()
    log_debug("edgar_master_idx_dag.py:download_master_idx_files", "Actual DB connection established", {
        "current_database": db_info[0],
        "current_user": db_info[1],
        "search_path": db_info[2] if len(db_info) > 2 else None
    }, "D")
    # #endregion
    
    if AIRFLOW_ENV == 'dev':
        logger.info("DEBUG (dev): Connected to PostgreSQL: Database=%s User=%s Host=%s:%s Search Path=%s",
                    db_info[0], db_info[1], dbhost, dbport, db_info[2] if len(db_info) > 2 else 'default')
    
    try:
        # Initialize master.idx tables only (creates schema and tables)
        init_master_idx_tables(conn)
        
        # Download only new/failed quarters
        master_idx_manager.save_master_idx_to_disk(conn, start_year=start_year)
    finally:
        conn.close()


def save_master_idx_to_database(**context):
    """Save parsed CSV files to PostgreSQL database"""
    # The src directory is mounted at /opt/airflow/src and added to PYTHONPATH
    # So we can import directly. If that doesn't work, add the path explicitly.
    import sys
    import os
    import json
    import time
    
    # #region agent log
    log_path = '/Users/Snake91/CursorProjects/TradingPythonAgent/.cursor/debug.log'
    def log_debug(location, message, data, hypothesis_id):
        try:
            with open(log_path, 'a') as f:
                f.write(json.dumps({
                    "sessionId": "debug-session",
                    "runId": "run1",
                    "hypothesisId": hypothesis_id,
                    "location": location,
                    "message": message,
                    "data": data,
                    "timestamp": int(time.time() * 1000)
                }) + '\n')
        except: pass
    # #endregion
    
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
    from trading_agent.fundamentals.edgar.master_idx.master_idx import MasterIdxManager
    from trading_agent.fundamentals.edgar.edgar_postgres import (
        get_postgres_connection,
        get_module_dbname,
        init_master_idx_tables
    )
    
    # Datalake connection: ENV drives host and user
    schema = get_module_dbname()
    env = os.getenv('ENV', os.getenv('AIRFLOW_ENV', 'dev'))
    dbpassword = os.getenv('POSTGRES_PASSWORD', '')
    dbport = int(os.getenv('POSTGRES_PORT', '5432'))
    
    # #region agent log
    log_debug("edgar_master_idx_dag.py:save_master_idx_to_database", "DB connection params (datalake)", {
        "schema": schema,
        "ENV": env,
        "POSTGRES_PORT": dbport,
        "POSTGRES_PASSWORD_set": bool(dbpassword),
    }, "C")
    # #endregion
    
    # Initialize master index manager with environment-specific storage directory
    user_agent = os.getenv('EDGAR_USER_AGENT', 'VittorioApicella apicellavittorio@hotmail.it')
    master_idx_manager = MasterIdxManager(user_agent=user_agent, master_dir=MASTER_DIR)
    
    # Connect to database (datalake, edgar schema)
    conn = get_postgres_connection(
        password=dbpassword or None,
        port=dbport,
    )
    
    # #region agent log
    cur = conn.cursor()
    cur.execute("SELECT current_database(), current_user, current_setting('search_path');")
    db_info = cur.fetchone()
    cur.close()
    log_debug("edgar_master_idx_dag.py:save_master_idx_to_database", "Actual DB connection established", {
        "current_database": db_info[0],
        "current_user": db_info[1],
        "search_path": db_info[2] if len(db_info) > 2 else None
    }, "D")
    # #endregion
    
    if AIRFLOW_ENV == 'dev':
        logger.info("DEBUG (dev): Connected to PostgreSQL: Database=%s User=%s Search Path=%s",
                    db_info[0], db_info[1], db_info[2] if len(db_info) > 2 else 'default')
    
    try:
        # Ensure master.idx tables exist (in case they weren't created in download step)
        init_master_idx_tables(conn)
        
        # Save parsed CSV files to database
        master_idx_manager.save_master_idx_to_db(conn)
    finally:
        conn.close()


# Task 1: Download master.idx files (only new/failed)
download_task = PythonOperator(
    task_id='download_master_idx_files',
    python_callable=download_master_idx_files,
    dag=dag,
)

# Task 2: Save parsed data to database
save_to_db_task = PythonOperator(
    task_id='save_master_idx_to_database',
    python_callable=save_master_idx_to_database,
    dag=dag,
)

# Set task dependencies
download_task >> save_to_db_task
