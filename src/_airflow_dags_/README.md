# Airflow DAGs for infra-data-pipelines

This directory contains Airflow DAGs for orchestrating data collection workflows.

## Environment Support

All DAGs are environment-aware and automatically adapt based on the `AIRFLOW_ENV` environment variable:
- **dev**: Development environment (dev/* branches)
- **staging**: Staging environment (staging branch)
- **prod**: Production environment (main branch)

Each environment has its own Airflow instance:
- **Airflow DEV**: http://localhost:8082
- **Airflow TEST**: http://localhost:8083
- **Airflow PROD**: http://localhost:8084

## DAGs

### 1. `edgar_filings_quarterly_download_{env}`

Automatically downloads filings for the most recent completed quarter.

**Schedule**: Monthly (1st of each month at 2 AM)

**Behavior**:
- Runs on the 1st of each month
- Downloads filings from the previous quarter
- Example: If run in January, downloads Q4 filings from the previous year
- Default form type: 10-K
- Limited to 1000 filings in non-prod environments

**Parameters**: None (automatically calculated)

### 2. `fred_catalog_generation_{env}`

Generates the FRED (Federal Reserve Economic Data) catalog of all downloadable time series.

**Schedule**: Quarterly (1st of Jan/Apr/Jul/Oct at 3 AM)

**Behavior**:
- Retrieves all series metadata from the FRED API
- Saves to PostgreSQL database (`fred`)
- Saves to disk as CSV at `{TRADING_AGENT_STORAGE}/{env}/macro/fred/master/fred_series_master.csv`
- Uses full category search (slower but comprehensive)

**Parameters**: None

**Requirements**: `FRED_API_KEY` environment variable

## Configuration

### Environment-Specific Settings

Each environment has different default limits and schedules:

| DAG | Environment | Schedule |
|-----|-------------|----------|
| edgar_filings_quarterly_download | all | Monthly (1st, 2 AM) |
| fred_catalog_generation | all | Quarterly (1st of Jan/Apr/Jul/Oct, 3 AM) |

### Database Configuration

The DAGs use environment-aware database configuration:
- EDGAR DAGs: Database `edgar`
- FRED DAG: Database `fred`
- Database user: `tradingAgent`
- Database host/port: From environment variables (`POSTGRES_HOST`, `POSTGRES_PORT`)

### Output Directories

- **EDGAR filings**: `{TRADING_AGENT_STORAGE}/edgar/filings/{year}/{quarter}/{form_type}`
- **FRED catalog**: `{TRADING_AGENT_STORAGE}/{env}/macro/fred/master/fred_series_master.csv`

## Manual DAG Execution

### Via Airflow UI

1. Navigate to the DAG in Airflow UI
2. Click "Trigger DAG"

### Via Airflow CLI

```bash
# Trigger edgar_filings_quarterly_download
airflow dags trigger edgar_filings_quarterly_download_dev

# Trigger fred_catalog_generation
airflow dags trigger fred_catalog_generation_dev
```

## Dependencies

The DAGs require:
- `trading_agent` package installed (via wheel in Airflow container)
- PostgreSQL: `edgar` database (EDGAR DAGs), `fred` database (FRED DAG)
- EDGAR: `master_idx_files` table populated (via catalog generation)
- FRED: `FRED_API_KEY` environment variable

## Troubleshooting

### DAG not appearing in Airflow UI

1. Check that DAG files are in `.ops/.airflow/dags/`
2. Verify Airflow can import the DAG (check for syntax errors)
3. Check Airflow logs for import errors

### Import errors

1. Ensure `trading_agent` wheel is installed in Airflow container
2. Check `PYTHONPATH` includes project `src/` directory
3. Verify all dependencies are installed in Airflow environment

### Database connection errors

1. Verify `POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_PASSWORD` are set
2. Check database exists and is accessible from Airflow container
3. Verify `master_idx_files` table exists and has data

### No filings found

1. Ensure catalog has been generated (`--generate-catalog`)
2. Check filter parameters match data in database
3. Verify `master_idx_files` table has entries for the specified filters

## Adding New DAGs

To add a new DAG:

1. Create a new Python file in `.ops/.airflow/dags/`
2. Follow the pattern of existing DAGs:
   - Use `AIRFLOW_ENV` for environment detection
   - Include environment-specific configuration
   - Use appropriate retry and error handling
   - Add descriptive tags

3. Example structure:
   ```python
   from airflow import DAG
   from airflow.operators.python import PythonOperator
   import os
   
   AIRFLOW_ENV = os.getenv('AIRFLOW_ENV', 'dev')
   
   dag = DAG(
       f'my_dag_{AIRFLOW_ENV}',
       # ... configuration
   )
   ```

## Related Documentation

- [EDGAR Downloader Documentation](../../../src/trading_agent/fundamentals/edgar/README.md)
- [Environment Configuration](../../../src/trading_agent/config.py)
- [Wheel Installation](../../../WHEELS.md)
