# Jenkins Pipeline - infra-data-pipelines

This document describes Jenkins CI for the `infra-data-pipelines` repository.

## What This Pipeline Does

The root `Jenkinsfile` is pipeline-focused and currently runs:

1. checkout
2. Airflow DAG validation (`src/_airflow_dags_`)
3. test execution (`pytest`)

This repo CI is dedicated to **data pipelines** only. Modeling/training jobs are out of scope and belong to other repositories.

## Branch Behavior

- `main`: production-oriented pipeline validation
- `staging`: staging validation
- `dev/TPA-{issue_number}/{project}-{subproject}`: feature branch validation

If an environment-specific requirements file exists, Jenkins uses:

- `requirements-dev.txt` for feature branches
- `requirements-staging.txt` for `staging`
- `requirements-prod.txt` for `main`
- fallback to `requirements.txt` when needed

## Local Equivalents

Validate DAG imports:

```bash
export AIRFLOW_HOME=/tmp/airflow_home
export AIRFLOW__CORE__DAGS_FOLDER=src/_airflow_dags_
python -m airflow dags list
```

Run tests:

```bash
pytest tests/
```

## Notes

- Keep CI changes aligned with data-ingestion and orchestration needs.
- Avoid adding model-training/deployment steps in this repository.
