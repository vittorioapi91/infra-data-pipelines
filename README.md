# infra-data-pipelines

Data pipelines repository for ingesting, validating, and storing financial datasets.

This repository contains data pipelines code and depends on the utilities setup managed in `infra-platform`:

- data ingestion modules (`fundamentals`, `macro`, `markets`)
- Airflow DAGs (`src/_airflow_dags_`)
- pipeline-oriented tests
- pipeline CI (`Jenkinsfile`)

## Scope

`infra-data-pipelines` focuses on:

- SEC EDGAR ingestion and catalog/scrape workflows
- macro data ingestion (FRED, BLS, BIS, Eurostat, IMF)
- market data ingestion (Yahoo Finance, iShares, NASDAQ Trader, Hyperliquid, FINRA)
- writing and updating PostgreSQL datasets used by downstream systems

## Repository Layout

```text
infra-data-pipelines/
├── src/
│   ├── _airflow_dags_/         # Airflow orchestration DAGs
│   ├── fundamentals/           # EDGAR ingestion
│   ├── macro/                  # Macro dataset ingestion
│   └── markets/                # Market dataset ingestion
├── tests/                      # Pipeline tests
├── scripts/                    # Pipeline utility scripts
├── .vscode/                    # Pipeline debug/launch configs
├── Jenkinsfile                 # Pipeline CI
└── README.md
```

## Quick Start

```bash
git clone https://github.com/vittorioapi91/infra-data-pipelines.git
cd infra-data-pipelines
python -m venv venv
source venv/bin/activate
pip install -r requirements-dev.txt
```

## Common Commands

Generate BLS catalog:

```bash
python -m src.macro.bls.main --generate-catalog
```

Download selected FRED series:

```bash
python -m src.macro.fred.fred --series GDP UNRATE CPIAUCSL
```

Run tests:

```bash
pytest tests/
```

## CI

The Jenkins pipeline in this repo validates Airflow DAG imports and runs the unit test suite.  
See `JENKINS.md` for pipeline behavior and branch workflow details.
