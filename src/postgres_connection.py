"""
Shared PostgreSQL connection for the datalake.

All former per-product databases (ishares, edgar, fred, bls, bis, nasdaqtrader,
yfinance, eurostat) are now schemas inside the single "datalake" database.
Connection uses host postgres.{env}.local.info and user {env}.user;
password from POSTGRES_PASSWORD, port from POSTGRES_PORT (default 5432).
ENV defaults to "dev" if not set.
"""

import os
from typing import Optional

import psycopg2


def get_datalake_env() -> str:
    """Return current env name (e.g. dev, staging, prod). Uses ENV var, default 'dev'."""
    return os.getenv("ENV", "dev")


def get_datalake_connection(
    schema: str,
    user: Optional[str] = None,
    host: Optional[str] = None,
    password: Optional[str] = None,
    port: Optional[int] = None,
) -> psycopg2.extensions.connection:
    """
    Connect to the datalake database and set search_path to the given schema.

    Args:
        schema: Schema name (same as former database name: ishares, edgar, fred, etc.)
        user: Override user (default: {env}.user)
        host: Override host (default: postgres.{env}.local.info)
        password: Override password (default: POSTGRES_PASSWORD)
        port: Override port (default: POSTGRES_PORT or 5432)

    Returns:
        PostgreSQL connection with search_path set to schema.
    """
    env = get_datalake_env()
    conn = psycopg2.connect(
        dbname="datalake",
        user=user or os.getenv("POSTGRES_USER"),
        host=host or os.getenv("POSTGRES_HOST"),
        password=password if password is not None else os.getenv("POSTGRES_PASSWORD"),
        port=port if port is not None else int(os.getenv("POSTGRES_PORT")),
    )
    with conn.cursor() as cur:
        cur.execute("SET search_path TO %s", (schema,))
    return conn
