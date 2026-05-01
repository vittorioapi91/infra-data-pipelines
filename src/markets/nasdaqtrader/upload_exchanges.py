"""
One-off: create nasdaqtrader.exchanges and upload the 5 reference rows.
Run from repo root: python src/markets/nasdaqtrader/upload_exchanges.py

Loads nasdaqtrader_postgres directly so we don't pull in the rest of markets (e.g. yfinance).
"""
import importlib.util
import logging
import os
import sys
from pathlib import Path

logger = logging.getLogger(__name__)

_root = Path(__file__).resolve().parent.parent.parent.parent
sys.path.insert(0, str(_root))

# Load env
try:
    from src.config import load_environment_config
    load_environment_config()
except Exception:
    if not os.getenv("POSTGRES_PASSWORD"):
        from dotenv import load_dotenv
        load_dotenv(_root / ".env.dev", override=True)

# Import only nasdaqtrader_postgres (avoid loading markets.__init__ and yfinance)
_spec = importlib.util.spec_from_file_location(
    "nasdaqtrader_postgres",
    Path(__file__).resolve().parent / "nasdaqtrader_postgres.py",
)
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)
get_postgres_connection = _mod.get_postgres_connection
init_nasdaqtrader_postgres_tables = _mod.init_nasdaqtrader_postgres_tables
upload_exchanges_reference_data = _mod.upload_exchanges_reference_data


def main() -> int:
    conn = get_postgres_connection()
    init_nasdaqtrader_postgres_tables(conn)  # creates exchanges if not exists
    n = upload_exchanges_reference_data(conn)
    conn.close()
    logger.info("Uploaded %s rows into nasdaqtrader.exchanges.", n)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
