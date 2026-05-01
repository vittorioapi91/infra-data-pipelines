"""
Main entry point for NASDAQ Trader symbol directory download.

Downloads nasdaqlisted.txt and otherlisted.txt into storage/dev/markets/nasdaqtrader
using storage derived from TRADING_AGENT_STORAGE / ENV. Upload to DB is available to other scripts via
nasdaqtrader_postgres.upload_symbol_directory_to_db().
"""

import argparse
import logging
import os
from pathlib import Path

from .nasdaqtrader_downloader import download_symbol_directory

logger = logging.getLogger(__name__)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Download NASDAQ Trader symbol directory files (nasdaqlisted.txt, otherlisted.txt)"
    )
    args = parser.parse_args()

    project_root = Path(__file__).resolve().parent.parent.parent.parent
    storage_root = os.getenv("TRADING_AGENT_STORAGE")
    storage_env = os.getenv("ENV", "dev")
    if storage_root:
        storage_base = Path(storage_root) / storage_env
    else:
        storage_base = project_root / "storage" / storage_env
    output_dir = str(storage_base / "markets" / "nasdaqtrader")

    paths = download_symbol_directory(output_dir=output_dir)
    logger.info("Downloaded %s file(s) to %s: %s", len(paths), output_dir, paths)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
