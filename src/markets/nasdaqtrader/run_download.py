#!/usr/bin/env python3
"""
Standalone script to download NASDAQ Trader symbol directory files.

Usage (from project root):
  python src/markets/nasdaqtrader/run_download.py

Saves nasdaqlisted.txt and otherlisted.txt to storage/dev/markets/nasdaqtrader by default.
"""

import argparse
import logging
import os
import sys
from pathlib import Path

logger = logging.getLogger(__name__)

# Run from project root or with PYTHONPATH=src
if __name__ == "__main__":
    project_root = Path(__file__).resolve().parent.parent.parent.parent
    storage_root = os.getenv("TRADING_AGENT_STORAGE")
    storage_env = os.getenv("ENV", "dev")
    if storage_root:
        storage_base = Path(storage_root) / storage_env
    else:
        storage_base = project_root / "storage" / storage_env
    default_out = str(storage_base / "markets" / "nasdaqtrader")

    parser = argparse.ArgumentParser(description="Download NASDAQ Trader symbol directory files")
    args = parser.parse_args()

    # Inline FTP download to avoid importing markets package
    from ftplib import FTP

    out_dir = Path(default_out)
    out_dir.mkdir(parents=True, exist_ok=True)
    files = ("nasdaqlisted.txt", "otherlisted.txt")
    written = []
    with FTP("ftp.nasdaqtrader.com") as ftp:
        ftp.login()
        ftp.cwd("symboldirectory")
        for name in files:
            path = out_dir / name
            with open(path, "wb") as f:
                ftp.retrbinary("RETR " + name, f.write)
            written.append(str(path))
    logger.info("Downloaded %s file(s) to %s: %s", len(written), out_dir, written)
    sys.exit(0)
