"""
Run with: python -m src.markets.nasdaqtrader

Downloads nasdaqlisted.txt and otherlisted.txt into storage/dev/markets/nasdaqtrader.
Supports --upload-db to upsert into the nasdaqtrader PostgreSQL database.
"""

from .nasdaqtrader import main

if __name__ == "__main__":
    raise SystemExit(main())
