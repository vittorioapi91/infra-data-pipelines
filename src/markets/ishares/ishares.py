"""
Main entry point for iShares ETF Data Scraper

This module provides a command-line interface to scrape ETF data from iShares.
"""

import os
import sys
from pathlib import Path

try:
    from .ishares_scraper import iSharesScraper
except ImportError:
    from src.markets.ishares.ishares_scraper import iSharesScraper

ISHARES_ETF_URL = "https://www.ishares.com/us/products/etf-investments#/?productView=etf&style=44342&pageNumber=1&sortColumn=totalNetAssets&sortDirection=desc&dataView=keyFacts"


def _get_ishares_storage() -> Path:
    """Return storage/{ENV}/markets/ishares directory.

    Uses TRADING_AGENT_STORAGE as a common storage root (without env) when set.
    """
    project_root = Path(__file__).resolve().parent.parent.parent.parent
    storage_root = os.getenv('TRADING_AGENT_STORAGE')
    storage_env = os.getenv('ENV', 'dev')
    if storage_root:
        return Path(storage_root) / storage_env / 'markets' / 'ishares'
    return project_root / 'storage' / storage_env / 'markets' / 'ishares'


def main():
    """Main function to scrape iShares ETF data"""
    import argparse

    ishares_storage = _get_ishares_storage()
    ishares_storage.mkdir(parents=True, exist_ok=True)
    etfs_csv = ishares_storage / 'ishares_etfs.csv'

    parser = argparse.ArgumentParser()
    parser.add_argument('--download', action='store_true', help='Download data (requires --master and/or --holdings)')
    parser.add_argument('--master', action='store_true', help='Download ishares_etfs.csv (deletes existing, downloads afresh)')
    parser.add_argument('--holdings', action='store_true', help='Download holdings CSVs (deletes existing, downloads afresh)')
    parser.add_argument('--upload', action='store_true', help='Upload from existing CSVs to PostgreSQL (ishares DB, no scrape)')
    args = parser.parse_args()

    try:
        if args.upload:
            if not etfs_csv.exists():
                print("Error: ishares_etfs.csv not found. Run --download --master first.")
                return 1
        elif args.download:
            if not args.master and not args.holdings:
                print("Error: --download requires --master and/or --holdings")
                return 1
        else:
            print("Error: specify --download (with --master/--holdings) or --upload")
            return 1

        scraper = iSharesScraper()
        if args.upload:
            pass
        else:
            import pandas as pd
            holdings_dir = ishares_storage / 'holdings'

            if args.master:
                if etfs_csv.exists():
                    etfs_csv.unlink()
                etfs = scraper.scrape_all_etfs(url=ISHARES_ETF_URL, csv_path=str(ishares_storage))
                if not etfs:
                    return 1

            if args.holdings:
                for sub in ("summary", "detailed"):
                    subdir = holdings_dir / sub
                    subdir.mkdir(parents=True, exist_ok=True)
                    for f in subdir.glob("*_holdings.csv"):
                        f.unlink()
                if not etfs_csv.exists():
                    print("Error: ishares_etfs.csv not found. Run --download --master first.")
                    return 1
                etfs = pd.read_csv(etfs_csv).to_dict('records')
                try:
                    if not os.getenv("POSTGRES_PASSWORD"):
                        try:
                            from src.config import load_environment_config
                            load_environment_config()
                        except Exception:
                            from dotenv import load_dotenv
                            project_root = Path(__file__).resolve().parent.parent.parent.parent
                            for f in (".env.dev", ".env"):
                                p = project_root / f
                                if p.exists():
                                    load_dotenv(p, override=True)
                                    break
                    from .ishares_postgres import get_postgres_connection, get_holdings_tickers_updated_today
                    conn = get_postgres_connection()
                    skip_tickers = get_holdings_tickers_updated_today(conn)
                    conn.close()
                    if skip_tickers:
                        etfs = [e for e in etfs if (e.get('ticker') or e.get('Ticker') or '').strip() not in skip_tickers]
                        print(f"Skipping {len(skip_tickers)} ticker(s) already updated today", file=sys.stderr)
                except Exception as e:
                    print(f"Warning: could not skip tickers updated today ({e}); downloading all", file=sys.stderr)
                limit_str = os.getenv('ISHARES_HOLDINGS_LIMIT')
                limit = int(limit_str) if limit_str else None
                scraper.scrape_holdings(etfs, holdings_dir, limit=limit)

        if args.upload:
            if not os.getenv("POSTGRES_PASSWORD"):
                try:
                    from src.config import load_environment_config
                    load_environment_config()
                except Exception:
                    from dotenv import load_dotenv
                    project_root = Path(__file__).resolve().parent.parent.parent.parent
                    for f in (".env.dev", ".env"):
                        p = project_root / f
                        if p.exists():
                            load_dotenv(p, override=True)
                            break
            from .ishares_postgres import (
                apply_views,
                get_postgres_connection,
                init_ishares_postgres_tables,
                upload_etfs,
                upload_holdings,
            )
            conn = get_postgres_connection()
            init_ishares_postgres_tables(conn)
            n_etfs = upload_etfs(conn, etfs_csv)
            n_holdings = upload_holdings(conn, ishares_storage / 'holdings')
            apply_views(conn)
            conn.close()
            print(f"Uploaded: {n_etfs} ETFs, {n_holdings} holdings")

        return 0
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        raise


if __name__ == "__main__":
    exit(main())
