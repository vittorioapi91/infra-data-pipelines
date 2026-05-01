"""
Main entry point for BLS Economic Data Downloader

This module provides a command-line interface to download time series data from BLS.
"""

import logging
import os
import sys
# Add current directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from bls_data_downloader import BLSDataDownloader
from src.config import load_environment_config

logger = logging.getLogger(__name__)


def main():
    """Main function to download BLS economic data"""
    import argparse
    from datetime import datetime

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )

    # Ensure .env.dev/.env.test/.env.prod is loaded based on current git branch.
    # This makes sure BLS_API_KEY is available to BLSDataDownloader.
    load_environment_config()

    # Use environment defaults when available (loaded by load_environment_config()).
    default_dbuser = os.getenv("POSTGRES_USER", "tradingAgent")
    default_dbhost = os.getenv("POSTGRES_HOST", "localhost")
    default_dbport = int(os.getenv("POSTGRES_PORT", "5432"))
    
    # Get the directory where this script is located
    script_dir = os.path.dirname(os.path.abspath(__file__))
    default_data_dir = os.path.join(script_dir, 'data')
    
    parser = argparse.ArgumentParser(description='Download BLS economic time series data')
    parser.add_argument('--api-key', type=str, help='BLS API key (or set BLS_API_KEY env var)')
    parser.add_argument('--series', type=str, nargs='+',
                       help='BLS series IDs to download (e.g., CUUR0000SA0 SUUR0000SA0). Required unless --generate-catalog is used.')
    parser.add_argument('--start-year', type=int, default=2020,
                       help='Start year (default: 2020)')
    parser.add_argument('--end-year', type=int, default=datetime.now().year,
                       help=f'End year (default: {datetime.now().year})')
    parser.add_argument('--generate-catalog', action='store_true',
                       help='Generate BLS series catalogue (save to PostgreSQL)')
    parser.add_argument('--series-files-dir', type=str, default=None,
                       help='Optional local path containing BLS bulk *.series files. '
                            'Expected layout: <dir>/<survey>/<survey>.series (or <dir>/<survey>.series).')

    
    # Database connection parameters
    parser.add_argument('--dbuser', type=str, default=default_dbuser,
                       help='PostgreSQL user (default: from POSTGRES_USER env)')
    parser.add_argument('--dbhost', type=str, default=default_dbhost,
                       help='PostgreSQL host (default: from POSTGRES_HOST env)')
    parser.add_argument('--dbport', type=int, default=default_dbport,
                       help='PostgreSQL port (default: from POSTGRES_PORT env)')
    parser.add_argument('--dbpassword', type=str, default=None,
                       help='PostgreSQL password (or set POSTGRES_PASSWORD env var)')
    
    args = parser.parse_args()
    
    try:
        # Get password from args or environment
        password = args.dbpassword or os.getenv('POSTGRES_PASSWORD', '')

        # Database name is fixed to 'bls' in BLSDataDownloader.__init__
        downloader = BLSDataDownloader(
            api_key=args.api_key,
            user=args.dbuser,
            host=args.dbhost,
            password=password,
            port=args.dbport
        )
        
        # If generate-catalog is requested, generate the catalog in PostgreSQL
        if args.generate_catalog:
            # Provision the schema if tables don't exist.
            # This DDL is executed only under an explicit CLI operation.
            from bls_postgres import get_postgres_connection, ensure_bls_schema

            conn = get_postgres_connection(
                user=args.dbuser,
                host=args.dbhost,
                password=password,
                port=args.dbport,
            )
            ensure_bls_schema(conn)
            conn.close()

            logger.info("Generating BLS series catalogue...")
            series_count = downloader.get_all_downloadable_series(
                series_files_dir=args.series_files_dir,
            )
            logger.info("Catalogue generated successfully with %s series! Database: bls", series_count)
            return 0
        
        # Series download requires --series argument
        if not args.series:
            parser.error("--series is required unless --generate-catalog is used")
        
        # Download series
        logger.info("Downloading %s BLS series... Series IDs: %s Date range: %s to %s",
                    len(args.series), ", ".join(args.series), args.start_year, args.end_year)
        
        df = downloader.download_multiple_series(
            args.series,
            args.start_year,
            args.end_year,
            save_to_db=True
        )
        
        if df.empty:
            logger.warning("No data was downloaded")
            return 1
        
        logger.info("Data saved to PostgreSQL database: bls. Download complete! Downloaded %s data points across %s series", len(df), len(df.columns))
        
    except Exception as e:
        logger.exception("Error: %s", e)
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())

