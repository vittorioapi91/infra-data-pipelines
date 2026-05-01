"""
Main entry point for IMF Economic Data Downloader

This module provides a command-line interface to download time series data from IMF.
"""

import logging
import os
import sys
from pathlib import Path
# Add current directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from imf_data_downloader import IMFDataDownloader

logger = logging.getLogger(__name__)


def main():
    """Main function to download IMF economic data"""
    import argparse
    from datetime import datetime
    
    # Resolve output directory from storage env (no --output-dir flag)
    # storage/{ENV}/macro/imf, or TRADING_AGENT_STORAGE/{ENV}/macro/imf when
    # TRADING_AGENT_STORAGE is set to a common storage root (without env).
    project_root = Path(__file__).resolve().parent.parent.parent.parent
    storage_root = os.getenv("TRADING_AGENT_STORAGE")
    storage_env = os.getenv("ENV", "dev")
    if storage_root:
        storage_base = Path(storage_root) / storage_env
    else:
        storage_base = project_root / "storage" / storage_env
    output_dir = storage_base / "macro" / "imf"
    
    parser = argparse.ArgumentParser(description='Download IMF economic time series data')
    parser.add_argument('--database', type=str, default='IFS',
                       help='IMF database ID (default: IFS). Common: IFS, BOP, WEO, GFS, DOT')
    parser.add_argument('--series', type=str, nargs='+',
                       help='Series codes to download (e.g., NGDP_RPCH). Required unless --generate-parquet, --list-databases, --list-indicators, or --list-countries is used.')
    parser.add_argument('--start-date', type=str,
                       help='Start date (YYYY-MM-DD format)')
    parser.add_argument('--end-date', type=str,
                       help=f'End date (YYYY-MM-DD format, default: today)')
    parser.add_argument('--countries', type=str, nargs='+',
                       help='Country codes (ISO 3-letter codes, e.g., USA GBR JPN)')
    # Note: output dir is derived from TRADING_AGENT_STORAGE / ENV; no --output-dir flag.
    parser.add_argument('--list-databases', action='store_true',
                       help='List all available IMF databases and exit')
    parser.add_argument('--list-indicators', action='store_true',
                       help='List all available indicators for the specified database and exit')
    parser.add_argument('--list-countries', action='store_true',
                       help='List all available countries for the specified database and exit')
    parser.add_argument('--generate-db', action='store_true',
                       help='Generate PostgreSQL database with all downloadable indicators')
    parser.add_argument('--from-db', action='store_true',
                       help='Download indicators from the database')
    parser.add_argument('--search', type=str,
                       help='Search for series by search term')
    
    args = parser.parse_args()
    
    try:
        downloader = IMFDataDownloader()
        
        # If listing databases, do that and exit
        if args.list_databases:
            databases_df = downloader.get_databases()
            logger.info("Found %s IMF databases:", len(databases_df))
            if not databases_df.empty:
                for _, row in databases_df.iterrows():
                    db_id = row.get('database_id', 'N/A')
                    db_desc = row.get('description', 'N/A')
                    logger.info("  %s - %s", db_id, db_desc)
            return 0
        
        # If listing indicators, do that and exit
        if args.list_indicators:
            indicators = downloader.get_indicators(args.database)
            logger.info("Found %s indicators in %s:", len(indicators), args.database)
            for ind in indicators[:50]:  # Show first 50
                ind_code = ind.get('code', 'N/A')
                ind_name = ind.get('name', 'N/A')
                logger.info("  %s - %s", ind_code, ind_name)
            if len(indicators) > 50:
                logger.info("  ... and %s more", len(indicators) - 50)
            return 0
        
        # If listing countries, do that and exit
        if args.list_countries:
            countries = downloader.get_countries(args.database)
            logger.info("Found %s countries in %s:", len(countries), args.database)
            for country in countries[:50]:  # Show first 50
                country_code = country.get('code', 'N/A')
                country_name = country.get('name', 'N/A')
                logger.info("  %s - %s", country_code, country_name)
            if len(countries) > 50:
                logger.info("  ... and %s more", len(countries) - 50)
            return 0
        
        # If generate-db is requested, generate the database
        if args.generate_db:
            logger.info("Generating PostgreSQL database with all downloadable IMF indicators from %s...", args.database)
            series_list = downloader.get_all_downloadable_series(
                database_id=args.database
            )
            logger.info("Database generated successfully with %s indicators! Database: imf", len(series_list))
            return 0
        
        # If from-db is specified, download from database
        if args.from_db:
            os.makedirs(output_dir, exist_ok=True)
            result = downloader.download_series_from_db(
                database_id=args.database,
                start_date=args.start_date,
                end_date=args.end_date,
                output_dir=str(output_dir)
            )
            logger.info("Download complete: %s series, %s errors", result.get('downloaded', 0), result.get('errors', 0))
            return 0
        
        # Series download requires --series argument (unless searching)
        if not args.series and not args.search:
            parser.error("--series is required unless --generate-db, --list-databases, --list-indicators, --list-countries, or --search is used")
        
        # Ensure output directory exists
        os.makedirs(output_dir, exist_ok=True)
        
        # Set default end date to today if not provided
        if not args.end_date:
            args.end_date = datetime.now().strftime('%Y-%m-%d')
        
        # Download series
        if args.search:
            logger.info("Searching for series matching '%s' in %s...", args.search, args.database)
            series_list = downloader.search_series(args.database, search_term=args.search)
            logger.info("Found %s matching series", len(series_list))
            if series_list:
                logger.info("First 10 results:")
                for s in series_list[:10]:
                    logger.info("  %s", s)
            return 0
        
        logger.info("Downloading %s IMF series from %s...", len(args.series), args.database)
        logger.info("Series codes: %s", ", ".join(args.series))
        if args.start_date:
            logger.info("Start date: %s", args.start_date)
        if args.end_date:
            logger.info("End date: %s", args.end_date)
        if args.countries:
            logger.info("Countries: %s", ", ".join(args.countries))
        
        df = downloader.download_multiple_series(
            database_id=args.database,
            indicators=args.series,
            start_date=args.start_date,
            end_date=args.end_date,
            countries=args.countries
        )
        
        if df.empty:
            logger.warning("No data was downloaded")
            return 1
        
        parquet_path = os.path.join(str(output_dir), f'imf_{args.database.lower()}_data.parquet')
        df.to_parquet(parquet_path)
        logger.info("Data saved to: %s", parquet_path)
        logger.info("Download complete! Downloaded %s rows across %s series", len(df), len(df.columns))
        
    except Exception as e:
        logger.exception("Error: %s", e)
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())
