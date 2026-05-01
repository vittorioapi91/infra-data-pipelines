"""
Main entry point for Eurostat Data Downloader

This module provides a command-line interface to download time series data from Eurostat.
"""

import logging
import os
import sys
from pathlib import Path

logger = logging.getLogger(__name__)

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.config import load_environment_config


def main():
    """Main function to download Eurostat data"""
    import argparse
    
    logging.basicConfig(level=logging.INFO, format='%(message)s')
    
    parser = argparse.ArgumentParser(description='Download Eurostat economic time series data')
    
    # Limit only for tests (set EUROSTAT_LIMIT env var)
    _limit = int(os.environ['EUROSTAT_LIMIT']) if os.environ.get('EUROSTAT_LIMIT') else None
    
    parser.add_argument('--generate-catalog', action='store_true',
                       help='Generate catalog of all downloadable datasets (saves to PostgreSQL)')
    parser.add_argument('--download-series', action='store_true',
                       help='Download datasets from the database')
    parser.add_argument('--series', type=str,
                       help='Optional: specific dataset code (e.g., tps00001). If omitted with --download-series, download all.')
    
    args = parser.parse_args()
    
    load_environment_config()
    
    try:
        from eurostat_data_downloader import EurostatDataDownloader
        downloader = EurostatDataDownloader()
        
        # If generate-catalog is requested, generate the catalog
        if args.generate_catalog:
            logger.info("Generating database with all downloadable Eurostat datasets...")
            datasets_list = downloader.get_all_downloadable_series(
                limit=_limit
            )
            logger.info("Database generated successfully with %d datasets!", len(datasets_list))
            logger.info("Use --download-series to download these datasets")
            return 0
        
        # If download-series or specific series requested, download
        if args.download_series or args.series:
            dataset_codes = [args.series] if args.series else None
            if dataset_codes:
                logger.info("Downloading dataset: %s", args.series)
            else:
                logger.info("Downloading all datasets from catalog...")
            downloader.download_datasets_from_db(dataset_codes=dataset_codes)
            logger.info("Download complete!")
            return 0
        
        # If no action specified, show help
        parser.print_help()
        return 0
        
    except Exception as e:
        logger.exception("Error: %s", e)
        return 1


if __name__ == "__main__":
    main()
