"""
Main entry point for FRED Economic Data Downloader

This module provides a command-line interface to download time series data from FRED.
"""

import logging
import os
import sys
from datetime import datetime
from pathlib import Path

logger = logging.getLogger(__name__)

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.config import get_environment, load_environment_config


def main():
    """Main function to download FRED economic data"""
    import argparse
    
    logging.basicConfig(level=logging.INFO, format='%(message)s')
    
    parser = argparse.ArgumentParser(description='Download FRED economic time series data')

    catalog_group = parser.add_argument_group('Options for --generate-catalog',
                                             'These options only apply when using --generate-catalog')
    catalog_group.add_argument('--generate-catalog', action='store_true',
                              help='Generate catalog of all downloadable series (saves to PostgreSQL)')
    catalog_group.add_argument('--use-categories', action='store_true', default=True,
                              help='Use categories to discover series (default: True)')
    catalog_group.add_argument('--full-category-search', action='store_true', default=False,
                              help='Recursively search ALL categories (slow). Default: only known major categories (fast)')
    catalog_group.add_argument('--include-search-terms', action='store_true', default=False,
                              help='Also search using economic terms to find additional series (slower but more comprehensive)')
    catalog_group.add_argument('--category-roots', type=int, nargs='+',
                              help='List of FRED category IDs to use as roots when using known-categories mode')

    download_group = parser.add_argument_group('Options for --download-series',
                                              'These options only apply when using --download-series')
    download_group.add_argument('--download-series', action='store_true',
                               help='Download series from the database')
    download_group.add_argument('--series', type=str, nargs='+',
                               help='Specific series IDs to download (e.g., GDP, UNRATE). Optional: if omitted, download all series from catalog.')
    download_group.add_argument('--series-query-file', type=str,
                               help='Path to SQL file that returns series_id values. Optional: if neither --series nor this is specified, download all.')
    
    args = parser.parse_args()
    
    # Load .env for FRED_API_KEY and POSTGRES_* credentials
    load_environment_config()

    # Limit only for tests (set FRED_LIMIT env var)
    _limit = int(os.environ['FRED_LIMIT']) if os.environ.get('FRED_LIMIT') else None

    try:
        from fred_data_downloader import FREDDataDownloader
        # API key and DB credentials loaded from .env by FREDDataDownloader
        downloader = FREDDataDownloader(api_key=os.getenv('FRED_API_KEY'))
        
        # If generate-catalog is requested, generate the catalog
        if args.generate_catalog:
            logger.info("Generating database with all downloadable FRED series...")
            if args.full_category_search:
                logger.info("  Mode: Full category search (recursive - slower)")
            else:
                logger.info("  Mode: Known categories only (fast)")
            if args.include_search_terms:
                logger.info("  Also searching with economic terms")
            
            storage_root = os.getenv('TRADING_AGENT_STORAGE')
            env = get_environment()
            storage_env = 'test' if env == 'staging' else env
            if storage_root:
                catalog_output_dir = str(
                    Path(storage_root) / storage_env / 'macro' / 'fred' / 'master'
                )
            else:
                catalog_output_dir = str(
                    _project_root / 'storage' / storage_env / 'macro' / 'fred' / 'master'
                )
            series_list = downloader.get_all_downloadable_series(
                limit=_limit,
                use_categories=args.use_categories,
                use_known_categories_only=not args.full_category_search,
                use_search_terms=args.include_search_terms,
                category_roots=args.category_roots,
                catalog_output_dir=catalog_output_dir
            )
            logger.info("Database generated successfully with %d series!", len(series_list))
            logger.info("Use --download-series to download these series")
            return 0
        
        # If download-series is specified, download from database
        if args.download_series:
            series_ids = None
            if args.series:
                series_ids = list(args.series)
                preview = series_ids[:10] if len(series_ids) <= 10 else series_ids[:10] + ['...']
                logger.info("Downloading %d specified series: %s", len(series_ids), preview)
            elif args.series_query_file:
                query_file = args.series_query_file
                if not os.path.isabs(query_file):
                    # If relative path, make it relative to the script directory or workspace root
                    script_dir = os.path.dirname(os.path.abspath(__file__))
                    query_file = os.path.join(script_dir, query_file)
                
                if not os.path.exists(query_file):
                    logger.error("Series query file not found: %s", query_file)
                    return 1
                
                logger.info("Reading series query from: %s", query_file)
                with open(query_file, 'r', encoding='utf-8') as f:
                    query = f.read().strip()
                
                # Remove SQL comments and empty lines for cleaner output
                query_lines = [line for line in query.split('\n') if line.strip() and not line.strip().startswith('--')]
                query_preview = ' '.join(query_lines[:3])
                if len(query_lines) > 3:
                    query_preview += '...'
                logger.info("  Query: %s", query_preview)
                
                from fred_postgres import get_postgres_connection
                from psycopg2.extras import RealDictCursor
                
                conn = get_postgres_connection()
                cur = conn.cursor(cursor_factory=RealDictCursor)
                cur.execute(query)
                results = cur.fetchall()
                cur.close()
                conn.close()
                
                # Extract series IDs from results
                if results:
                    series_ids = []
                    for row in results:
                        if isinstance(row, dict):
                            # Try common column names for series_id
                            series_id = (row.get('series_id') or 
                                       row.get('series_id::text') or 
                                       row.get('series') or
                                       list(row.values())[0])
                        elif isinstance(row, (tuple, list)):
                            series_id = row[0]
                        else:
                            series_id = row
                        
                        if series_id:
                            series_ids.append(str(series_id))
                    
                    if series_ids:
                        preview = series_ids[:10] if len(series_ids) <= 10 else series_ids[:10] + ['...']
                        logger.info("  Found %d series from query: %s", len(series_ids), preview)
                    else:
                        logger.warning("No valid series IDs found in query results")
                        series_ids = None
                else:
                    logger.warning("Series query returned no results")
                    series_ids = None
            else:
                # Neither --series nor --series-query-file: load all from catalog
                logger.info("Loading all series from catalog...")
                series_ids = downloader.load_series_from_db()
                if not series_ids:
                    logger.error("No series in catalog. Run --generate-catalog first.")
                    return 1
                logger.info("  Found %d series to download", len(series_ids))
            
            _env = get_environment()
            _log_dir = _project_root / "storage" / _env / "macro" / "fred"
            run_log_path = _log_dir / ("fred-download-%s.log" % datetime.now().strftime("%Y-%m-%dT%H%M%S"))
            command_line = " ".join(sys.argv)
            downloader.download_series_from_db(
                series_ids=series_ids,
                run_log_path=str(run_log_path),
                command_line=command_line,
            )
            logger.info("Download complete!")
            return 0
        
        # If specific series are requested, download those
        if args.series:
            logger.info("Downloading %d specified series...", len(args.series))
            for series_id in args.series:
                logger.info("Downloading %s...", series_id)
                data = downloader.download_series(series_id, save_to_db=True)
                
                if data is not None and not data.empty:
                    logger.info("  Saved %d data points to database for %s", len(data), series_id)
                else:
                    logger.warning("  Failed to download %s", series_id)
        else:
            # Download all available series
            downloader.download_all_available_series(limit=_limit)
        
        logger.info("Download complete!")
    except Exception as e:
        logger.exception("Error: %s", e)
        return 1
    
    return 0


if __name__ == "__main__":
    main()
