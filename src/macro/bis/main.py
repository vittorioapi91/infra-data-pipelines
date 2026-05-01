"""CLI for BIS Data Downloader"""

import argparse
import logging
import os
import traceback as tb

# Handle relative import for both module and script usage
try:
    from .bis_data_downloader import BISDataDownloader
except ImportError:
    from src.macro.bis.bis_data_downloader import BISDataDownloader

logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(description="BIS Data Downloader (SDMX)")
    parser.add_argument('--list-dataflows', action='store_true', help='List available BIS dataflows')
    parser.add_argument('--generate-db', action='store_true', help='Generate PostgreSQL database with all BIS dataflows')
    parser.add_argument('--from-db', action='store_true', help='Download datasets from the database')
    parser.add_argument('--dataflow', type=str, help='Dataflow ID to download')
    parser.add_argument('--key', type=str, default='', help='SDMX key (dimension filters). Default: ALL')
    parser.add_argument('--start-period', type=str, help='Optional startPeriod parameter')
    parser.add_argument('--end-period', type=str, help='Optional endPeriod parameter')
    parser.add_argument('--user-agent', type=str, default='VittorioApicella apicellavittorio@hotmail.it', help='User-Agent for BIS requests')
    
    # Database connection parameters
    parser.add_argument('--dbuser', type=str, default='tradingAgent',
                       help='PostgreSQL user (default: tradingAgent)')
    parser.add_argument('--dbhost', type=str, default='localhost',
                       help='PostgreSQL host (default: localhost)')
    parser.add_argument('--dbport', type=int, default=5432,
                       help='PostgreSQL port (default: 5432)')
    parser.add_argument('--dbpassword', type=str, default=None,
                       help='PostgreSQL password (or set POSTGRES_PASSWORD env var)')

    args = parser.parse_args()

    try:
        # Get password from args or environment
        password = args.dbpassword or os.getenv('POSTGRES_PASSWORD', '')
        
        # Database name is fixed to 'bis' in BISDataDownloader.__init__
        downloader = BISDataDownloader(
            user_agent=args.user_agent,
            user=args.dbuser,
            host=args.dbhost,
            password=password,
            port=args.dbport
        )

        if args.list_dataflows:
            flows = downloader.list_dataflows()
            logger.info("Found %s dataflows:", len(flows))
            for f in flows[:20]:
                logger.info("  %s: %s", f.get('dataflow_id'), f.get('name'))
            if len(flows) > 20:
                logger.info("  ... (truncated)")
            return 0

        if args.generate_db:
            logger.info("Generating PostgreSQL database with all BIS dataflows...")
            downloader.generate_db()
            logger.info("Database generated for BIS dataflows. Database: bis. Use --from-db to download datasets using this database")
            return 0

        if args.from_db and args.dataflow:
            params = {}
            if args.start_period:
                params['startPeriod'] = args.start_period
            if args.end_period:
                params['endPeriod'] = args.end_period

            data = downloader.download_from_db(
                dataflow_id=args.dataflow,
                key=args.key,
                params=params,
                save_to_db=True
            )
            if data:
                logger.info("Download complete: %s. Data saved to PostgreSQL database", args.dataflow)
            return 0

        logger.info("Nothing to do. Use --list-dataflows, --generate-db, or --from-db with --dataflow.")
        return 0

    except Exception as e:
        logger.exception("Error: %s", e)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
