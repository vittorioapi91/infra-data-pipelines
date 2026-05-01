"""
SEC EDGAR Filings Downloader

This module downloads all company filings from SEC EDGAR database.
Focuses on XBRL filings from 2009 onwards.
"""

import logging
from typing import Optional

logger = logging.getLogger(__name__)

# Load environment configuration early (only if not in Airflow)
# In Airflow, trading_agent_dags.py already loads env vars before importing DAGs
import os
from pathlib import Path
_is_airflow = os.getenv('AIRFLOW_HOME') is not None or '/opt/airflow' in str(Path(__file__).absolute())
_edgar_project_root = Path(__file__).resolve().parent.parent.parent.parent
if not _is_airflow:
    try:
        from ...config import load_environment_config
        load_environment_config()
    except (ImportError, ValueError):
        # Handle both relative import errors and when run as a script
        import sys
        # Add project root to path for absolute import
        # File is at: src/fundamentals/edgar/edgar.py
        # Need to go up 4 levels to get to project root (edgar -> fundamentals -> src -> root)
        project_root = _edgar_project_root
        if str(project_root) not in sys.path:
            sys.path.insert(0, str(project_root))
        from src.config import load_environment_config
        load_environment_config()
        # Fallback: load .env.dev if POSTGRES vars not set (config import failed or skipped)
        if not os.getenv('POSTGRES_PASSWORD'):
            from dotenv import load_dotenv
            env_file = project_root / '.env.dev'
            if env_file.exists():
                load_dotenv(env_file, override=True)
                logger.info("Loaded environment from: %s", env_file.name)


import os
import time
import shutil
import requests
import asyncio
import aiohttp
from typing import List, Dict, Optional, Set
from datetime import datetime
import json
import tempfile
import uuid
import warnings
import io
import zipfile
import xml.etree.ElementTree as ET
from collections import defaultdict
import re
from urllib.parse import urljoin, urlparse
import logging
from html.parser import HTMLParser
from concurrent.futures import ThreadPoolExecutor, as_completed
from multiprocessing import Pool, cpu_count
from threading import Lock
import pandas as pd
from pathlib import Path
import psycopg2

from tqdm import tqdm

# Handle imports for both module import and direct script execution
try:
    from .edgar_postgres import (
        get_postgres_connection, init_edgar_postgres_tables
    )
    from .form_type_path import form_type_filesystem_slug
    from .quarter_filings_zip_path import quarter_filings_zip_path
except ImportError:
    # Handle direct script execution - use absolute imports
    import sys
    # Add project root to path if not already there (edgar -> fundamentals -> src -> root)
    file_path = Path(__file__).resolve()
    project_root = file_path.parent.parent.parent.parent
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))
    from src.fundamentals.edgar.edgar_postgres import (
        get_postgres_connection, init_edgar_postgres_tables
    )
    from src.fundamentals.edgar.form_type_path import form_type_filesystem_slug
    from src.fundamentals.edgar.quarter_filings_zip_path import quarter_filings_zip_path

warnings.filterwarnings('ignore')


class EDGARDownloader:
    """Class to download SEC EDGAR filings"""
    
    def __init__(self, user_agent: str = "VittorioApicella apicellavittorio@hotmail.it"):
        """
        Initialize EDGAR downloader
        
        Args:
            user_agent: User-Agent string for SEC EDGAR requests (required by SEC)
        """
        self.user_agent = user_agent
        self.base_url = "https://www.sec.gov"
        self.data_base_url = "https://data.sec.gov"
        self.headers = {
            'User-Agent': user_agent,
            'Accept-Encoding': 'gzip, deflate',
            'Host': 'www.sec.gov'
        }
        self.data_headers = {
            'User-Agent': user_agent,
            'Accept-Encoding': 'gzip, deflate',
            'Host': 'data.sec.gov'
        }


def get_edgar_arelle_temp_dir() -> Optional[Path]:
    """
    Directory for short-lived Arelle entrypoint files (e.g. a zip member copied from RAM).

    Set ``EDGAR_ARELLE_TEMP_DIR`` or ``RAMDISK`` to a folder on a RAM disk or tmpfs
    so iXBRL scraping does not write large temporaries to SSD.

    Returns:
        Resolved path if the env var is set and the directory exists and is writable; else None.
    """
    raw = os.environ.get("EDGAR_ARELLE_TEMP_DIR") or os.environ.get("RAMDISK")
    if raw and str(raw).strip():
        candidates = [Path(str(raw).strip()).expanduser()]
    else:
        # Auto-detect common RAM disk mount points when env vars are not set.
        candidates = [
            Path("/Volumes/RAMDisk")
        ]

    p: Optional[Path] = None
    for candidate in candidates:
        resolved = candidate.resolve()
        if resolved.is_dir():
            p = resolved
            break

    if p is None:
        return None

    if not p.is_dir():
        logger.warning("EDGAR_ARELLE_TEMP_DIR is not a directory: %s", p)
        return None
    try:
        with tempfile.NamedTemporaryFile(dir=p, delete=True):
            pass
    except OSError as e:
        logger.warning("EDGAR_ARELLE_TEMP_DIR is not writable: %s (%s)", p, e)
        return None
    return p


def _scrape_one_filing(args):
    """Worker for multiprocessing: scrape one .txt filing, write adj output.

    Returns a 6-tuple: (ok_xbrl, ok_html, ok_adj, err, failure_entry or None, skipped_ixbrl).
    ``skipped_ixbrl`` is always 0 for this worker (reserved for zip scrape parity).

    Optional 10th element ``accession_override``: use when ``fp`` is a temp file whose stem
    is not the SEC accession (e.g. zip+iXBRL entrypoint on a RAM disk).
    """
    import sys
    _root = Path(__file__).resolve().parent.parent.parent.parent
    if str(_root) not in sys.path:
        sys.path.insert(0, str(_root))
    accession_override: Optional[str] = None
    if len(args) >= 10:
        accession_override = args[9]
    if len(args) >= 9:
        (
            fp_str,
            form_type,
            input_path_str,
            exchange_lookup,
            year_quarter,
            company_name_lookup,
            debug,
            scrape_errors_log_str,
            check_warnings_log_str,
        ) = args[:9]
    elif len(args) >= 7:
        fp_str, form_type, input_path_str, exchange_lookup, year_quarter, company_name_lookup, debug = args[:7]
        scrape_errors_log_str = None
        check_warnings_log_str = None
    elif len(args) >= 6:
        fp_str, form_type, input_path_str, exchange_lookup, year_quarter, company_name_lookup = args[:6]
        debug = False
        scrape_errors_log_str = None
        check_warnings_log_str = None
    elif len(args) >= 5:
        fp_str, form_type, input_path_str, exchange_lookup, year_quarter = args[:5]
        company_name_lookup = {}
        debug = False
        scrape_errors_log_str = None
        check_warnings_log_str = None
    else:
        fp_str, form_type, input_path_str = args[0], args[1], args[2]
        exchange_lookup = {}
        year_quarter = ""
        company_name_lookup = {}
        debug = False
    fp = Path(fp_str)
    input_path = Path(input_path_str)
    ok_xbrl, ok_html, ok_adj, err = 0, 0, 0, 0
    failure_entry = None
    accession = accession_override if accession_override is not None else fp.stem
    # Keep intermediates next to final outputs (storage), not next to fp — fp may be on a RAM disk temp.
    out_xbrl = input_path / f"{accession}.xbrl.json"
    out_html = input_path / f"{accession}.html.json"
    out_adj = input_path / f"{accession}.json"

    try:
        from src.fundamentals.edgar.filings.filings_scraper import (
            FilingsScraperInlineXBRL,
            FilingsScraperHTML,
            FilingScraperDispatcher,
        )
        from src.fundamentals.edgar.filings.filings_scraper_xbrl_adj import (
            xbrl_to_adj, extract_dei_ticker_exchange, extract_dei_from_txt_fallback,
            _is_etf, has_no_trading_symbol_flag,
        )
        from src.fundamentals.edgar.accounting_checks import run_all_checks
    except ImportError:
        from .filings.filings_scraper import (
            FilingsScraperInlineXBRL,
            FilingsScraperHTML,
            FilingScraperDispatcher,
        )
        from .filings.filings_scraper_xbrl_adj import (
            xbrl_to_adj, extract_dei_ticker_exchange, extract_dei_from_txt_fallback,
            _is_etf, has_no_trading_symbol_flag,
        )
        from .accounting_checks import run_all_checks

    scraper_xbrl = FilingsScraperInlineXBRL()
    scraper_html = FilingsScraperHTML()
    classifier = FilingScraperDispatcher()

    # Log files live next to the form-type directory; when called from the
    # CLI scrape pipeline we pass explicit, suffix-based paths so that logs
    # for different year/quarter/form_type combinations do not overwrite.
    if scrape_errors_log_str is not None:
        scrape_errors_log = Path(scrape_errors_log_str)
    else:
        scrape_errors_log = input_path.parent / "scrape_errors.log"
    if check_warnings_log_str is not None:
        check_warnings_log = Path(check_warnings_log_str)
    else:
        check_warnings_log = input_path.parent / "check_warnings.log"
    def _log_scrape_err(stage: str, exc: BaseException) -> None:
        try:
            msg = str(exc).replace("\n", " ")[:500]
            with open(scrape_errors_log, "a", encoding="utf-8") as f:
                f.write(f"{accession} [{stage}]: {msg}\n")
        except OSError:
            pass

    # Use the dispatcher to decide whether this filing is inline XBRL or a legacy
    # text / HTML filing. Legacy filings are scraped via the legacy path and
    # written as a simpler JSON, while inline XBRL filings go through the full
    # XBRL → adj pipeline.
    kind, scraper_for_kind = classifier.create_scraper(fp)
    if kind != "ixbrl":
        try:
            legacy_data = scraper_for_kind.scrape_filing(fp, form_type=form_type)
            with open(out_adj, "w", encoding="utf-8") as f:
                json.dump(legacy_data, f, indent=2, ensure_ascii=False)
                f.flush()
                os.fsync(f.fileno())
            ok_adj = 1
            # Run legacy accounting checks and build failure_entry for logging
            results = run_all_checks(legacy_data, form_type=form_type)
            total_checks = len(results)
            failed = [(name, (detail or {}).get("DifferencePct")) for name, passed, _msg, detail in results if not passed]
            if failed:
                ticker_legacy = None
                exchange_legacy = exchange_lookup.get(accession) if exchange_lookup else None
                failure_entry = (accession, failed, len(failed), total_checks, ticker_legacy, exchange_legacy or "NONE")
        except Exception as e:
            err = 1
            _log_scrape_err(kind, e)
        return (ok_xbrl, ok_html, ok_adj, err, failure_entry, 0)

    try:
        if out_xbrl.exists():
            xbrl_data = json.loads(out_xbrl.read_text(encoding="utf-8"))
        else:
            xbrl_data = scraper_xbrl.scrape_filing(fp, form_type=form_type)
            with open(out_xbrl, "w", encoding="utf-8") as f:
                json.dump(xbrl_data, f, indent=2)
                f.flush()
                os.fsync(f.fileno())
        ok_xbrl = 1
    except Exception as e:
        err = 1
        _log_scrape_err("xbrl", e)
        return (ok_xbrl, ok_html, ok_adj, err, failure_entry, 0)

    adj_data = None
    ticker, exchange = None, None
    try:
        ticker, exchange = extract_dei_ticker_exchange(xbrl_data)
        no_ticker_flag = has_no_trading_symbol_flag(xbrl_data)
        if (ticker is None or exchange is None) and not no_ticker_flag:
            txt_ticker, txt_exchange = extract_dei_from_txt_fallback(fp)
            ticker = ticker or txt_ticker
            exchange = exchange or txt_exchange
        if ticker is None and no_ticker_flag:
            exchange = exchange or exchange_lookup.get(accession) or "NONE"
            adj_data = {"ticker": "dei:NoTradingSymbolFlag", "exchange": exchange}
            with open(out_adj, "w", encoding="utf-8") as f:
                json.dump(adj_data, f, indent=2, ensure_ascii=False)
                f.flush()
                os.fsync(f.fileno())
            ok_adj = 1
            try:
                with open(check_warnings_log, "a", encoding="utf-8") as wf:
                    wf.write(f"{accession}: dei:NoTradingSymbolFlag (no ticker)\n")
            except OSError:
                pass
            to_delete = [out_xbrl, out_html] if not debug else [out_html]
            for intermediate in to_delete:
                if intermediate.exists():
                    try:
                        intermediate.unlink()
                    except OSError:
                        pass
            return (ok_xbrl, ok_html, ok_adj, err, None, 0)
        else:
            if exchange is None:
                exchange = exchange_lookup.get(accession)
            if exchange is None:
                exchange = "NONE"
            if ticker and _is_etf(ticker):
                etf_to_delete = [out_adj, out_html] if debug else [out_xbrl, out_adj, out_html]
                for f in etf_to_delete:
                    if f.exists():
                        try:
                            f.unlink()
                        except OSError:
                            pass
                return (ok_xbrl, 0, 0, 0, None, 0)
            adj_data = xbrl_to_adj(xbrl_data, ticker=ticker, exchange=exchange, accession=accession)
            with open(out_adj, "w", encoding="utf-8") as f:
                json.dump(adj_data, f, indent=2, ensure_ascii=False)
                f.flush()
                os.fsync(f.fileno())
            ok_adj = 1
    except Exception as e:
        err = 1
        _log_scrape_err("adj", e)

    if form_type != "10-Q":
        try:
            html_data = scraper_html.scrape_filing(fp)
            with open(out_html, "w", encoding="utf-8") as f:
                json.dump(html_data, f, indent=2)
                f.flush()
                os.fsync(f.fileno())
            ok_html = 1
        except Exception as e:
            err += 1
            _log_scrape_err("html", e)

    if adj_data is not None:
        period_keys = [k for k in adj_data if k not in ("ticker", "exchange")]
        # Minimal adj (dei:NoTradingSymbolFlag) has no period data; don't count as failure
        if not period_keys and set(adj_data.keys()) <= {"ticker", "exchange"}:
            pass
        elif not period_keys or not adj_data:
            failed = [("(empty adj - no XBRL data)", None)]
            failure_entry = (accession, failed, 1, 14, ticker, exchange)
        else:
            results = run_all_checks(adj_data, form_type=form_type)
            total_checks = len(results)
            failed = [(name, (detail or {}).get("DifferencePct")) for name, passed, msg, detail in results if not passed]
            if failed:
                failure_entry = (accession, failed, len(failed), total_checks, ticker, exchange)

    # Delete intermediates on success (keep .xbrl.json when debug)
    if failure_entry is None:
        to_delete = [out_xbrl, out_html] if not debug else [out_html]
        for intermediate in to_delete:
            if intermediate.exists():
                try:
                    intermediate.unlink()
                except OSError:
                    pass

    return (ok_xbrl, ok_html, ok_adj, err, failure_entry, 0)


def _scrape_zip_member(args):
    """
    Worker: read one ``.txt`` member from the quarter zip, copy it to ``RAMDISK`` /
    ``EDGAR_ARELLE_TEMP_DIR``, then run :func:`_scrape_one_filing` — same code path as
    scraping an on-disk ``.txt`` under ``input_path`` (no separate legacy/HTML/ixbrl branch).

    Returns (ok_xbrl, ok_html, ok_adj, err, failure_entry, skipped_ixbrl). The last field is
    always ``0``; if the temp dir env is unset, returns ``err=1``.
    """
    (
        zip_path_str,
        member_name,
        output_dir_str,
        form_type,
        exchange_lookup,
        year_quarter,
        company_name_lookup,
        debug,
        scrape_errors_log_str,
        check_warnings_log_str,
    ) = args
    zip_path = Path(zip_path_str)
    output_path = Path(output_dir_str)
    accession = Path(member_name).stem
    scrape_errors_log = Path(scrape_errors_log_str)

    def _log_scrape_err(stage: str, exc: BaseException) -> None:
        try:
            msg = str(exc).replace("\n", " ")[:500]
            with open(scrape_errors_log, "a", encoding="utf-8") as f:
                f.write(f"{accession} [{stage}]: {msg}\n")
        except OSError:
            pass

    def _log_no_ramdisk() -> None:
        try:
            with open(scrape_errors_log, "a", encoding="utf-8") as f:
                f.write(
                    f"{accession} [zip_scrape]: set EDGAR_ARELLE_TEMP_DIR or RAMDISK to copy zip members "
                    f"before scrape (same pipeline as on-disk .txt).\n"
                )
        except OSError:
            pass

    try:
        with zipfile.ZipFile(zip_path, "r") as zf:
            with zf.open(member_name, "r") as raw_in:
                raw_bytes = raw_in.read()
    except (OSError, zipfile.BadZipFile, KeyError) as e:
        _log_scrape_err("zip_read", e)
        return (0, 0, 0, 1, None, 0)

    temp_root = get_edgar_arelle_temp_dir()
    if temp_root is None:
        _log_no_ramdisk()
        return (0, 0, 0, 1, None, 0)

    temp_path = temp_root / f"{accession}_{uuid.uuid4().hex}.txt"
    try:
        temp_path.write_bytes(raw_bytes)
        return _scrape_one_filing(
            (
                str(temp_path),
                form_type,
                str(output_path),
                exchange_lookup,
                year_quarter,
                company_name_lookup,
                debug,
                scrape_errors_log_str,
                check_warnings_log_str,
                accession,
            )
        )
    except Exception as e:
        _log_scrape_err("zip_scrape", e)
        return (0, 0, 0, 1, None, 0)
    finally:
        try:
            temp_path.unlink(missing_ok=True)
        except OSError as uerr:
            logger.warning("Could not remove zip-scrape temp %s: %s", temp_path, uerr)


def main():
    """Main function to download EDGAR filings"""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Download SEC EDGAR filings',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Generate catalog with companies and filings:
  python -m src.fundamentals.edgar --generate-catalog --download-companies
  
  # Generate catalog using existing companies (no company download):
  python -m src.fundamentals.edgar --generate-catalog
  
  # Download raw filings from database with filters (output auto-resolves to storage):
  python -m src.fundamentals.edgar --download-raw-quarter-filings --year 2005 --quarter QTR2 --form-type 10-K
        """
    )
    # Note: output directory is derived from TRADING_AGENT_STORAGE / ENV; no --output-dir flag.
    
    # Filing download filter arguments
    download_group = parser.add_argument_group('Filing Download Filters',
                                              'Filter options for downloading filings from database')
    download_group.add_argument('--year', type=int, default=None,
                                help='Year filter for filings (e.g., 2005)')
    download_group.add_argument('--quarter', type=str, default=None,
                               help='Quarter filter for filings (e.g., QTR1, QTR2, QTR3, QTR4)')
    download_group.add_argument('--form-type', type=str, default=None,
                               help='Form type filter for filings (e.g., 10-K, 10-Q)')
    download_group.add_argument('--cik', type=str, default=None,
                               help='CIK (Central Index Key) filter for filings')
    download_group.add_argument('--company-name', type=str, default=None,
                               help='Company name filter (partial match, case-insensitive)')
    
    # Catalog generation arguments
    catalog_group = parser.add_argument_group('Catalog Generation',
                                             'Options for --generate-catalog mode')
    catalog_group.add_argument('--generate-catalog', action='store_true',
                              help='Generate catalog of companies and filings, save to PostgreSQL database. '
                                   'This mode processes filings from SEC EDGAR and stores metadata in the database.')
    catalog_group.add_argument('--download-companies', action='store_true',
                              help='[REQUIRED for first run] Fetch and enrich companies from EDGAR when generating catalog. '
                                   'If not specified, only existing companies from the database are used. '
                                   'Company details (ticker, SIC code, entity type) are fetched and updated. '
                                   'New companies are added, and existing companies are updated if details change.')
    
    # Other modes
    parser.add_argument('--download-raw-quarter-filings', action='store_true',
                       help='Download raw filing files from companies in PostgreSQL database. '
                            'Use this with filter arguments (--year, --quarter, --form-type, etc.) to download specific filings.')
    parser.add_argument('--scrape-filings', action='store_true',
                       help='Scrape XBRL and HTML from existing filing .txt files in storage. '
                            'Requires --year, --quarter, and --form-type to build the storage path. '
                            'Writes .xbrl.json and .html.json alongside each .txt.')
    parser.add_argument(
        '--filings-zip',
        action='store_true',
        help='With --scrape-filings: read members from the quarter archive zip next to filings/ '
             '(same path as --archive-to-zip: {year}-{quarter}-{form_type_fs}.zip). '
             'Each member is copied to RAMDISK / EDGAR_ARELLE_TEMP_DIR, then scraped like an on-disk .txt. '
             'Requires --year, --quarter, --form-type.',
    )
    parser.add_argument('--debug', action='store_true',
                       help='Keep .xbrl.json intermediate files after scraping (otherwise they are removed).')
    parser.add_argument('--upload-scraped-filings', action='store_true',
                       help='Upload adj .json files to filings_facts table. '
                            'Requires --year, --quarter, and --form-type to build the storage path.')
    parser.add_argument(
        '--force-all',
        action='store_true',
        help='When used with --download-raw-quarter-filings, force re-download of all .txt filings '
             'even if they already exist in the target folder. By default, existing .txt files are skipped.',
    )
    parser.add_argument(
        '--archive-to-zip',
        action='store_true',
        help='When used with --download-raw-quarter-filings (year/quarter/form, no --cik / --company-name), '
             'after a successful full catalog download, create the quarter zip and remove the form-type folder.',
    )
    parser.add_argument(
        '--extract-filings-zip',
        action='store_true',
        help='Extract {year}-{quarter}-{form_type_fs}.zip from the filings folder into '
             'filings/<year>/<quarter>/<form_type_fs>/*.txt (reverse of --archive-to-zip). '
             'Requires --year, --quarter, and --form-type (one archive per combination).',
    )
    args = parser.parse_args()
    
    try:
        user_agent = os.getenv('EDGAR_USER_AGENT', 'VittorioApicella apicellavittorio@hotmail.it')
        downloader = EDGARDownloader(user_agent=user_agent)
        # Resolve base storage dir once (TRADING_AGENT_STORAGE common root + ENV)
        project_root = Path(__file__).resolve().parent.parent.parent.parent
        storage_root = os.getenv("TRADING_AGENT_STORAGE")
        storage_env = os.getenv("ENV", "dev")
        if storage_root:
            storage_base = Path(storage_root) / storage_env
        else:
            storage_base = project_root / "storage" / storage_env

        # Optional download limit for tests only: controlled via EDGAR_DOWNLOAD_LIMIT when ENV=test.
        download_limit: Optional[int] = None
        if storage_env == "test":
            limit_str = os.getenv("EDGAR_DOWNLOAD_LIMIT")
            if limit_str:
                try:
                    download_limit = int(limit_str)
                except ValueError:
                    logger.warning("Ignoring invalid EDGAR_DOWNLOAD_LIMIT=%r (must be integer)", limit_str)

        if args.extract_filings_zip:
            if not (args.year and args.quarter and args.form_type):
                logger.error("--extract-filings-zip requires --year, --quarter, and --form-type")
                return 1
            filings_root = storage_base / "fundamentals" / "edgar" / "filings"
            try:
                from .filings.filings_quarter_archive import extract_quarter_filings_zip
            except ImportError:
                from src.fundamentals.edgar.filings.filings_quarter_archive import extract_quarter_filings_zip
            n_archives = extract_quarter_filings_zip(
                filings_root,
                str(args.year),
                str(args.quarter),
                args.form_type,
            )
            logger.info(
                "Extract-filings-zip finished: %s archive(s) processed under %s",
                n_archives,
                filings_root,
            )
            return 0
        
        # If generate-catalog is requested, generate the catalog in PostgreSQL
        if args.generate_catalog:
            logger.info("Generating catalog with all companies and filings...")
            
            # Download and save master.idx files
            try:
                # Import MasterIdxManager
                try:
                    from .master_idx.master_idx import MasterIdxManager
                except ImportError:
                    from src.fundamentals.edgar.master_idx.master_idx import MasterIdxManager
                
                conn = get_postgres_connection()
                # Initialize tables including ledger
                init_edgar_postgres_tables(conn)
                
                # Use MasterIdxManager for master.idx operations
                master_idx_manager = MasterIdxManager(user_agent=user_agent)
                # Download only new/failed quarters
                master_idx_manager.save_master_idx_to_disk(conn)
                # Save parsed master.idx CSV files to database
                master_idx_manager.save_master_idx_to_db(conn)
                conn.close()
            except Exception as e:
                logger.error("Failed to process master.idx files: %s", e)
                raise
        
        # Download raw filings from database (only if --download-raw-quarter-filings is specified)
        if args.download_raw_quarter_filings:
            # Import FilingDownloader
            try:
                from .filings.filings_downloader import FilingDownloader
            except ImportError:
                from src.fundamentals.edgar.filings.filings_downloader import FilingDownloader
            
            logger.info("Downloading filings from database...")
            filing_downloader = FilingDownloader(user_agent=user_agent)
            
            # Build filter dictionary
            filters = {}
            if args.year is not None:
                filters['year'] = args.year
            if args.quarter is not None:
                filters['quarter'] = args.quarter
            if args.form_type is not None:
                filters['form_type'] = args.form_type
            if args.cik is not None:
                filters['cik'] = args.cik
            if args.company_name is not None:
                filters['company_name'] = args.company_name
            
            # Download filings
            # Default output directory under storage:
            # storage/{ENV}/fundamentals/edgar/filings/{year}/{quarter}/{form_type_fs}
            # (falls back to just "filings" if filters omitted)
            out_dir = storage_base / "fundamentals" / "edgar" / "filings"
            if args.year:
                out_dir = out_dir / str(args.year)
            if args.quarter:
                out_dir = out_dir / str(args.quarter)
            if args.form_type:
                out_dir = out_dir / form_type_filesystem_slug(args.form_type)
            downloaded_files = filing_downloader.download_filings(
                output_dir=str(out_dir),
                limit=download_limit,
                force_all=args.force_all,
                archive_to_zip=args.archive_to_zip,
                **filters
            )
            
            logger.info("Successfully downloaded %s filing(s)", len(downloaded_files))
            return 0

        # Scrape filings from existing .txt files (--scrape-filings)
        if args.scrape_filings:
            if not (args.year and args.quarter and args.form_type):
                logger.error("--scrape-filings requires --year, --quarter, and --form-type")
                return 1
            input_path = (
                storage_base
                / "fundamentals"
                / "edgar"
                / "filings"
                / str(args.year)
                / args.quarter
                / form_type_filesystem_slug(args.form_type)
            )
            use_zip = bool(args.filings_zip)
            zip_path: Optional[Path] = None
            txt_members: List[str] = []
            if use_zip:
                zip_path = quarter_filings_zip_path(input_path)
                if zip_path is None:
                    logger.error(
                        "Cannot resolve quarter zip from storage path (expected .../filings/<year>/<QTRn>/<form_type>): %s",
                        input_path,
                    )
                    return 1
                zip_path = zip_path.resolve()
                if not zip_path.is_file():
                    logger.error("Quarter zip not found (run download with --archive-to-zip or create): %s", zip_path)
                    return 1
                try:
                    with zipfile.ZipFile(zip_path, "r") as zf:
                        txt_members = sorted(
                            n
                            for n in zf.namelist()
                            if n.endswith(".txt") and not n.startswith("__MACOSX/")
                        )
                except zipfile.BadZipFile as e:
                    logger.error("Invalid or unreadable zip: %s: %s", zip_path, e)
                    return 1
                if not txt_members:
                    logger.warning("No .txt members in zip: %s", zip_path)
                    return 0
                input_path.mkdir(parents=True, exist_ok=True)
            else:
                if not input_path.exists():
                    logger.error("Input directory not found: %s", input_path)
                    return 1
            # Remove any existing .json before scraping (fresh run)
            removed = 0
            for jf in input_path.glob("*.json"):
                try:
                    jf.unlink()
                    removed += 1
                except OSError:
                    pass
            if removed:
                logger.info("Removed %s existing .json file(s) from %s", removed, input_path)
            if use_zip:
                total = len(txt_members)
            else:
                txt_files = list(input_path.glob("*.txt"))
                if not txt_files:
                    logger.warning("No .txt filings found in %s", input_path)
                    return 0
                total = len(txt_files)
            # SEC form for scrapers/checks (not the filesystem slug in input_path.name)
            form_type = args.form_type
            # Build per-run log file names that encode year, quarter, and form type
            # so that multiple runs do not overwrite each other's logs.
            suffix = f"{args.year}_{args.quarter}_{form_type_filesystem_slug(form_type)}"
            check_failures_log = input_path.parent / f"check_failures_{suffix}.log"
            scrape_errors_log = input_path.parent / f"scrape_errors_{suffix}.log"
            check_warnings_log = input_path.parent / f"check_warnings_{suffix}.log"
            n_workers = max(1, cpu_count() - 1)
            # Pre-load exchange lookup from master_idx + companies (required; no fallback)
            try:
                from .edgar_postgres import get_postgres_connection as _get_conn, init_edgar_postgres_tables as _init_edgar
                from .filings.filings_postgres import get_exchange_for_accessions, get_company_name_for_accessions
                from .filings.filings_scraper_xbrl_adj import _get_market_cap, _MKT_CAP_THRESHOLD
            except ImportError:
                from src.fundamentals.edgar.edgar_postgres import get_postgres_connection as _get_conn, init_edgar_postgres_tables as _init_edgar
                from src.fundamentals.edgar.filings.filings_postgres import get_exchange_for_accessions, get_company_name_for_accessions
                from src.fundamentals.edgar.filings.filings_scraper_xbrl_adj import _get_market_cap, _MKT_CAP_THRESHOLD
            conn = _get_conn()
            _init_edgar(conn)  # ensures companies.exchange exists (ADD COLUMN IF NOT EXISTS)
            if use_zip:
                accessions = [Path(name).stem for name in txt_members]
            else:
                accessions = [fp.stem for fp in txt_files]
            exchange_lookup = get_exchange_for_accessions(conn, accessions)
            company_name_lookup = get_company_name_for_accessions(conn, accessions)  # master_idx_files
            conn.close()
            # Extract year/quarter for error message
            parts = input_path.parts
            year_quarter = ""
            for i, p in enumerate(parts):
                if p.isdigit() and len(p) == 4 and i + 1 < len(parts) and parts[i + 1].startswith("QTR"):
                    year_quarter = f"{p} {parts[i + 1]}"
                    break
            if use_zip:
                logger.info(
                    "Scraping %s .txt member(s) from zip (copy to RAMDISK, then same pipeline as on-disk): %s",
                    total,
                    zip_path,
                )
                worker_args = [
                    (
                        str(zip_path),
                        member,
                        str(input_path),
                        form_type,
                        exchange_lookup,
                        year_quarter,
                        company_name_lookup,
                        getattr(args, "debug", False),
                        str(scrape_errors_log),
                        str(check_warnings_log),
                    )
                    for member in txt_members
                ]
            else:
                logger.info("Scraping %s filing(s)", total)
                worker_args = [
                    (
                        str(fp),
                        form_type,
                        str(input_path),
                        exchange_lookup,
                        year_quarter,
                        company_name_lookup,
                        getattr(args, "debug", False),
                        str(scrape_errors_log),
                        str(check_warnings_log),
                    )
                    for fp in txt_files
                ]
            check_failures_log.open("w").close()  # truncate at start
            scrape_errors_log.open("w").close()  # truncate at start
            check_warnings_log.open("w").close()  # truncate at start

            ok_xbrl, ok_html, ok_adj, err = 0, 0, 0, 0
            skipped_ixbrl = 0
            failures: List[tuple] = []
            scrape_worker = _scrape_zip_member if use_zip else _scrape_one_filing
            with Pool(processes=n_workers) as pool:
                with tqdm(total=total, desc="Scraping filings", unit="file", dynamic_ncols=True) as pbar:
                    for result in pool.imap(scrape_worker, worker_args, chunksize=1):
                        o_xbrl, o_html, o_adj, o_err, fail_entry, skip_ix = result
                        ok_xbrl += o_xbrl
                        ok_html += o_html
                        ok_adj += o_adj
                        err += o_err
                        skipped_ixbrl += skip_ix
                        if fail_entry is not None:
                            failures.append(fail_entry)
                        pbar.update(1)
                        pbar.set_postfix(
                            xbrl=ok_xbrl,
                            html=ok_html,
                            adj=ok_adj,
                            scrape_err=err,
                            check_fail=len(failures),
                            skip_ixbrl=skipped_ixbrl,
                        )

            if failures:
                logger.info("Check failures logged to: %s (%s file(s), %s failed tests)", check_failures_log, len(failures), sum(f[2] for f in failures))
                small_cap: List[tuple] = []

                def _mkt_cap_str(mkt_cap: Optional[float]) -> str:
                    if mkt_cap is None:
                        return "N/A"
                    if mkt_cap >= 1e9:
                        return f"${mkt_cap / 1e9:.2f}B"
                    if mkt_cap >= 1e6:
                        return f"${mkt_cap / 1e6:.1f}M"
                    if mkt_cap >= 1e3:
                        return f"${mkt_cap / 1e3:.1f}K"
                    return f"${mkt_cap:,.0f}"

                # Fetch market cap for all failures, sort main list by mkt_cap descending (largest first)
                failures_with_mcap: List[tuple] = []
                for fail_entry in failures:
                    accession, failed_list, n_failed, n_total, ticker, exchange = fail_entry
                    mkt_cap = _get_market_cap(ticker) if ticker else None
                    if mkt_cap is not None and mkt_cap < _MKT_CAP_THRESHOLD:
                        small_cap.append((accession, ticker, exchange, n_failed, n_total, mkt_cap))
                    failures_with_mcap.append((fail_entry, mkt_cap))
                # None last, then descending by mkt_cap
                failures_with_mcap.sort(key=lambda x: (x[1] is None, -(x[1] or 0)))

                with open(check_failures_log, "w", encoding="utf-8") as lf:
                    for fail_entry, mkt_cap in failures_with_mcap:
                        accession, failed_list, n_failed, n_total, ticker, exchange = fail_entry
                        _t = ticker or "N/A"
                        _e = exchange or "N/A"
                        mc_str = _mkt_cap_str(mkt_cap)
                        lf.write(f"{accession} ticker={_t} exchange={_e} mkt_cap={mc_str}: {n_failed}/{n_total} failed\n")
                        for name, pct in failed_list:
                            if pct is not None:
                                lf.write(f"  - {name}: LHS > RHS by {pct:.2f}%\n")
                            else:
                                lf.write(f"  - {name}\n")

                if small_cap:
                    small_cap.sort(key=lambda x: -x[5])  # by mkt_cap descending (closest to $1B first)
                    with open(check_failures_log, "a", encoding="utf-8") as lf:
                        lf.write("\n--- Check failures with market cap < $1B ---\n")
                        for accession, ticker, exchange, n_failed, n_total, mkt_cap in small_cap:
                            _t = ticker or "N/A"
                            _e = exchange or "N/A"
                            mc_str = _mkt_cap_str(mkt_cap)
                            lf.write(f"{accession} ticker={_t} exchange={_e}: mkt_cap={mc_str} ({n_failed}/{n_total} failed)\n")
                    # Remove adj.json for small-cap failures
                    removed_small_cap = 0
                    for accession, *_ in small_cap:
                        adj_path = input_path / f"{accession}.json"
                        if adj_path.exists():
                            try:
                                adj_path.unlink()
                                removed_small_cap += 1
                            except OSError:
                                pass
                    if removed_small_cap:
                        logger.info("Removed %s adj.json file(s) for check failures with market cap < $1B", removed_small_cap)
            elif check_failures_log.exists():
                check_failures_log.unlink()

            # Remove all .xbrl.json when not --debug (workers keep them on failure)
            if not getattr(args, 'debug', False):
                removed_xbrl = 0
                for p in input_path.glob("*.xbrl.json"):
                    try:
                        p.unlink()
                        removed_xbrl += 1
                    except OSError:
                        pass
                if removed_xbrl:
                    logger.info("Removed %s .xbrl.json file(s)", removed_xbrl)

            n_failed_tests = sum(f[2] for f in failures)
            logger.info(
                "Scraped XBRL: %s, HTML: %s, adj: %s, scrape_err: %s, filings_with_check_fail: %s, failed_tests: %s",
                ok_xbrl, ok_html, ok_adj, err, len(failures), n_failed_tests,
            )
            if err:
                logger.info("Scrape errors logged to: %s", scrape_errors_log)
            return 0

        # Upload adj .json to filings_facts (--upload-scraped-filings)
        if args.upload_scraped_filings:
            if not (args.year and args.quarter and args.form_type):
                logger.error("--upload-scraped-filings requires --year, --quarter, and --form-type")
                return 1
            input_path = (
                storage_base
                / "fundamentals"
                / "edgar"
                / "filings"
                / str(args.year)
                / args.quarter
                / form_type_filesystem_slug(args.form_type)
            )
            if not input_path.exists():
                logger.error("Input directory not found: %s", input_path)
                return 1
            form_type = args.form_type or "10-Q"
            try:
                from .edgar_postgres import get_postgres_connection as _get_conn, init_edgar_postgres_tables as _init_edgar, upload_adj_jsons_from_dir
            except ImportError:
                from src.fundamentals.edgar.edgar_postgres import get_postgres_connection as _get_conn, init_edgar_postgres_tables as _init_edgar, upload_adj_jsons_from_dir
            conn = _get_conn()
            _init_edgar(conn)
            logger.info("Uploading scraped filings to PostgreSQL (filings_facts, filings_facts_integrity_checks, filings_facts_integrity_checks_errors, filings_facts_integrity_checks_warnings)")
            total = upload_adj_jsons_from_dir(conn, input_path, filing_type=form_type)
            conn.close()
            logger.info("Uploaded adj JSON to filings_facts: %s rows from %s", total, input_path)
            return 0

    except ValueError as e:
        logger.error("%s", e)
        return 1
    except Exception as e:
        logger.exception("Error: %s", e)
        return 1


if __name__ == "__main__":
    main()

