"""
Download SEC EDGAR filings by path

This script downloads filings from SEC EDGAR using full paths.
Downloads are saved to a "filings" subfolder.
"""

import os
import sys
import argparse
import logging
import shutil
import time
from pathlib import Path
import requests
from typing import Optional, List, TYPE_CHECKING
from tqdm import tqdm

if TYPE_CHECKING:
    import psycopg2

# Handle imports for both module import and direct script execution
try:
    from ...download_logger import get_download_logger
    from ..edgar import EDGARDownloader
    from ..edgar_postgres import get_postgres_connection
    from .filings_postgres import get_filings_filenames, get_filing_metadata_by_accession
    from ..form_type_path import form_type_filesystem_slug
    from .filings_quarter_archive import (
        quarter_filings_zip_path,
        zip_folder_and_remove,
    )
except ImportError:
    # Handle direct script execution - use absolute imports (filings -> edgar -> fundamentals -> src -> project root)
    file_path = Path(__file__).resolve()
    project_root = file_path.parent.parent.parent.parent.parent
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))
    from src.fundamentals.download_logger import get_download_logger
    from src.fundamentals.edgar.edgar import EDGARDownloader
    from src.fundamentals.edgar.edgar_postgres import get_postgres_connection
    from src.fundamentals.edgar.filings.filings_postgres import get_filings_filenames, get_filing_metadata_by_accession
    from src.fundamentals.edgar.form_type_path import form_type_filesystem_slug
    from src.fundamentals.edgar.filings.filings_quarter_archive import (
        quarter_filings_zip_path,
        zip_folder_and_remove,
    )

# Set up logger using download_logger utility with console output
logger = get_download_logger('edgar_filings', log_level=logging.INFO, add_console_handler=True)

# HTTP status codes that indicate SEC.gov is temporarily unavailable or rate-limiting
# (not a URL/path construction problem).
#  - 429: Too Many Requests (rate limit) – respect Retry-After when present
#  - 502/503/504: temporary outage / maintenance
SEC_UNAVAILABLE_STATUS_CODES = (429, 502, 503, 504)


class SECUnavailableError(RuntimeError):
    """Raised when SEC.gov is temporarily unavailable or rate-limiting (429/502/503/504)."""

    def __init__(
        self,
        message: str,
        status_code: Optional[int] = None,
        url: str = "",
        retry_after: Optional[int] = None,
    ):
        super().__init__(message)
        self.status_code = status_code
        self.url = url
        # Optional Retry-After hint (seconds) for 429 responses
        self.retry_after = retry_after


class FilingDownloader(EDGARDownloader):
    """Subclass of EDGARDownloader for downloading filings by path"""

    def download_filing_by_path(
        self,
        filing_path: str,
        output_dir: Optional[str] = None
    ) -> Path:
        """
        Download a filing from SEC EDGAR by full path

        Args:
            filing_path: Full path to filing (e.g., "edgar/data/315293/0001179110-05-003398.txt")
            output_dir: Output directory for downloaded files (default: "filings" subfolder in edgar directory)

        Returns:
            Path to downloaded file

        Raises:
            FileNotFoundError: If the filing is not found (404) — check path/accession.
            SECUnavailableError: If SEC.gov returns 502/503/504 (temporary outage), not a URL error.
            RuntimeError: Other HTTP or network errors.
        """
        # Determine output directory
        if output_dir is None:
            # Default: storage/{env}/fundamentals/edgar/filings
            try:
                from src.config import get_environment
                env = get_environment()
                storage_env = 'test' if env == 'staging' else env
            except Exception:
                storage_env = 'dev'
            project_root = Path(__file__).resolve().parent.parent.parent.parent.parent
            storage_root = os.getenv('TRADING_AGENT_STORAGE')
            if storage_root:
                base = Path(storage_root) / storage_env
            else:
                base = project_root / 'storage' / storage_env
            output_dir = str(base / 'fundamentals' / 'edgar' / 'filings')
        else:
            output_dir = str(Path(output_dir).resolve())

        # Create output directory if it doesn't exist
        Path(output_dir).mkdir(parents=True, exist_ok=True)

        # SEC URLs require 3 segments: edgar/data/{cik}/{accession_no_hyphens}/{accession}.txt
        sec_path = _filing_path_to_sec_url_path(filing_path)
        filing_url = f"{self.base_url}/Archives/{sec_path}"

        # Download the filing
        logger.debug(f"Downloading filing: {filing_path}")
        logger.debug(f"URL: {filing_url}")

        try:
            response = requests.get(filing_url, headers=self.headers, timeout=30)
            response.raise_for_status()

            # Extract filename from path
            filename = Path(filing_path).name

            # Save to file
            output_file = Path(output_dir) / filename
            output_file.write_bytes(response.content)

            file_size = output_file.stat().st_size
            logger.debug(f"Downloaded successfully: {output_file} ({file_size:,} bytes)")

            return output_file

        except requests.exceptions.HTTPError as e:
            status_code = e.response.status_code if e.response is not None else None
            if status_code == 404:
                raise FileNotFoundError(
                    f"Filing not found (check path/accession): {filing_path}. URL: {filing_url}"
                ) from e
            if status_code in SEC_UNAVAILABLE_STATUS_CODES:
                retry_after: Optional[int] = None
                if status_code == 429 and e.response is not None:
                    ra = e.response.headers.get("Retry-After")
                    if ra is not None:
                        try:
                            retry_after = int(ra)
                        except ValueError:
                            retry_after = None
                raise SECUnavailableError(
                    f"SEC.gov temporarily unavailable (HTTP {status_code}). "
                    "This is not a URL error; try again later. URL: {url}".format(
                        url=filing_url
                    ),
                    status_code=status_code,
                    url=filing_url,
                    retry_after=retry_after,
                ) from e
            raise RuntimeError(f"HTTP error downloading filing {filing_path}: {e}") from e
        except requests.exceptions.RequestException as e:
            raise RuntimeError(f"Error downloading filing {filing_path}: {e}") from e

    def download_filings(
        self,
        dbname: Optional[str] = None,
        output_dir: Optional[str] = None,
        limit: Optional[int] = None,
        sql_query: Optional[str] = None,
        force_all: bool = False,
        archive_to_zip: bool = False,
        **filters
    ) -> List[Path]:
        """
        Download filings from master_idx_files table based on flexible filter criteria or raw SQL query

        This method uses the query building functions from filings_postgres to construct
        dynamic SQL queries based on provided filters, or accepts a raw SQL query string.
        Any combination of filters can be specified, and only matching filings will be downloaded.

        Args:
            dbname: Database name. If None, derived from module (e.g. edgar)
            output_dir: Output directory for downloaded files (default: "filings" subfolder)
            limit: Optional limit on number of filings to download (primarily for tests).
                   Ignored if sql_query is provided.
            force_all: If True, download all filings even if the .txt file already exists
                       in the target output directory. If False (default), skip already
                       existing .txt files and only download missing ones.
            archive_to_zip: If True, after a successful full-quarter download (no sql_query/limit,
                            all expected .txt present), zip the form folder and remove loose files.
                            Only applies when **not** filtering by CIK (full catalog quarter download).
                            Default False.
            sql_query: Optional raw SQL query string. If provided, filters and limit are ignored.
                      Query should return a column named 'filename' or be a SELECT * query.
                      Example: "SELECT * FROM master_idx_files WHERE company_name LIKE '%NVIDIA%' AND year = 2019"
            **filters: Flexible filter criteria. Supported filters:
                - year: Year (e.g., 2005)
                - quarter: Quarter (e.g., 'QTR1', 'QTR2', 'QTR3', 'QTR4')
                - form_type: Form type (e.g., '10-K', '10-Q')
                - cik: CIK (Central Index Key) as string or int
                - filename: Exact filename (e.g., 'edgar/data/315293/0001179110-05-003398.txt')
                - date_filed: Filing date (DATE format: 'YYYY-MM-DD' or date object)
                - company_name: Company name (partial match with ILIKE, case-insensitive)

        Returns:
            List of Paths to downloaded files

        Examples:
            # Download all filings for year 2005
            downloader.download_filings(year=2005)

            # Download 10-K filings for Q1 2005, limit 100
            downloader.download_filings(year=2005, quarter='QTR1', form_type='10-K', limit=100)

            # Download NVIDIA filings for Q1 2019, 10-K (equivalent to SQL query)
            downloader.download_filings(company_name='NVIDIA', year=2019, quarter='QTR1', form_type='10-K')

            # Download using raw SQL query
            downloader.download_filings(sql_query="SELECT * FROM master_idx_files WHERE company_name LIKE '%NVIDIA%' AND year = 2019 AND quarter = 'QTR1' AND form_type = '10-K'")

            # Download all filings for a specific CIK
            downloader.download_filings(cik='0000315293')

            # Download a specific filing by filename
            downloader.download_filings(filename='edgar/data/315293/0001179110-05-003398.txt')

            # Download all filings for a CIK in a specific year
            downloader.download_filings(cik='0000315293', year=2005)
        """
        # Resolve output directory first (needed to skip download when quarter is already archived).
        if output_dir is None:
            try:
                from src.config import get_environment
                env = get_environment()
                storage_env = 'test' if env == 'staging' else env
            except Exception:
                storage_env = 'dev'
            project_root = Path(__file__).resolve().parent.parent.parent.parent.parent
            storage_root = os.getenv('TRADING_AGENT_STORAGE')
            if storage_root:
                base = Path(storage_root)
            else:
                base = project_root / 'storage' / storage_env
            effective_out_dir = base / 'fundamentals' / 'edgar' / 'filings'
        else:
            effective_out_dir = Path(output_dir).resolve()

        quarter_zip_path = quarter_filings_zip_path(effective_out_dir)
        if quarter_zip_path is not None:
            if force_all and quarter_zip_path.exists():
                quarter_zip_path.unlink()
                logger.info("Removed existing quarter archive (--force-all): %s", quarter_zip_path)
            elif quarter_zip_path.exists():
                logger.info(
                    "Quarter already archived (zip present), skipping download: %s",
                    quarter_zip_path,
                )
                return []

        conn = get_postgres_connection(dbname=dbname)

        try:
            # Get filenames using query building functions or raw SQL
            filenames = get_filings_filenames(conn, limit=limit, sql_query=sql_query, **filters)

            if not filenames:
                if sql_query:
                    raise ValueError(f"No filings found for SQL query")
                else:
                    filter_str = ", ".join(f"{k}={v}" for k, v in filters.items() if v is not None)
                    hint = (
                        " Ensure the EDGAR catalog includes this year/quarter (run 'EDGAR - Generate Catalog'). "
                        "Year/quarter are the SEC index period when the filing was filed (e.g. 2025 Q4 10-Q is filed in Oct–Nov 2025 → year=2025, quarter=QTR4)."
                    )
                    raise ValueError(f"No filings found for filters: {filter_str}.{hint}")

            logger.info(f"Found {len(filenames)} filings to download")

            effective_out_dir.mkdir(parents=True, exist_ok=True)

            # Download each filing with progress bar
            downloaded_files = []
            failed_downloads = []

            with tqdm(total=len(filenames), desc="Downloading filings", unit="file") as pbar:
                for filename in filenames:
                    # Skip download if the target .txt already exists in the output directory,
                    # unless force_all=True is explicitly requested.
                    target_path = effective_out_dir / Path(filename).name
                    if target_path.exists() and not force_all:
                        logger.debug("Skipping already-downloaded filing (exists on disk): %s", target_path)
                        pbar.update(1)
                        downloaded_files.append(target_path)
                        continue
                    # Best-effort retries for temporary SEC.gov outages / rate limits
                    attempt = 1
                    while True:
                        try:
                            output_file = self.download_filing_by_path(filename, output_dir=output_dir)
                            downloaded_files.append(output_file)
                            pbar.update(1)
                            break
                        except SECUnavailableError as e:
                            # Infinite retry with backoff until SEC.gov recovers.
                            # Only log a warning on the first failure to avoid flooding the logs.
                            # Subsequent attempts wait silently.
                            # Retry with backoff:
                            # - Prefer explicit Retry-After from SEC for 429
                            # - Otherwise exponential backoff capped at 300s
                            if getattr(e, "status_code", None) == 429 and getattr(e, "retry_after", None):
                                ra = int(getattr(e, "retry_after"))
                                sleep_seconds = max(30, min(300, ra))
                            else:
                                sleep_seconds = min(300, 30 * attempt)
                            if attempt == 1:
                                logger.warning(
                                    "SEC.gov temporarily unavailable for %s. "
                                    "Will keep retrying with backoff; next retry in %s seconds. Error: %s",
                                    filename,
                                    sleep_seconds,
                                    e,
                                )
                            time.sleep(sleep_seconds)
                            attempt += 1
                            continue
                        except Exception as e:
                            failed_downloads.append((filename, str(e)))
                            logger.warning(f"Error downloading {filename}: {e}")
                            pbar.update(1)
                            # Continue with next filing instead of stopping
                            break

            if failed_downloads:
                logger.warning(f"Failed to download {len(failed_downloads)}/{len(filenames)} filings")
            else:
                logger.info(f"Successfully downloaded {len(downloaded_files)}/{len(filenames)} filings")

            # Full quarter / catalog download only — not CIK- (or company-) filtered subsets.
            quarter_year_catalog_only = filters.get("cik") is None and filters.get("company_name") is None

            every_expected_txt_on_disk = all(
                (effective_out_dir / Path(fn).name).is_file() for fn in filenames
            )
            can_auto_archive = (
                archive_to_zip
                and quarter_year_catalog_only
                and quarter_zip_path is not None
                and sql_query is None
                and limit is None
                and not failed_downloads
                and every_expected_txt_on_disk
            )
            if can_auto_archive:
                zip_folder_and_remove(effective_out_dir, quarter_zip_path)
            elif quarter_zip_path is not None and sql_query is None and limit is None:
                if not archive_to_zip:
                    logger.debug(
                        "Quarter zip skipped (archive_to_zip=False); use --archive-to-zip on full quarter download.",
                    )
                elif not quarter_year_catalog_only:
                    logger.debug(
                        "Quarter zip skipped (CIK or company_name filter); archive only for full quarter catalog download.",
                    )
                elif failed_downloads:
                    logger.info(
                        "Skipping quarter zip/archive: %s download(s) failed; "
                        "archive runs only when every filing succeeds.",
                        len(failed_downloads),
                    )
                elif not every_expected_txt_on_disk:
                    logger.info(
                        "Skipping quarter zip/archive: not all expected .txt files present on disk.",
                    )

            return downloaded_files

        finally:
            conn.close()


def _accession_code_to_path(accession_code: str) -> str:
    """Build SEC EDGAR full path from accession code.
    SEC URL format: edgar/data/{cik}/{accession_no_hyphens}/{accession_with_hyphens}.txt
    e.g. 0000320193-25-000073 -> edgar/data/320193/000032019325000073/0000320193-25-000073.txt
    """
    code = accession_code.strip()
    if code.endswith('.txt'):
        code = code[:-4]
    if '/' in code:
        code = code.split('/')[-1]
    if '-' not in code:
        raise ValueError(f"Invalid accession code: {accession_code!r} (expected format e.g. 0000320193-26-000006)")
    cik_part = code.split('-')[0]
    try:
        cik = str(int(cik_part))
    except ValueError:
        raise ValueError(f"Invalid accession code: {accession_code!r} (CIK part must be digits)")
    # SEC requires middle segment: accession with hyphens removed
    accession_no_hyphens = code.replace('-', '')
    return f"edgar/data/{cik}/{accession_no_hyphens}/{code}.txt"


def _filing_path_to_sec_url_path(filing_path: str) -> str:
    """Convert filing path to SEC URL path (3 segments after edgar/data).
    DB and master.idx use 2 segments: edgar/data/{cik}/{accession}.txt
    SEC URLs require 3 segments: edgar/data/{cik}/{accession_no_hyphens}/{accession}.txt
    """
    path = filing_path.strip()
    if not path.startswith('edgar/data/'):
        return path
    parts = path.split('/')
    # edgar, data, cik, [compact?,] accession.txt -> need 5 parts for correct form
    if len(parts) == 4:
        # edgar/data/cik/filename.txt -> insert compact segment
        cik, filename = parts[2], parts[3]
        if filename.endswith('.txt') and '-' in filename:
            accession = filename[:-4]
            accession_no_hyphens = accession.replace('-', '')
            return f"edgar/data/{cik}/{accession_no_hyphens}/{filename}"
    return path


def main():
    """Main function for command-line usage"""
    parser = argparse.ArgumentParser(
        description='Download SEC EDGAR filings by accession code or by CIK/year/quarter/form',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Download by accession code; output dir is resolved from master_idx (year/quarter/form_type):
  python -m src.fundamentals.edgar.filings.filings_downloader --accession-code 0000320193-25-000073

  # Download by CIK for a specific quarter and form type:
  python -m src.fundamentals.edgar.filings.filings_downloader --cik 320193 --year 2026 --quarter QTR1 --filing 10-Q
        """
    )
    mode_group = parser.add_mutually_exclusive_group(required=True)
    mode_group.add_argument(
        '--accession-code',
        metavar='CODE',
        dest='accession_code',
        type=str,
        help='Accession code only (e.g. 0000320193-25-000073). Path and output subdir (year/quarter/form_type) are resolved from master_idx.'
    )
    mode_group.add_argument(
        '--cik',
        metavar='CIK',
        type=str,
        help='Download filings for this CIK; requires --year, --quarter, and --filing.'
    )
    parser.add_argument(
        '--year',
        type=int,
        metavar='YEAR',
        help='Year (required with --cik only).'
    )
    parser.add_argument(
        '--quarter',
        type=str,
        choices=['QTR1', 'QTR2', 'QTR3', 'QTR4'],
        metavar='QTR',
        help='Quarter (required with --cik only).'
    )
    parser.add_argument(
        '--filing',
        dest='form_type',
        type=str,
        metavar='FORM',
        help='Form type (required with --cik), e.g. 10-K, 10-Q, 8-K.'
    )

    args = parser.parse_args()

    if args.accession_code is not None:
        if args.form_type is not None:
            parser.error('--filing is not allowed with --accession-code')
        if args.year is not None or args.quarter is not None:
            parser.error('--year and --quarter are not allowed with --accession-code')
    else:
        if args.year is None or args.quarter is None or args.form_type is None:
            parser.error('--cik requires --year, --quarter, and --filing')

    user_agent = os.getenv('EDGAR_USER_AGENT', 'VittorioApicella apicellavittorio@hotmail.it')
    try:
        downloader = FilingDownloader(user_agent=user_agent)
        # Resolve storage base (TRADING_AGENT_STORAGE common root + ENV)
        project_root = Path(__file__).resolve().parent.parent.parent.parent.parent
        storage_root = os.getenv("TRADING_AGENT_STORAGE")
        storage_env = os.getenv("ENV", "dev")
        if storage_root:
            storage_base = Path(storage_root) / storage_env
        else:
            storage_base = project_root / "storage" / storage_env
        base_out = storage_base / "fundamentals" / "edgar" / "filings"
        if args.accession_code is not None:
            filing_path = _accession_code_to_path(args.accession_code)
            # Resolve final dir from master_idx: year/quarter/form_type
            conn = get_postgres_connection()
            try:
                meta = get_filing_metadata_by_accession(conn, args.accession_code)
            finally:
                conn.close()
            if meta:
                year, quarter, form_type = meta
                target_dir = base_out / str(year) / quarter / form_type_filesystem_slug(form_type)
                target_dir.mkdir(parents=True, exist_ok=True)
                output_file = downloader.download_filing_by_path(
                    filing_path=filing_path,
                    output_dir=str(target_dir)
                )
            else:
                # Not in catalog: place in unknown/
                unknown_dir = base_out / "unknown"
                unknown_dir.mkdir(parents=True, exist_ok=True)
                output_file = downloader.download_filing_by_path(
                    filing_path=filing_path,
                    output_dir=str(unknown_dir)
                )
            logger.info(f"Filing downloaded successfully: {output_file}")
            return 0
        else:
            # CIK mode: query catalog and download
            target_dir = (
                base_out
                / str(args.year)
                / args.quarter
                / form_type_filesystem_slug(args.form_type)
            )
            target_dir.mkdir(parents=True, exist_ok=True)
            downloaded = downloader.download_filings(
                output_dir=str(target_dir),
                cik=args.cik,
                year=args.year,
                quarter=args.quarter,
                form_type=args.form_type,
            )
            for f in downloaded:
                logger.info(f"Downloaded: {f}")
            return 0
    except Exception as e:
        logger.error(f"Error: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    main()
