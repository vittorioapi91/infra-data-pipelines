"""
EDGAR filings: download, scrape, and postgres helpers.
"""

from .filings_downloader import FilingDownloader, SECUnavailableError, main as filings_downloader_main
from .filings_postgres import (
    get_exchange_by_accession,
    get_exchange_for_accessions,
    get_company_name_for_accessions,
    get_filing_metadata_by_accession,
    build_filings_query,
    get_filings_filenames,
)
from .filings_scraper import (
    FilingsScraperInlineXBRL,
    FilingsScraperHTML,
    GUIDANCE_DESCRIPTION_KEYWORDS,
    GUIDANCE_CONTENT_KEYWORDS,
    GUIDANCE_CONTENT_PATTERNS,
)
from .filings_scraper_xbrl_adj import (
    xbrl_to_adj,
    extract_dei_ticker_exchange,
    extract_dei_from_txt_fallback,
    _is_etf,
    has_no_trading_symbol_flag,
    _get_market_cap,
    _MKT_CAP_THRESHOLD,
)

__all__ = [
    "FilingDownloader",
    "SECUnavailableError",
    "filings_downloader_main",
    "get_exchange_by_accession",
    "get_exchange_for_accessions",
    "get_company_name_for_accessions",
    "get_filing_metadata_by_accession",
    "build_filings_query",
    "get_filings_filenames",
    "FilingsScraperInlineXBRL",
    "FilingsScraperHTML",
    "GUIDANCE_DESCRIPTION_KEYWORDS",
    "GUIDANCE_CONTENT_KEYWORDS",
    "GUIDANCE_CONTENT_PATTERNS",
    "xbrl_to_adj",
    "extract_dei_ticker_exchange",
    "extract_dei_from_txt_fallback",
    "_is_etf",
    "has_no_trading_symbol_flag",
    "_get_market_cap",
    "_MKT_CAP_THRESHOLD",
]
