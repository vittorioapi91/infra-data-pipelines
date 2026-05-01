"""
Yahoo Finance ETF Data Downloader

Stripped-down downloader for ETF metadata. Fetches only Ticker.info (no analyst
recommendations, financial statements, etc.). Inherits from YahooDownloaderBase.
"""

from typing import Any, Dict, Optional

from .yahoo_downloader_base import YahooDownloaderBase


class YahooETFDownloader(YahooDownloaderBase):
    """Download ETF info from Yahoo Finance (Ticker.info only)."""

    def get_etf_info(self, ticker: str) -> Optional[Dict[str, Any]]:
        """Return the full .info blob for the ETF (JSON-serializable)."""
        return self.get_info(ticker)
