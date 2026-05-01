"""
Abstract base for Yahoo Finance downloaders.

Provides shared init, yfinance Ticker access, and get_info(). Both
YahooEquitiesDownloader and YahooETFDownloader inherit from here to keep
implementations aligned.
"""

import logging
from abc import ABC
from typing import TYPE_CHECKING, Any, Dict, Optional

from .utils import _series_or_dict_to_json_safe

if TYPE_CHECKING:
    import pandas as pd

logger = logging.getLogger(__name__)

try:
    import yfinance as yf
except ImportError:
    raise ImportError("yfinance is required. Install it with: pip install yfinance")


class YahooDownloaderBase(ABC):
    """Base class for Yahoo Finance data downloaders."""

    def __init__(self, delay_between_requests: float = 0.1):
        self.delay_between_requests = delay_between_requests

    def _ticker(self, symbol: str):
        """Return yfinance Ticker for the symbol. Override if different source needed."""
        return yf.Ticker(symbol)

    def get_info(self, ticker: str) -> Optional[Dict[str, Any]]:
        """Return the full .info blob for the ticker (JSON-serializable)."""
        try:
            stock = self._ticker(ticker)
            info = stock.info
            if not info:
                return None
            return _series_or_dict_to_json_safe(info)
        except Exception as e:
            logger.warning("Error fetching info for %s: %s", ticker, e)
            return None

    def get_history(
        self,
        ticker: str,
        period: str = "max",
        interval: str = "1d",
        start: Optional[str] = None,
        end: Optional[str] = None,
    ):
        """Return OHLCV history as DataFrame.
        Use period (1d, 5d, 1mo, 3mo, 6mo, 1y, 2y, 5y, 10y, ytd, max) OR start/end (YYYY-MM-DD).
        interval: 1d, 1wk, 1mo.
        """
        try:
            t = self._ticker(ticker)
            if start is not None or end is not None:
                df = t.history(start=start, end=end, interval=interval, auto_adjust=True)
            else:
                df = t.history(period=period, interval=interval, auto_adjust=True)
            return df if df is not None and not df.empty else None
        except Exception as e:
            logger.warning("Error fetching history for %s: %s", ticker, e)
            return None
