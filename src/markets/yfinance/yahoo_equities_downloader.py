"""
Yahoo Finance Equities Data Downloader

This module downloads equity data and metadata from Yahoo Finance.
"""

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

import pandas as pd

from .utils import _df_to_records, _series_or_dict_to_json_safe
from .yahoo_downloader_base import YahooDownloaderBase

logger = logging.getLogger(__name__)


class YahooEquitiesDownloader(YahooDownloaderBase):
    """Class to download equity data and metadata from Yahoo Finance."""

    def get_equity_extended_data(self, ticker: str) -> Dict:
        """
        Get all extended equity data including valuation measures, EPS revisions,
        revenue estimates, and analyst recommendations

        Args:
            ticker: Stock ticker symbol

        Returns:
            Dictionary with all data sections
        """
        result = {
            'equity_info': None,
            'valuation_measures': [],
            'eps_revisions': [],
            'revenue_estimates': [],
            'analyst_recommendations': [],
            'analyst_price_targets': [],
        }

        try:
            stock = self._ticker(ticker)
            
            # Get basic info
            info = stock.info
            if info:
                result['equity_info'] = info
            
            # Get analyst recommendations (historical)
            try:
                recommendations = stock.recommendations
                if recommendations is not None and len(recommendations) > 0:
                    for date, row in recommendations.iterrows():
                        result['analyst_recommendations'].append({
                            'symbol': ticker,
                            'date': date.strftime('%Y-%m-%d') if hasattr(date, 'strftime') else str(date),
                            'firm': str(row.get('Firm', '')),
                            'to_grade': str(row.get('To Grade', '')),
                            'from_grade': str(row.get('From Grade', '')),
                            'action': str(row.get('Action', '')),
                            'created_at': datetime.now().isoformat(),
                        })
            except Exception as e:
                logger.warning("Could not fetch recommendations for %s: %s", ticker, e)
            
            # Get upgrades/downgrades (more detailed rating changes)
            try:
                upgrades_downgrades = stock.upgrades_downgrades
                if upgrades_downgrades is not None and len(upgrades_downgrades) > 0:
                    for date, row in upgrades_downgrades.iterrows():
                        result['analyst_recommendations'].append({
                            'symbol': ticker,
                            'date': date.strftime('%Y-%m-%d') if hasattr(date, 'strftime') else str(date),
                            'firm': str(row.get('Firm', '')),
                            'to_grade': str(row.get('To Grade', '')),
                            'from_grade': str(row.get('From Grade', '')),
                            'action': str(row.get('Action', '')),
                            'created_at': datetime.now().isoformat(),
                        })
            except Exception as e:
                logger.warning("Could not fetch upgrades/downgrades for %s: %s", ticker, e)
            
            # valuation_measures live in .info (equity_info); no separate fetch
        except Exception as e:
            logger.warning("Error fetching extended data for %s: %s", ticker, e)
        
        return result

    def get_equity_info(self, ticker: str) -> Optional[Dict]:
        """Return the full .info blob for the ticker (JSON-serializable)."""
        return self.get_info(ticker)

    def get_analyst_recommendations(self, ticker: str) -> List[Dict[str, Any]]:
        """Return list of analyst recommendation/upgrade-downgrade records (JSON-serializable)."""
        out = []
        try:
            stock = self._ticker(ticker)
            for df, source in [(stock.recommendations, "recommendations"), (stock.upgrades_downgrades, "upgrades_downgrades")]:
                if df is not None and not df.empty:
                    for date, row in df.iterrows():
                        rec = {
                            "symbol": ticker,
                            "date": date.strftime("%Y-%m-%d") if hasattr(date, "strftime") else str(date),
                            "firm": str(row.get("Firm", row.get("firm", "")) or ""),
                            "to_grade": str(row.get("To Grade", row.get("toGrade", "")) or ""),
                            "from_grade": str(row.get("From Grade", row.get("fromGrade", "")) or ""),
                            "action": str(row.get("Action", row.get("action", "")) or ""),
                            "source": source,
                        }
                        out.append(rec)
        except Exception as e:
            logger.warning("Could not fetch analyst recommendations for %s: %s", ticker, e)
        return out

    def get_eps_revisions(self, ticker: str) -> List[Dict[str, Any]]:
        """Return EPS revisions (period -> up/down counts) as list of records."""
        try:
            stock = self._ticker(ticker)
            df = stock.eps_revisions
            if df is None or df.empty:
                return []
            return _df_to_records(df)
        except Exception as e:
            logger.warning("Could not fetch eps_revisions for %s: %s", ticker, e)
            return []

    def get_revenue_estimates(self, ticker: str) -> List[Dict[str, Any]]:
        """Return revenue estimate DataFrame as list of records."""
        try:
            stock = self._ticker(ticker)
            df = stock.revenue_estimate
            return _df_to_records(df) if df is not None and not df.empty else []
        except Exception as e:
            logger.warning("Could not fetch revenue_estimate for %s: %s", ticker, e)
            return []

    def get_analyst_price_targets(self, ticker: str) -> Optional[Dict[str, Any]]:
        """Return analyst price targets dict (current, low, high, mean, median)."""
        try:
            stock = self._ticker(ticker)
            d = stock.analyst_price_targets
            if not d:
                return None
            return _series_or_dict_to_json_safe(d)
        except Exception as e:
            logger.warning("Could not fetch analyst_price_targets for %s: %s", ticker, e)
            return None

    def get_earnings_calendar(self, ticker: str) -> List[Dict[str, Any]]:
        """Return earnings dates (earnings_dates) as list of records."""
        try:
            stock = self._ticker(ticker)
            df = stock.earnings_dates
            return _df_to_records(df) if df is not None and not df.empty else []
        except Exception as e:
            logger.warning("Could not fetch earnings_dates for %s: %s", ticker, e)
            return []

    def get_earnings_history(self, ticker: str) -> List[Dict[str, Any]]:
        """Return earnings history as list of records."""
        try:
            stock = self._ticker(ticker)
            df = stock.earnings_history
            return _df_to_records(df) if df is not None and not df.empty else []
        except Exception as e:
            logger.warning("Could not fetch earnings_history for %s: %s", ticker, e)
            return []

    def get_financial_statements(self, ticker: str) -> Dict[str, Any]:
        """Return income_stmt, balance_sheet, cashflow (yearly + quarterly) as JSON-serializable dict."""
        out = {}
        try:
            stock = self._ticker(ticker)
            for name, attr in [
                ("income_stmt", stock.income_stmt),
                ("quarterly_income_stmt", stock.quarterly_income_stmt),
                ("balance_sheet", stock.balance_sheet),
                ("quarterly_balance_sheet", stock.quarterly_balance_sheet),
                ("cashflow", stock.cashflow),
                ("quarterly_cashflow", stock.quarterly_cashflow),
            ]:
                if attr is not None and not (isinstance(attr, pd.DataFrame) and attr.empty):
                    out[name] = _series_or_dict_to_json_safe(attr)
                else:
                    out[name] = None
        except Exception as e:
            logger.warning("Could not fetch financial_statements for %s: %s", ticker, e)
        return out

    def get_calendar(self, ticker: str) -> Optional[Dict[str, Any]]:
        """Return calendar (dividend, ex-dividend, earnings dates) as dict."""
        try:
            stock = self._ticker(ticker)
            d = stock.calendar
            return _series_or_dict_to_json_safe(d) if d else None
        except Exception as e:
            logger.warning("Could not fetch calendar for %s: %s", ticker, e)
            return None

    def get_sec_filings(self, ticker: str) -> List[Dict[str, Any]]:
        """Return SEC filings list (each item has date, type, title, url, etc.)."""
        try:
            stock = self._ticker(ticker)
            raw = stock.sec_filings
            filings = raw if isinstance(raw, list) else (raw.get("filings", []) if isinstance(raw, dict) else [])
            out = []
            for f in filings:
                if isinstance(f, dict):
                    row = {}
                    for k, v in f.items():
                        if hasattr(v, "isoformat"):
                            row[k] = v.isoformat()
                        else:
                            row[k] = v
                    out.append(row)
            return out
        except Exception as e:
            logger.warning("Could not fetch sec_filings for %s: %s", ticker, e)
            return []


