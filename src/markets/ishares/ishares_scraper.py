"""
iShares ETF Data Scraper

This module scrapes ETF data from the iShares website and stores it in CSV format.
"""

import csv
import io
import re
import sys
import time
import warnings
import requests
from typing import List, Dict, Optional, Tuple

# Retry config for HTTP requests (timeout / connection / 5xx)
_REQUEST_MAX_RETRIES = 3
_REQUEST_RETRY_BACKOFF_SEC = 2
from datetime import datetime
from pathlib import Path

from bs4 import BeautifulSoup
import pandas as pd
from tqdm import tqdm

# Date columns in iShares holdings CSV (exact header names); normalized to ISO YYYY-MM-DD on download
_HOLDINGS_DATE_COLUMNS = ["Accrual Date", "Maturity", "Effective Date"]
# Numeric columns (exact header names): comma = thousands, dot = decimal -> format as double
_HOLDINGS_NUMERIC_COLUMNS = [
    "Market Value",
    "Weight (%)",
    "Notional Value",
    "Quantity",
    "Price",
    "FX Rate",
]
_HOLDINGS_DATE_FORMATS = [
    "%Y-%m-%d",
    "%Y-%m-%d %H:%M:%S",
    "%b %d, %Y",
    "%B %d, %Y",
    "%d %b %Y",
    "%m/%d/%Y",
    "%d/%m/%Y",
    "%Y/%m/%d",
    "%d-%b-%Y",
    "%m-%d-%Y",
    "%Y",
]


def _normalize_holdings_date_series(
    s: pd.Series, file_context: Optional[str] = None
) -> pd.Series:
    """Parse date strings with multiple formats; return series of ISO YYYY-MM-DD or '' for null."""
    s = s.astype(str).str.strip()
    null_like = s.str.lower().isin(("nan", "none", "")) | (s == "")
    out = pd.Series("", index=s.index, dtype=str)
    out.loc[null_like] = ""
    remaining = s.loc[~null_like]
    if remaining.empty:
        return out
    for fmt in _HOLDINGS_DATE_FORMATS:
        if remaining.empty:
            break
        try:
            p = pd.to_datetime(remaining, format=fmt, errors="coerce")
            good = p.notna()
            out.loc[good.index] = p.loc[good].dt.strftime("%Y-%m-%d")
            remaining = remaining.loc[~good]
        except Exception as e:
            warnings.warn(f"Date format {fmt!r} failed: {e}", UserWarning, stacklevel=2)
            raise
    if not remaining.empty:
        try:
            with warnings.catch_warnings(record=True) as wlist:
                warnings.simplefilter("always", UserWarning)
                p = pd.to_datetime(remaining, errors="coerce")
                good = p.notna()
                out.loc[good.index] = p.loc[good].dt.strftime("%Y-%m-%d")
                remaining = remaining.loc[~good]
            if file_context:
                for w in wlist:
                    if (
                        w.category is UserWarning
                        and "Could not infer format" in str(w.message)
                    ):
                        warnings.warn(
                            f"{w.message} (file: {file_context})",
                            UserWarning,
                            stacklevel=2,
                        )
                        break
        except Exception as e:
            warnings.warn(f"Date fallback parse failed: {e}", UserWarning, stacklevel=2)
            raise
    return out


class iSharesScraper:
    """Class to scrape iShares ETF data from their website"""

    def __init__(self):
        self.base_url = "https://www.ishares.com"
        # Headers (omit Accept-Encoding - server returns minimal HTML when br requested)
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
        }

    def _get_with_retry(
        self,
        url: str,
        timeout: int = 30,
        *,
        max_retries: int = _REQUEST_MAX_RETRIES,
        backoff_sec: float = _REQUEST_RETRY_BACKOFF_SEC,
    ) -> requests.Response:
        """GET with retries on timeout, connection error, or 5xx."""
        last_error: Optional[Exception] = None
        for attempt in range(max_retries + 1):
            try:
                response = requests.get(url, headers=self.headers, timeout=timeout)
                response.raise_for_status()
                return response
            except requests.Timeout as e:
                last_error = e
                if attempt < max_retries:
                    time.sleep(backoff_sec * (attempt + 1))
                    continue
                raise
            except requests.ConnectionError as e:
                last_error = e
                if attempt < max_retries:
                    time.sleep(backoff_sec * (attempt + 1))
                    continue
                raise
            except requests.HTTPError as e:
                if e.response is not None and e.response.status_code in (502, 503, 504) and attempt < max_retries:
                    time.sleep(backoff_sec * (attempt + 1))
                    continue
                raise
        if last_error is not None:
            raise last_error
        raise RuntimeError("_get_with_retry exhausted retries")

    def _scrape_via_html(self, url: str) -> List[Dict]:
        """Scrape ETF table from page HTML using requests + BeautifulSoup."""
        fetch_url = url.split('#')[0] if '#' in url else url
        response = self._get_with_retry(fetch_url, timeout=30)
        soup = BeautifulSoup(response.text, 'html.parser')
        tables = soup.find_all('table')
        if not tables:
            return []

        etfs = []
        for table in tables:
            headers = []
            header_row = table.find('thead')
            if header_row:
                ths = header_row.find_all(['th', 'td'])
                headers = [th.get_text(strip=True) for th in ths]

            tbody = table.find('tbody') or table
            rows = tbody.find_all('tr')
            start = 0
            if not headers and rows:
                first = rows[0]
                cells = first.find_all(['th', 'td'])
                if cells and not any(c.get_text(strip=True).lower() in ('ticker', 'symbol', 'name') for c in cells):
                    pass
                elif cells:
                    headers = [c.get_text(strip=True) for c in cells]
                    start = 1

            for row in tqdm(rows[start:], desc="iShares ETFs", unit="etf"):
                cells = row.find_all(['td', 'th'])
                row_data = [c.get_text(strip=True) for c in cells]
                if not any(row_data):
                    continue

                links = row.find_all('a', href=True)
                fund_url = None
                for link in links:
                    href = link.get('href', '')
                    if '/us/products/' in href or '/funds/' in href:
                        fund_url = href if href.startswith('http') else f"{self.base_url}{href}"
                        break

                etf_data = self._parse_row_data(row_data, headers, fund_url=fund_url)
                if etf_data.get('ticker') or etf_data.get('name'):
                    etfs.append(etf_data)

            if etfs:
                return etfs

        return etfs

    def _parse_row_data(self, row_data: List[str], headers: List[str], *, fund_url: Optional[str] = None) -> Dict:
        """Parse a table row into ETF dictionary."""
        etf = {
            'ticker': '',
            'name': '',
            'asset_class': None,
            'expense_ratio': None,
            'total_net_assets': None,
            'ytd_return': None,
            'one_year_return': None,
            'three_year_return': None,
            'five_year_return': None,
            'ten_year_return': None,
            'inception_date': None,
            'primary_benchmark': None,
            'fund_url': fund_url,
            'created_at': datetime.now().isoformat(),
            'updated_at': datetime.now().isoformat(),
        }

        # Try to find ticker symbol (usually first or second column, often all caps, 2-5 chars)
        for i, cell_text in enumerate(row_data):
            cell_text_clean = cell_text.upper().strip()
            # Ticker patterns: 2-5 uppercase letters, possibly with numbers
            if cell_text_clean and len(cell_text_clean) <= 5 and cell_text_clean.isalnum():
                etf['ticker'] = cell_text_clean
                break

        # Try to find name (usually contains more text, not just ticker)
        for i, cell_text in enumerate(row_data):
            if cell_text and len(cell_text) > 5 and cell_text != etf['ticker']:
                etf['name'] = cell_text
                break

        # Map headers to fields if available
        if headers:
            header_mapping = {
                'ticker': ['ticker', 'symbol', 'ticker symbol'],
                'name': ['name', 'fund name', 'etf name', 'product name'],
                'asset_class': ['asset class', 'category'],
                'expense_ratio': ['expense ratio', 'exp ratio', 'gross expense', 'net expense', 'fee'],
                'total_net_assets': ['total net assets', 'aum', 'assets', 'net assets', 'tna'],
                'ytd_return': ['ytd', 'ytd return', 'year to date'],
                'one_year_return': ['1 year', '1yr', 'one year', '1-year'],
                'three_year_return': ['3 year', '3yr', 'three year', '3-year'],
                'five_year_return': ['5 year', '5yr', 'five year', '5-year'],
                'ten_year_return': ['10 year', '10yr', 'ten year', '10-year'],
                'inception_date': ['inception', 'incept', 'inception date', 'start date'],
                'primary_benchmark': ['benchmark', 'index', 'primary benchmark'],
            }

            for field, possible_names in header_mapping.items():
                for j, header in enumerate(headers):
                    header_lower = header.lower()
                    if any(name in header_lower for name in possible_names):
                        if j < len(row_data) and row_data[j]:
                            etf[field] = row_data[j]
                        break

        return etf
    
    def scrape_all_etfs(self, url: Optional[str] = None, csv_path: Optional[str] = None) -> List[Dict]:
        """Scrape all ETFs from iShares website and optionally save to CSV."""
        if url is None:
            url = "https://www.ishares.com/us/products/etf-investments#/?productView=etf&pageNumber=1&sortColumn=totalNetAssets&sortDirection=desc&dataView=keyFacts&style=44342"

        out_csv = None
        if csv_path:
            p = Path(csv_path)
            out_csv = p if p.suffix.lower() == '.csv' else p / 'ishares_etfs.csv'
            out_csv.parent.mkdir(parents=True, exist_ok=True)

        etfs = self._scrape_via_html(url)
        if not etfs:
            return []

        if out_csv:
            pd.DataFrame(etfs).to_csv(out_csv, index=False)

        return etfs

    def _get_holdings_csv_url(self, fund_url: str) -> Optional[str]:
        """Fetch fund page and extract the holdings CSV download URL."""
        try:
            response = self._get_with_retry(fund_url, timeout=30)
            m = re.search(r'(/us/products/\d+/[^\s"\']+\.ajax\?fileType=csv[^"\'\s]+)', response.text)
            if m:
                path = m.group(1)
                if not path.startswith('http'):
                    return f"{self.base_url}{path}"
                return path
        except Exception as e:
            warnings.warn(f"_get_holdings_csv_url failed: {e}", UserWarning, stacklevel=2)
            raise
        return None

    def _fetch_holdings_csv(self, csv_url: str) -> Tuple[List[Dict], List[Dict]]:
        """
        Fetch holdings CSV and parse into two tables: summary (first) and detailed (second).
        iShares CSVs have a summary holdings table, then fund meta rows, then a detailed
        holdings table with a second header row. Returns (summary_rows, detailed_rows).
        """
        def _row_to_dict(header: List[str], row: List[str]) -> Dict:
            padded = (row + [""] * len(header))[: len(header)]
            return dict(zip(header, padded))

        try:
            response = self._get_with_retry(csv_url, timeout=30)
            text = response.text
            if text.startswith("\ufeff"):
                text = text[1:]
            lines = text.strip().split("\n")
            start_i = None
            for i, line in enumerate(lines):
                if "Name" in line and (
                    "Sector" in line or "Weight" in line or "Asset Class" in line
                ):
                    start_i = i
                    break
            if start_i is None:
                return [], []

            reader = csv.reader(io.StringIO("\n".join(lines[start_i:])))
            header1 = next(reader)
            summary: List[Dict] = []
            detailed: List[Dict] = []
            header2: Optional[List[str]] = None

            for row in reader:
                def _v(i: int) -> str:
                    return row[i].strip().strip('"') if i < len(row) else ""

                is_second_header = False
                for start in range(min(3, len(row))):
                    if _v(start) == "Name" and (_v(start + 1) == "Sector" or _v(start + 2) == "Asset Class"):
                        is_second_header = True
                        break
                if is_second_header:
                    header2 = row
                    break
                summary.append(_row_to_dict(header1, row))

            if header2 is not None:
                h2 = list(header2)
                if h2 and re.match(r"^[A-Z]{2,5}$", str(h2[0]).strip()):
                    h2[0] = "etf_ticker"
                for row in reader:
                    detailed.append(_row_to_dict(h2, row))

            return (summary, detailed)
        except Exception as e:
            warnings.warn(f"_fetch_holdings_csv failed: {e}", UserWarning, stacklevel=2)
            raise
        return [], []

    def _clean_holdings_df(self, df: pd.DataFrame, out_name: str) -> pd.DataFrame:
        """Drop header/footer rows, normalize dates and numerics. Mutates and returns df."""
        if "Name" in df.columns:
            df = df[df["Name"].astype(str).str.strip() != "Name"]
        if "Ticker" in df.columns:
            df = df[df["Ticker"].astype(str).str.strip() != "Name"]
        sector_col = "Sector" if "Sector" in df.columns else ("sector" if "sector" in df.columns else None)
        if sector_col is not None:
            df = df[df[sector_col].astype(str).str.strip() != "Sector"]
            s = df[sector_col].astype(str).str.strip()
            df = df[df[sector_col].notna() & (s != "") & (s.str.lower() != "nan")]
        df = df.replace({"-": "", "--": ""})
        for col in _HOLDINGS_DATE_COLUMNS:
            if col in df.columns:
                df[col] = _normalize_holdings_date_series(df[col], file_context=out_name)
        for col in _HOLDINGS_NUMERIC_COLUMNS:
            if col in df.columns:
                s = df[col].astype(str).str.strip().str.replace(",", "", regex=False).str.replace("%", "", regex=False)
                df[col] = pd.to_numeric(s, errors="coerce")
        return df

    def scrape_holdings(self, etfs: List[Dict], holdings_dir: Path, *, limit: Optional[int] = None) -> int:
        """
        For each ETF, scrape holdings and save two CSVs: summary and detailed.
        Writes to holdings/summary/{ticker}_holdings.csv and holdings/detailed/{ticker}_holdings.csv.
        """
        summary_dir = holdings_dir / "summary"
        detailed_dir = holdings_dir / "detailed"
        summary_dir.mkdir(parents=True, exist_ok=True)
        detailed_dir.mkdir(parents=True, exist_ok=True)
        etfs_to_scrape = etfs[:limit] if limit else etfs
        saved = 0
        for etf in tqdm(etfs_to_scrape, desc="Holdings", unit="etf"):
            fund_url = etf.get("fund_url")
            ticker = etf.get("ticker", "")
            if not fund_url or not ticker:
                print(f"Holdings skip {ticker or 'unknown'}: missing fund_url or ticker", file=sys.stderr)
                continue
            csv_url = self._get_holdings_csv_url(fund_url)
            if not csv_url:
                print(f"Holdings skip {ticker}: no holdings CSV URL found on fund page", file=sys.stderr)
                continue
            summary_rows, detailed_rows = self._fetch_holdings_csv(csv_url)
            if not summary_rows and not detailed_rows:
                print(f"Holdings skip {ticker}: empty or unparseable holdings CSV", file=sys.stderr)
                continue
            out_name = f"{ticker}_holdings.csv"
            if summary_rows:
                df1 = pd.DataFrame(summary_rows)
                if "etf_ticker" not in df1.columns:
                    df1.insert(0, "etf_ticker", ticker)
                df1 = self._clean_holdings_df(df1, out_name)
                df1.to_csv(summary_dir / out_name, index=False)
            if detailed_rows:
                df2 = pd.DataFrame(detailed_rows)
                if "etf_ticker" not in df2.columns:
                    df2.insert(0, "etf_ticker", ticker)
                df2 = self._clean_holdings_df(df2, out_name)
                df2.to_csv(detailed_dir / out_name, index=False)
            saved += 1
        return saved

