"""
Scrape the SEC Standard Industrial Classification (SIC) code list and write CSV.

Standalone script: run directly from this directory or as module:
  python -m src.fundamentals.edgar.codes.sic_codes
  python src/fundamentals/edgar/codes/sic_codes.py

Output: storage/{ENV}/fundamentals/edgar/codes/sic_codes.csv
Source: https://www.sec.gov/search-filings/standard-industrial-classification-sic-code-list
"""

from __future__ import annotations

import csv
import os
import re
import sys
from pathlib import Path

import requests
from bs4 import BeautifulSoup

URL = "https://www.sec.gov/search-filings/standard-industrial-classification-sic-code-list"

# SEC requires a descriptive User-Agent (use EDGAR_USER_AGENT from env, e.g. in .env.dev)
DEFAULT_USER_AGENT = "VittorioApicella apicellavittorio@hotmail.it"


def _get_storage_path() -> Path:
    """Return storage/{ENV}/fundamentals/edgar/codes directory.

    Uses TRADING_AGENT_STORAGE as a common storage root (without env) when set.
    """
    project_root = Path(__file__).resolve().parent.parent.parent.parent.parent
    storage_root = os.getenv("TRADING_AGENT_STORAGE")
    env = os.getenv("ENV", "dev")
    if storage_root:
        return Path(storage_root) / env / "fundamentals" / "edgar" / "codes"
    return project_root / "storage" / env / "fundamentals" / "edgar" / "codes"


def _fetch_sic_page(*, user_agent: str | None = None) -> str:
    """Fetch the SEC SIC code list HTML page."""
    headers = {
        "User-Agent": user_agent or os.getenv("EDGAR_USER_AGENT", DEFAULT_USER_AGENT),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Host": "www.sec.gov",
    }
    resp = requests.get(URL, headers=headers, timeout=30)
    resp.raise_for_status()
    return resp.text


def _parse_sic_table(html: str) -> list[tuple[str, str, str]]:
    """
    Parse the SIC table from SEC HTML. Returns list of (sic_code, office, industry_title).
    """
    soup = BeautifulSoup(html, "html.parser")
    tables = soup.find_all("table")
    for table in tables:
        rows = table.find_all("tr")
        if not rows:
            continue
        # First row may be header
        first_cells = [c.get_text(strip=True) for c in rows[0].find_all(["th", "td"])]
        if not first_cells:
            continue
        # Check if this looks like the SIC table (SIC Code, Office, Industry Title)
        if "sic" in first_cells[0].lower() and len(first_cells) >= 3:
            data_start = 1
        elif re.match(r"^\d+$", first_cells[0].strip()):
            # First row is data (numeric SIC)
            data_start = 0
        else:
            continue
        result = []
        for tr in rows[data_start:]:
            cells = tr.find_all(["td", "th"])
            texts = [c.get_text(strip=True) for c in cells]
            if len(texts) >= 3:
                result.append((texts[0], texts[1], texts[2]))
            elif len(texts) == 2:
                result.append((texts[0], texts[1], ""))
            elif len(texts) == 1 and texts[0].strip():
                result.append((texts[0], "", ""))
        if result:
            return result
    # Fallback: try markdown-style table in pre or raw text (some SEC pages)
    text = soup.get_text()
    # Pattern: | 100 | Industrial Applications... | AGRICULTURAL...
    md_row = re.compile(r"\|\s*(\d+)\s*\|\s*([^|]+)\|\s*([^|]+)\|")
    result = []
    for m in md_row.finditer(text):
        result.append((m.group(1).strip(), m.group(2).strip(), m.group(3).strip()))
    return result


def scrape_sic_codes() -> list[tuple[str, str, str]]:
    """Fetch SEC page and parse SIC table. Returns list of (sic_code, office, industry_title)."""
    html = _fetch_sic_page()
    rows = _parse_sic_table(html)
    if not rows:
        raise ValueError("No SIC table found on the SEC page; page structure may have changed.")
    return rows


def main() -> int:
    # Load env so EDGAR_USER_AGENT is set (e.g. from .env.dev)
    try:
        from src.config import load_environment_config
        load_environment_config()
    except Exception:
        try:
            from dotenv import load_dotenv
            project_root = Path(__file__).resolve().parent.parent.parent.parent.parent
            for name in (".env.dev", ".env"):
                p = project_root / name
                if p.exists():
                    load_dotenv(p, override=True)
                    break
        except Exception:
            pass

    out_dir = _get_storage_path()
    out_dir.mkdir(parents=True, exist_ok=True)
    out_csv = out_dir / "sic_codes.csv"

    try:
        rows = scrape_sic_codes()
    except Exception as e:
        print(f"Error scraping SIC codes: {e}", file=sys.stderr)
        return 1

    with out_csv.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["sic_code", "office", "industry_title"])
        w.writerows(rows)

    print(f"Wrote {len(rows)} SIC codes to {out_csv}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
