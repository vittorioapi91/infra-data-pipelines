"""
Scrape SEC EDGAR filings: XBRL facts, HTML/narrative content, and legacy SGML text.

Re-exports scrapers from dedicated modules:
- FilingsScraperInlineXBRL from filings_scraper_xbrl
- FilingsScraperHTML from filings_scraper_html
- FilingsScraperLegacyText from filings_scraper_legacy

And exposes a dispatcher that chooses the appropriate scraper automatically
based on the filing's on-disk content.
"""

from pathlib import Path
from typing import Any, Dict, Optional, Union

from .filings_scraper_xbrl import FilingsScraperInlineXBRL
from .filings_scraper_html import (
    FilingsScraperHTML,
    GUIDANCE_CONTENT_KEYWORDS,
    GUIDANCE_CONTENT_PATTERNS,
    GUIDANCE_DESCRIPTION_KEYWORDS,
)
from .filings_scraper_legacy import FilingsScraperLegacyText


class FilingScraperDispatcher:
    """
    Dispatch to the appropriate scraper based on file content:

    - Inline XBRL (iXBRL)
    - HTML / narrative content
    - Legacy SGML / PEM‑wrapped text
    """

    def __init__(self) -> None:
        self._ixbrl = FilingsScraperInlineXBRL()
        self._html = FilingsScraperHTML()
        self._legacy = FilingsScraperLegacyText()

    def _classify_head_lower(self, head_lower: str) -> str:
        """Classify from the first ~200k chars, already lowercased."""
        # Inline XBRL check: namespace + ix:* tags.
        if (
            "http://www.xbrl.org/2008/inlinexbrl" in head_lower
            or "http://www.xbrl.org/2013/inlinexbrl" in head_lower
        ) and "<ix:" in head_lower:
            return "ixbrl"

        # Legacy PEM/IMS SGML filings.
        if head_lower.startswith("-----begin privacy-enhanced message-----") or "<ims-header>" in head_lower:
            return "legacy"

        # HTML-ish filings (modern 10-K/10-Q/8-K without inline XBRL).
        if "<html" in head_lower or "<document>" in head_lower:
            return "html"

        # Fallback: treat as legacy text.
        return "legacy"

    def _classify(self, path: Path) -> str:
        """
        Best‑effort classification based on the first chunk of the file.
        """
        with path.open("r", encoding="utf-8", errors="ignore") as f:
            head = f.read(200_000).lower()
        return self._classify_head_lower(head)

    def classify(self, path: Union[str, Path]) -> str:
        """
        Public wrapper around the classifier, accepting string or Path.
        """
        return self._classify(Path(path).resolve())

    def classify_text(self, text: str) -> str:
        """
        Classify from in-memory filing text (e.g. a member read from a zip archive).
        Uses the first 200k characters, matching on-disk classification behavior.
        """
        return self._classify_head_lower(text[:200_000].lower())

    def create_scraper(
        self,
        path: Union[str, Path],
    ) -> tuple[str, Any]:
        """
        Factory method: given a filing path, return (kind, scraper_instance),
        where kind is one of: "ixbrl", "html", "legacy".
        """
        p = Path(path).resolve()
        kind = self._classify(p)
        if kind == "ixbrl":
            return kind, self._ixbrl
        if kind == "html":
            return kind, self._html
        return "legacy", self._legacy

    def create_scraper_from_text(self, text: str) -> tuple[str, Any]:
        """
        Return (kind, scraper) using classify_text — for in-memory content (no Path).
        """
        kind = self.classify_text(text)
        if kind == "ixbrl":
            return kind, self._ixbrl
        if kind == "html":
            return kind, self._html
        return "legacy", self._legacy

    def scrape_filing(
        self,
        path: Union[str, Path],
        form_type: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Convenience method: factory + scrape in one call.
        """
        p = Path(path).resolve()
        kind, scraper = self.create_scraper(p)
        if kind == "ixbrl":
            return scraper.scrape_filing(p, form_type=form_type)
        if kind == "html":
            return scraper.scrape_filing(p, form_type=form_type)
        return scraper.scrape_filing(p, form_type=form_type)


__all__ = [
    "FilingsScraperInlineXBRL",
    "FilingsScraperHTML",
    "FilingsScraperLegacyText",
    "FilingScraperDispatcher",
    "GUIDANCE_DESCRIPTION_KEYWORDS",
    "GUIDANCE_CONTENT_KEYWORDS",
    "GUIDANCE_CONTENT_PATTERNS",
]

