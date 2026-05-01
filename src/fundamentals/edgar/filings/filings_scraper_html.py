"""
Scrape SEC EDGAR filings: HTML/narrative content and guidance.

FilingsScraperHTML extracts HTML/narrative content from all DOCUMENT blocks
(including those inside XBRL). Guidance extraction uses heuristics.
"""

import html
import json
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from .filings_scraper_html_heuristics import extract_guidance as _extract_guidance_heuristics

# Heuristics for guidance detection (which documents to process)
# Loaded from rules-html/8k_guidance-detection.yaml
def _load_guidance_detection():
    from ..rules_loader import load_guidance_detection

    desc, content_kw, pats = load_guidance_detection()
    if not desc and not content_kw and not pats:
        raise RuntimeError(
            "EDGAR guidance detection rules could not be loaded. "
            "Ensure rules-html/8k_guidance-detection.yaml exists and PyYAML is installed."
        )
    return desc, content_kw, pats


_GUIDANCE_DESC, _GUIDANCE_CONTENT_KW, _GUIDANCE_CONTENT_PATS = _load_guidance_detection()
GUIDANCE_DESCRIPTION_KEYWORDS = _GUIDANCE_DESC
GUIDANCE_CONTENT_KEYWORDS = _GUIDANCE_CONTENT_KW
GUIDANCE_CONTENT_PATTERNS = _GUIDANCE_CONTENT_PATS

_NARRATIVE_START = re.compile(
    r"UNITED\s+STATES\s+SECURITIES\s+AND\s+EXCHANGE|"
    r"U\.S\.\s+SECURITIES\s+AND\s+EXCHANGE|"
    r"FORM\s+10-[KQ]|FORM\s+8-K|"
    r"TABLE\s+OF\s+CONTENTS|"
    r"PART\s+[IV]+\s|"
    r"ITEM\s+1\.|"
    r"Exhibit\s+99",
    re.I,
)

_SECTION_PATTERN = re.compile(
    r"ITEM\s+(\d+[A-Z]?)\s*[.\-]?\s*[A-Z][A-Z0-9\s\&'\u2019\-]{0,70}(?=\s+[A-Z][a-z])",
    re.I,
)


def _strip_html(text: str) -> str:
    """Remove HTML tags and decode entities."""
    no_tags = re.sub(r"<[^>]+>", " ", str(text))
    return html.unescape(re.sub(r"\s+", " ", no_tags)).strip()


def _meaningful_snippet(text: str, max_len: int = 500) -> str:
    """Compute snippet from first narrative content, skipping iXBRL metadata."""
    if not text:
        return ""
    start = 0
    m = _NARRATIVE_START.search(text)
    if m:
        start = m.start()
    snippet_len = min(max_len, len(text) - start)
    snippet = text[start : start + snippet_len].strip()
    return (snippet + "…") if len(text) > start + snippet_len else snippet


def _extract_sections(text: str) -> Dict[str, str]:
    """Extract ITEM sections from 10-K/10-Q narrative text."""
    sections: Dict[str, str] = {}
    matches = list(_SECTION_PATTERN.finditer(text))
    for i, m in enumerate(matches):
        item_id = m.group(1).upper().replace(" ", "")
        start = m.end()
        end = matches[i + 1].start() if i + 1 < len(matches) else len(text)
        content = text[start:end].strip()
        if content:
            key = f"item_{item_id.lower()}"
            sections[key] = content
    return sections


class FilingsScraperHTML:
    """
    Scraper for extracting HTML/narrative content from SEC EDGAR filings.

    Handles plain HTML, exhibits (e.g. press releases, investor presentations),
    and narrative text that is not XBRL-tagged (e.g. forward guidance).

    Guidance extraction uses rule-based heuristics.
    """

    def scrape_filing_text(
        self,
        raw: str,
        *,
        path_label: Union[str, Path],
        write_sidecar: bool = False,
        form_type: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Parse filing HTML/narrative from raw text (e.g. zip member bytes decoded in memory).

        Args:
            raw: Full filing text.
            path_label: Logical source label stored in output (e.g. zip path + member name).
            write_sidecar: If True, also write ``{stem}.html.json`` next to ``path_label`` when it is a filesystem path.
            form_type: Accepted for API compatibility with other scrapers; unused here.
        """
        _ = form_type
        documents: List[Dict[str, Any]] = []

        for block in self._parse_document_blocks(raw):
            doc_type = (block.get("type") or "").strip().upper()
            if doc_type in ("GRAPHIC", "XML", "JSON", "ZIP", "PDF", "EXCEL"):
                continue
            text_raw = block.get("text") or ""
            text_plain = _strip_html(text_raw)
            desc_lower = (block.get("description") or "").lower()
            content_lower = text_plain.lower()
            has_guidance = self._has_guidance(desc_lower, content_lower)
            snippet = _meaningful_snippet(text_plain)
            doc_entry: Dict[str, Any] = {
                "type": block.get("type", ""),
                "filename": block.get("filename", ""),
                "description": block.get("description", ""),
                "text_plain": text_plain,
                "content_snippet": snippet,
            }
            sections = _extract_sections(text_plain)
            if sections:
                doc_entry["sections"] = sections
            if has_guidance:
                doc_entry["content"] = "guidance"
                guidance = _extract_guidance_heuristics(text_plain)
                if guidance["revenue"] or guidance["earnings"] or guidance["margins"]:
                    doc_entry["guidance"] = guidance
            documents.append(doc_entry)

        label = str(path_label)
        content: Dict[str, Any] = {"path": label, "documents": documents}
        if write_sidecar:
            path = Path(path_label).resolve()
            out_path = path.parent / f"{path.stem}.html.json"
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(content, f, indent=2, default=str)
        return content

    def scrape_filing(
        self,
        path: Union[str, Path],
        form_type: Optional[str] = None,
        write_sidecar: bool = True,
    ) -> Dict[str, Any]:
        """
        Parse a SEC filing and extract HTML/narrative content from all HTML blocks.

        Processes DOCUMENT blocks whether wrapped in <XBRL> or not.
        Applies heuristics to flag documents likely containing forward guidance.
        When write_sidecar is True, saves output as stem.html.json in the same directory as the input file.

        Args:
            path: Path to the filing file (.htm, .html, .txt).
            form_type: Accepted for API compatibility with other scrapers; unused here.
            write_sidecar: If True, write ``{stem}.html.json`` alongside the filing.

        Returns:
            Dict with path and documents list.
        """
        path = Path(path).resolve()
        if not path.exists():
            raise FileNotFoundError(f"Filing not found: {path}")

        raw = path.read_text(encoding="utf-8", errors="replace")
        return self.scrape_filing_text(
            raw,
            path_label=path,
            write_sidecar=write_sidecar,
            form_type=form_type,
        )

    def _parse_document_blocks(self, raw: str) -> List[Dict[str, Any]]:
        """Split raw filing into DOCUMENT blocks and parse headers + TEXT."""
        blocks: List[Dict[str, Any]] = []
        parts = re.split(r"<DOCUMENT>", raw, flags=re.I)
        for part in parts[1:]:
            end = part.find("</DOCUMENT>")
            if end < 0:
                continue
            block_raw = part[:end]
            info: Dict[str, Any] = {}
            for tag in ("TYPE", "SEQUENCE", "FILENAME", "DESCRIPTION"):
                m = re.search(rf"<{tag}>(.*?)(?=<[A-Z]|$)", block_raw, re.I | re.S)
                info[tag.lower()] = (m.group(1).strip() if m else "")
            text_m = re.search(r"<TEXT>(.*)</TEXT>", block_raw, re.I | re.S)
            text = text_m.group(1) if text_m else ""
            info["text"] = text
            info["is_xbrl"] = "<XBRL>" in text and "</XBRL>" in text
            blocks.append(info)
        return blocks

    def _has_guidance(self, description_lower: str, content_lower: str) -> bool:
        """Apply heuristics to detect forward guidance."""
        desc_match = any(kw in description_lower for kw in GUIDANCE_DESCRIPTION_KEYWORDS)
        kw_match = any(kw in content_lower for kw in GUIDANCE_CONTENT_KEYWORDS)
        pat_match = any(p.search(content_lower) for p in GUIDANCE_CONTENT_PATTERNS)
        return desc_match or kw_match or pat_match
