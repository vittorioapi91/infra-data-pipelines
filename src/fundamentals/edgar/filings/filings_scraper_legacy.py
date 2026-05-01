"""
Scraper for early EDGAR SGML / PEM‑wrapped text filings (no HTML, no XBRL).

These legacy filings typically look like:

-----BEGIN PRIVACY-ENHANCED MESSAGE-----
...
<IMS-DOCUMENT>...
<IMS-HEADER>...
...
</IMS-HEADER>
<DOCUMENT>
<TYPE>10-Q
<SEQUENCE>1
<DESCRIPTION>10-Q
<TEXT>
... plain ASCII body, including ASCII tables with <TABLE>/<CAPTION>/<S>/<C> tags ...
</TEXT>
</DOCUMENT>
"""

import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Union


class FilingsScraperLegacyText:
    """
    Extract header metadata, plain‑text documents, and best‑effort parsed
    tables from early EDGAR text filings.
    """

    def scrape_filing_text(
        self,
        text: str,
        form_type: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Parse legacy filing content already loaded in memory (e.g. from a zip member)."""
        lines = text.splitlines()

        # Strip legacy PEM wrapper if present: skip down to the first blank line
        # after the header block.
        start = 0
        if lines and lines[0].startswith("-----BEGIN PRIVACY-ENHANCED MESSAGE-----"):
            for i, line in enumerate(lines):
                if line.strip() == "" and i > 0:
                    start = i + 1
                    break
        body_lines = lines[start:]

        # Extract IMS header block between <IMS-HEADER> and </IMS-HEADER>
        header_lines: List[str] = []
        in_header = False
        for line in body_lines:
            if "<IMS-HEADER>" in line:
                in_header = True
                continue
            if "</IMS-HEADER>" in line:
                break
            if in_header:
                header_lines.append(line)

        def _get_field(prefix: str) -> Optional[str]:
            for ln in header_lines:
                if ln.strip().startswith(prefix):
                    return ln.split(":", 1)[1].strip()
            return None

        accession = _get_field("ACCESSION NUMBER")
        sub_type = _get_field("CONFORMED SUBMISSION TYPE") or form_type
        raw_period = _get_field("CONFORMED PERIOD OF REPORT")
        period = (
            datetime.strptime(raw_period.strip(), "%Y%m%d").strftime("%Y-%m-%d")
            if raw_period and raw_period.strip().isdigit() and len(raw_period.strip()) == 8
            else raw_period
        )

        company: Dict[str, Any] = {}
        for ln in header_lines:
            s = ln.strip()
            if s.startswith("COMPANY CONFORMED NAME:"):
                company["name"] = s.split(":", 1)[1].strip()
            elif s.startswith("CENTRAL INDEX KEY:"):
                company["cik"] = s.split(":", 1)[1].strip()
            elif s.startswith("STANDARD INDUSTRIAL CLASSIFICATION:"):
                company["sic"] = s.split(":", 1)[1].strip()
            elif s.startswith("STATE OF INCORPORATION:"):
                company["state_incorp"] = s.split(":", 1)[1].strip()

        # Parse <DOCUMENT> blocks; each may contain a <TEXT> section.
        documents: List[Dict[str, Any]] = []
        doc_lines: List[str] = []
        in_doc = False
        for ln in body_lines:
            if "<DOCUMENT>" in ln:
                in_doc = True
                doc_lines = []
                continue
            if "</DOCUMENT>" in ln and in_doc:
                parsed = self._parse_document_block(doc_lines, period=period)
                if parsed:
                    documents.append(parsed)
                in_doc = False
                continue
            if in_doc:
                doc_lines.append(ln)

        out: Dict[str, Any] = {
            "accession": accession,
            "form_type": sub_type,
            "period": period,
            "company": company,
        }
        if documents:
            out.update(documents[0])
        return out

    def scrape_filing(
        self,
        path: Union[str, Path],
        form_type: Optional[str] = None,
    ) -> Dict[str, Any]:
        p = Path(path).resolve()
        if not p.exists():
            raise FileNotFoundError(f"Filing not found: {p}")

        text = p.read_text(encoding="utf-8", errors="ignore")
        return self.scrape_filing_text(text, form_type=form_type)

    def _parse_document_block(
        self, lines: List[str], period: Optional[str] = None
    ) -> Dict[str, Any]:
        doc: Dict[str, Any] = {}
        text_lines: List[str] = []
        in_text = False
        for ln in lines:
            if "<TEXT>" in ln:
                in_text = True
                continue
            elif "</TEXT>" in ln:
                in_text = False
                continue
            elif in_text:
                text_lines.append(ln.rstrip("\n"))

        if not doc and not text_lines:
            return {}

        if text_lines:
            full_text = "\n".join(text_lines).strip()
            tables = self._extract_tables(full_text, period=period)
            if tables:
                doc.update(tables)
        return doc

    def _extract_tables(
        self, text: str, period: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Extract best‑effort numeric tables from the TEXT body.

        We treat any <TABLE> ... </TABLE> block as a candidate table. The
        pseudo‑caption for classification is inferred from a small window of
        context lines immediately preceding the <TABLE> tag, since many early
        filings omit explicit </CAPTION> tags.
        """
        lines = text.splitlines()
        tables: List[Dict[str, Any]] = []
        n = len(lines)
        i = 0
        while i < n:
            ln = lines[i]
            if "<TABLE" in ln:
                context_start = max(0, i - 5)
                context = [l.strip() for l in lines[context_start:i] if l.strip()]
                table_lines: List[str] = []
                i += 1
                while i < n and "</TABLE>" not in lines[i]:
                    table_lines.append(lines[i])
                    i += 1
                # skip the closing </TABLE> line
                while i < n and "</TABLE>" not in lines[i]:
                    i += 1
           	       # consumed at end of loop
                parsed = self._parse_table_block(context, table_lines, period=period)
                if parsed:
                    tables.append(parsed)
            i += 1

        # One table per statement type (first table of each kind only)
        grouped: Dict[str, Dict[str, Any]] = {}
        for tbl in tables:
            kind = tbl.pop("kind", "other")
            if kind == "other":
                continue
            if kind not in grouped:
                grouped[kind] = tbl
        return grouped

    _MONTH_NAMES = (
        "January", "February", "March", "April", "May", "June",
        "July", "August", "September", "October", "November", "December",
    )

    def _extract_date_column_headers(self, lines: List[str]) -> tuple[List[str], int, Optional[List[int]]]:
        """
        Detect date column headers in the first lines of a table (e.g. "December 31, 1993").
        Returns (date_keys, lines_to_skip, keep_column_indices).
        keep_column_indices: if set, only these column indices are kept (quarter-end only;
        six months ended columns are dropped). None = keep all columns.
        """
        # Pattern: "Month DD, YYYY" on same line
        full_pat = re.compile(
            r"(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{1,2}),?\s*(\d{4})",
            re.IGNORECASE,
        )
        # Pattern: "Month DD" (year on next line)
        month_day_pat = re.compile(
            r"(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{1,2}),?",
            re.IGNORECASE,
        )
        year_pat = re.compile(r"\b(19\d{2}|20\d{2})\b")
        # Detect "Six Months Ended" so we can keep only quarter (Three Months) columns
        six_months_pat = re.compile(r"six\s+months\s+ended", re.IGNORECASE)
        three_months_pat = re.compile(r"three\s+months\s+ended", re.IGNORECASE)

        def _has_three_and_six(lines_slice: List[str]) -> bool:
            text = " ".join(
                ln.replace("<S>", "").replace("</S>", "").replace("<C>", "").replace("</C>", "").strip()
                for ln in lines_slice
            )
            return bool(three_months_pat.search(text) and six_months_pat.search(text))

        for i, ln in enumerate(lines[:10]):
            clean = (
                ln.replace("<S>", "")
                .replace("</S>", "")
                .replace("<C>", "")
                .replace("</C>", "")
                .replace("<CAPTION>", "")
                .replace("</CAPTION>", "")
            ).strip()
            if not clean:
                continue
            # Try full "Month DD, YYYY" on this line
            full_matches = full_pat.findall(clean)
            if full_matches:
                date_keys = [f"{m[0]} {m[1]}, {m[2]}" for m in full_matches]
                keep: Optional[List[int]] = None
                if len(date_keys) >= 2 and _has_three_and_six(lines[max(0, i - 2) : i + 1]):
                    keep = list(range(len(date_keys) // 2))
                return (date_keys, 1, keep)
            # Try "Month DD" on this line and years on a following line (skip decorative ----- lines)
            month_day_matches = month_day_pat.findall(clean)
            if not month_day_matches or i + 1 >= len(lines):
                continue
            years: List[str] = []
            years_line_idx = i + 1
            for j in range(i + 1, min(i + 5, len(lines))):
                next_ln = (
                    lines[j]
                    .replace("<S>", "")
                    .replace("</S>", "")
                    .replace("<C>", "")
                    .replace("</C>", "")
                    .strip()
                )
                years = year_pat.findall(next_ln)
                if len(years) >= len(month_day_matches):
                    years_line_idx = j
                    break
            if not years or len(years) < len(month_day_matches):
                if len(month_day_matches) >= 1 and len(years) >= 1:
                    date_keys = [
                        f"{m[0]} {m[1]}, {years[min(k, len(years) - 1)]}"
                        for k, m in enumerate(month_day_matches)
                    ]
                    keep = None
                    if len(date_keys) >= 2 and _has_three_and_six(lines[max(0, i - 2) : i + 2]):
                        keep = list(range(len(date_keys) // 2))
                    return (date_keys, 2, keep)
                continue
            # Build one date key per column: if 4 years and 2 month-day groups, we have 4 columns
            if len(years) > len(month_day_matches):
                cols_per_period = len(years) // len(month_day_matches)
                date_keys = []
                for j, m in enumerate(month_day_matches):
                    for k in range(cols_per_period):
                        y_idx = j * cols_per_period + k
                        if y_idx < len(years):
                            date_keys.append(f"{m[0]} {m[1]}, {years[y_idx]}")
            else:
                date_keys = [
                    f"{m[0]} {m[1]}, {years[j]}"
                    for j, m in enumerate(month_day_matches)
                ]
            keep = None
            if len(date_keys) >= 2 and _has_three_and_six(lines[max(0, i - 2) : years_line_idx + 1]):
                keep = list(range(len(date_keys) // 2))
            lines_to_skip = years_line_idx - i + 1
            return (date_keys, lines_to_skip, keep)
        return ([], 0, None)

    def _parse_table_block(
        self,
        context: List[str],
        lines: List[str],
        period: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Parse a table block into a caption + rows. If date column headers are
        detected (e.g. "December 31, 1993"), row values are keyed by full date;
        otherwise by position (list). When period is set, only the column matching
        the filing period is kept (keyed by period YYYY-MM-DD).
        """
        caption = " ".join(context).strip()
        kind = self._infer_table_kind(caption)

        date_keys, date_header_skip, keep_column_indices = self._extract_date_column_headers(lines)
        if date_keys and keep_column_indices is not None:
            date_keys = [date_keys[j] for j in keep_column_indices]
        table_lines = lines[date_header_skip:]

        # Skip lines that are only date-like (month + day/year) so they are not parsed as data rows
        def _looks_like_date_header(line: str) -> bool:
            stripped = line.strip()
            if not stripped or "$" in stripped:
                return False
            month_day = re.compile(
                r"(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2}",
                re.IGNORECASE,
            )
            if not month_day.search(stripped):
                return False
            # Remove month DD and month DD, YYYY; remove years; remove (Note)/(Unaudited)
            rest = month_day.sub("", stripped)
            rest = re.sub(r"\d{4}", "", rest)
            rest = re.sub(r"\([^)]*\)", "", rest)
            rest = rest.replace(",", "").replace(" ", "").replace("-", "").replace(".", "")
            return len(rest) < 5

        # Build (indent, clean) for each line to preserve hierarchy (e.g. Inventories -> Raw materials).
        candidate_lines: List[tuple[int, str]] = []
        for ln in table_lines:
            clean = (
                ln.replace("<S>", "")
                .replace("</S>", "")
                .replace("<C>", "")
                .replace("</C>", "")
                .replace("<CAPTION>", "")
                .replace("</CAPTION>", "")
            ).rstrip()
            if not clean.strip():
                continue
            if _looks_like_date_header(clean):
                continue
            if date_keys and date_header_skip >= 1 and re.match(r"^\s*(\d{4}\s+)+\s*$", clean):
                continue
            indent = len(clean) - len(clean.lstrip())
            candidate_lines.append((indent, clean))

        # Resolve parent from indentation: header lines (no numbers) push a parent; data rows get key "Label [Parent]" when under a parent.
        parent_stack: List[tuple[int, str]] = []
        rows: List[Dict[str, Any]] = []
        for indent, clean in candidate_lines:
            parsed_row = self._parse_numeric_row(clean)
            if parsed_row is not None:
                while parent_stack and parent_stack[-1][0] >= indent:
                    parent_stack.pop()
                parent_label = parent_stack[-1][1] if parent_stack else None
                key = f"{parsed_row['label']} [{parent_label}]" if parent_label else parsed_row["label"]
                rows.append({"key": key, "values": parsed_row["values"]})
            else:
                header_label = self._label_from_line(clean)
                if header_label:
                    while parent_stack and parent_stack[-1][0] >= indent:
                        parent_stack.pop()
                    parent_stack.append((indent, header_label))

        if not rows:
            return {}

        # Build row map: key -> values. If we have date columns, use date as key.
        # When keep_column_indices was applied, date_keys already contains only quarter columns.
        row_map: Dict[str, Any] = {}
        for r in rows:
            key = r["key"]
            values = r["values"]
            if date_keys and len(values) <= len(date_keys):
                by_date: Dict[str, Optional[float]] = {}
                for idx, dk in enumerate(date_keys):
                    by_date[dk] = values[idx] if idx < len(values) else None
                row_map[key] = by_date
            elif date_keys and len(values) > len(date_keys):
                # More values than date_keys: use first len(date_keys) (e.g. quarter-only already filtered)
                by_date = {}
                for idx in range(len(date_keys)):
                    by_date[date_keys[idx]] = values[idx] if idx < len(values) else None
                row_map[key] = by_date
            else:
                row_map[key] = values

        # Keep only the column matching the filing period (keyed by period YYYY-MM-DD)
        if period and row_map:
            filtered_row_map: Dict[str, Any] = {}
            for row_key, val in row_map.items():
                if isinstance(val, dict):
                    matched = None
                    for date_key, v in val.items():
                        try:
                            norm = datetime.strptime(date_key.strip(), "%B %d, %Y").strftime("%Y-%m-%d")
                        except ValueError:
                            norm = None
                        if norm == period:
                            matched = v
                            break
                    filtered_row_map[row_key] = matched
                else:
                    filtered_row_map[row_key] = val
            row_map = filtered_row_map

        table: Dict[str, Any] = {"kind": kind}
        table.update(row_map)
        return table

    def _infer_table_kind(self, caption: str) -> str:
        c = caption.lower()
        if "balance sheet" in c:
            return "balance_sheet"
        if "statements of operations" in c or "statement of operations" in c or "statements of income" in c:
            return "income_statement"
        if "cash flow" in c:
            return "cash_flow_statement"
        return "other"

    def _label_from_line(self, line: str) -> Optional[str]:
        """
        Normalise a line to a title-case label (no values). Used for parent
        context when a line has no numbers (e.g. "Inventories" above "Raw materials").
        """
        number_pattern = r"(-?\$?\s*\(?[0-9][0-9,]*\)?(?:\.[0-9]+)?)"
        first_num_match = re.search(number_pattern, line)
        if first_num_match:
            raw_label = line[: first_num_match.start()].strip(" .:\t")
        else:
            raw_label = line.strip(" .:\t")
        if not raw_label:
            return None
        label_compact = re.sub(r"\s+", " ", raw_label).rstrip(".:").strip()
        return label_compact.title() if label_compact else None

    def _parse_numeric_row(self, line: str) -> Optional[Dict[str, Any]]:
        """
        Parse a single table row into label + list of numeric values.

        We look for one or more numeric tokens at the end of the line and treat
        the leading text as the label.
        """
        # Require at least one digit so comma in "equipment, net" is not matched
        number_pattern = r"(-?\$?\s*\(?[0-9][0-9,]*\)?(?:\.[0-9]+)?)"
        nums = re.findall(number_pattern, line)
        if not nums:
            return None

        first_num_match = re.search(number_pattern, line)
        if not first_num_match:
            return None
        # Take the leading text before the first numeric token as the label and
        # normalise it into a compact, title‑cased key (e.g. "Net margin" ->
        # "Net Margin").
        raw_label = line[: first_num_match.start()].strip(" .:\t")
        if not raw_label:
            return None

        # Collapse internal whitespace and strip trailing punctuation.
        label_compact = re.sub(r"\s+", " ", raw_label)
        label_compact = label_compact.rstrip(".:")
        if not label_compact:
            return None
        label = label_compact.title()

        def _to_number(raw: str) -> Optional[float]:
            s = raw.strip().replace("$", "").replace(",", "")
            negative = False
            if s.startswith("(") and s.endswith(")"):
                negative = True
                s = s[1:-1]
            try:
                if "." in s:
                    val = float(s)
                else:
                    val = float(int(s))
                return -val if negative else val
            except ValueError:
                return None

        values: List[Optional[float]] = []
        for n in nums:
            v = _to_number(n)
            values.append(v)

        return {
            "label": label,
            "values": values,
        }

