"""
Unit tests for EDGAR filings scrapers (iXBRL with Arelle, HTML/narrative)
"""

import zipfile

import pytest
from pathlib import Path
import sys

# Add project root to path for imports
project_root = Path(__file__).parent.parent.parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from src.fundamentals.edgar.filings.filings_scraper import (
    FilingsScraperInlineXBRL,
    FilingsScraperHTML,
    FilingScraperDispatcher,
)
from src.fundamentals.edgar.edgar import (
    _scrape_zip_member,
    get_edgar_arelle_temp_dir,
)

# Versioned test fixtures (available in CI/Jenkins)
SAMPLE_10K = Path(__file__).parent / "data" / "sample_10k.txt"
SAMPLE_10Q = Path(__file__).parent / "data" / "sample_10q.txt"
SAMPLE_8K = Path(__file__).parent / "data" / "sample_8k.txt"


@pytest.fixture
def scraper_html():
    """Create FilingsScraperHTML instance."""
    return FilingsScraperHTML()


class TestFilingsScraperInlineXBRL:
    """Minimal tests for FilingsScraperInlineXBRL (direct usage)."""

    def test_scrape_filing_file_not_found(self):
        """Test FileNotFoundError when filing path does not exist."""
        scraper_xbrl = FilingsScraperInlineXBRL(apply_filters=False)
        with pytest.raises(FileNotFoundError, match="Filing not found"):
            scraper_xbrl.scrape_filing("/nonexistent/path/to/filing.txt")


class TestFilingScraperDispatcher:
    """Classifier/dispatcher tests: choose appropriate scraper automatically."""

    def _all_facts(self, by_stmt):
        return (
            by_stmt.get("balance_sheet", [])
            + by_stmt.get("income_statement", [])
            + by_stmt.get("cash_flow", [])
            + by_stmt.get("other", [])
        )

    def test_dispatcher_chooses_ixbrl_for_10k(self):
        """Dispatcher should treat sample_10k as inline XBRL and return facts."""
        disp = FilingScraperDispatcher()
        by_stmt = disp.scrape_filing(SAMPLE_10K)
        assert isinstance(by_stmt, dict)
        facts = self._all_facts(by_stmt)
        assert len(facts) > 0
        assert "concept" in facts[0] and "value" in facts[0]

    def test_dispatcher_chooses_legacy_for_pem_ims_text(self, tmp_path: Path):
        """
        Dispatcher should route legacy PEM/IMS SGML filings to the legacy scraper.

        We synthesise a minimal example that looks like early EDGAR text filings.
        """
        sample = tmp_path / "legacy_10q.txt"
        sample.write_text(
            "-----BEGIN PRIVACY-ENHANCED MESSAGE-----\n"
            "Proc-Type: 2001,MIC-CLEAR\n"
            "\n"
            "<IMS-DOCUMENT>0000000000-00-000000.txt : 19930101\n"
            "<IMS-HEADER>0000000000-00-000000.hdr.sgml : 19930101\n"
            "ACCESSION NUMBER:\t0000000000-00-000000\n"
            "CONFORMED SUBMISSION TYPE:\t10-Q\n"
            "CONFORMED PERIOD OF REPORT:\t19921231\n"
            "FILED AS OF DATE:\t19930115\n"
            "\n"
            "\tCOMPANY DATA:\n"
            "\t\tCOMPANY CONFORMED NAME:\tTEST COMPANY INC\n"
            "\t\tCENTRAL INDEX KEY:\t0000000000\n"
            "\t\tSTANDARD INDUSTRIAL CLASSIFICATION:\t1311\n"
            "\t\tSTATE OF INCORPORATION:\tDE\n"
            "\t\tFISCAL YEAR END:\t1231\n"
            "</IMS-HEADER>\n"
            "<DOCUMENT>\n"
            "<TYPE>10-Q\n"
            "<SEQUENCE>1\n"
            "<DESCRIPTION>10-Q\n"
            "<TEXT>\n"
            "TEST BODY\n"
            "</TEXT>\n"
            "</DOCUMENT>\n",
            encoding="utf-8",
        )

        disp = FilingScraperDispatcher()
        data = disp.scrape_filing(sample, form_type="10-Q")
        assert disp.classify(sample) == "legacy"
        assert data["form_type"] == "10-Q"
        assert data["company"]["name"] == "TEST COMPANY INC"
        assert data["accession"] == "0000000000-00-000000"
        # Legacy scraper merges first <DOCUMENT> block into the top-level dict (not a "documents" list).

    def test_classify_text_matches_read_path(self, tmp_path: Path) -> None:
        """In-memory classify_text should agree with on-disk classify for the same bytes."""
        sample = tmp_path / "x.txt"
        sample.write_text(
            "-----BEGIN PRIVACY-ENHANCED MESSAGE-----\n\n"
            "<IMS-HEADER>\nACCESSION NUMBER:\t0000000000-00-000000\n</IMS-HEADER>\n",
            encoding="utf-8",
        )
        disp = FilingScraperDispatcher()
        text = sample.read_text(encoding="utf-8")
        assert disp.classify_text(text) == disp.classify(sample)


class TestZipMemberScrapeLegacyHtml:
    """--filings-zip worker: copy member to RAMDISK, then same pipeline as on-disk .txt."""

    def test_scrape_zip_ixbrl_with_ramdisk_calls_pipeline(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """With EDGAR_ARELLE_TEMP_DIR set, iXBRL zip members delegate to _scrape_one_filing and remove temp."""
        ram = tmp_path / "ram"
        ram.mkdir()
        monkeypatch.setenv("EDGAR_ARELLE_TEMP_DIR", str(ram))
        ixbrl_stub = (
            'http://www.xbrl.org/2013/inlineXBRL\n'
            "<html><body><ix:nonFraction>1</ix:nonFraction></body></html>\n"
        )
        zpath = tmp_path / "x.zip"
        with zipfile.ZipFile(zpath, "w") as zf:
            zf.writestr("0000000000-00-000002.txt", ixbrl_stub)
        out_dir = tmp_path / "out3"
        out_dir.mkdir()
        scrape_log = tmp_path / "scrape3.log"
        check_log = tmp_path / "check3.log"
        args = (
            str(zpath),
            "0000000000-00-000002.txt",
            str(out_dir),
            "10-Q",
            {},
            "",
            {},
            False,
            str(scrape_log),
            str(check_log),
        )
        seen: list = []

        def fake_one(a: tuple) -> tuple:
            seen.append(a)
            return (1, 0, 1, 0, None, 0)

        monkeypatch.setattr(
            "src.fundamentals.edgar.edgar._scrape_one_filing",
            fake_one,
        )
        result = _scrape_zip_member(args)
        assert result == (1, 0, 1, 0, None, 0)
        assert len(seen) == 1
        assert seen[0][9] == "0000000000-00-000002"
        assert list(ram.iterdir()) == []

    def test_get_edgar_arelle_temp_dir_none_when_unset(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("EDGAR_ARELLE_TEMP_DIR", raising=False)
        monkeypatch.delenv("RAMDISK", raising=False)
        assert get_edgar_arelle_temp_dir() is None

    def test_get_edgar_arelle_temp_dir_resolves(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("EDGAR_ARELLE_TEMP_DIR", str(tmp_path))
        assert get_edgar_arelle_temp_dir() == tmp_path.resolve()


class TestFilingsScraperHTML:
    """Tests for FilingsScraperHTML class"""

    def test_scrape_filing_file_not_found(self, scraper_html):
        """Test FileNotFoundError when filing path does not exist."""
        with pytest.raises(FileNotFoundError, match="Filing not found"):
            scraper_html.scrape_filing("/nonexistent/path/to/filing.txt")

    def test_scrape_filing_returns_dict(self, scraper_html):
        """Test scraping returns dict with expected keys."""
        content = scraper_html.scrape_filing(SAMPLE_8K)
        assert isinstance(content, dict)
        assert "path" in content
        assert "documents" in content

    def test_scrape_filing_includes_all_html_blocks(self, scraper_html):
        """Test that all HTML blocks are processed (including XBRL-wrapped)."""
        content = scraper_html.scrape_filing(SAMPLE_8K)
        docs = content["documents"]
        # sample_8k: 8-K (XBRL), EX-99.1, EX-99.2 - all included
        assert len(docs) >= 2
        types = [d["type"] for d in docs]
        assert "EX-99.1" in types
        assert "EX-99.2" in types

    def test_scrape_filing_ex99_1_has_guidance(self, scraper_html):
        """Test that EX-99.1 press release is flagged with content=guidance."""
        content = scraper_html.scrape_filing(SAMPLE_8K)
        ex99_1 = next(d for d in content["documents"] if d["type"] == "EX-99.1")
        assert ex99_1.get("content") == "guidance"
        assert "guidance" in ex99_1["text_plain"].lower()

    def test_scrape_filing_extracts_revenue_earnings_guidance(self, scraper_html):
        """Test that revenue and earnings guidance are extracted."""
        content = scraper_html.scrape_filing(SAMPLE_8K)
        ex99_1 = next(d for d in content["documents"] if d["type"] == "EX-99.1")
        guidance = ex99_1.get("guidance")
        assert guidance is not None
        # Full year 2026 revenue guidance: $540-$555 million
        rev = [r for r in guidance.get("revenue", []) if r.get("period") == "FY2026"]
        assert len(rev) >= 1
        assert rev[0].get("low") == 540.0 and rev[0].get("high") == 555.0
        # Full year 2026 earnings guidance: adjusted net income positive
        earn = [e for e in guidance.get("earnings", []) if e.get("period") == "FY2026"]
        assert len(earn) >= 1
        assert earn[0].get("qualitative") == "positive"
