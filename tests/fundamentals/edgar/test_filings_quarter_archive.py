"""Tests for quarter filing zip archive helpers."""

from pathlib import Path

from src.fundamentals.edgar.form_type_path import form_type_filesystem_slug
from src.fundamentals.edgar.filings.filings_quarter_archive import (
    extract_quarter_filings_zip,
    parse_quarter_form_output_dir,
    quarter_filings_zip_path,
    zip_folder_and_remove,
)


def test_parse_quarter_form_output_dir_nested() -> None:
    p = (
        Path("storage")
        / "dev"
        / "fundamentals"
        / "edgar"
        / "filings"
        / "2024"
        / "QTR3"
        / "10-Q"
    )
    assert parse_quarter_form_output_dir(p) == ("2024", "QTR3", "10-Q")


def test_parse_quarter_form_output_dir_rejects_flat() -> None:
    p = Path("/data/edgar/filings")
    assert parse_quarter_form_output_dir(p) is None


def test_quarter_filings_zip_path() -> None:
    form = Path("x") / "filings" / "2024" / "QTR3" / "10-Q"
    expected = Path("x") / "filings" / "2024-QTR3-10-Q.zip"
    assert quarter_filings_zip_path(form) == expected


def test_form_type_filesystem_slug() -> None:
    assert form_type_filesystem_slug("10-K/A") == "10-K_A"
    assert form_type_filesystem_slug("10-Q") == "10-Q"
    assert form_type_filesystem_slug("8-K\\12") == "8-K_12"


def test_quarter_filings_zip_path_amended_form_dir() -> None:
    form = Path("x") / "filings" / "2024" / "QTR3" / "10-K_A"
    expected = Path("x") / "filings" / "2024-QTR3-10-K_A.zip"
    assert quarter_filings_zip_path(form) == expected


def test_zip_folder_and_remove(tmp_path: Path) -> None:
    form = tmp_path / "fundamentals" / "edgar" / "filings" / "2020" / "QTR1" / "10-K"
    form.mkdir(parents=True)
    (form / "f1.txt").write_text("hello", encoding="utf-8")
    zip_path = quarter_filings_zip_path(form)
    assert zip_path is not None
    assert zip_path == tmp_path / "fundamentals" / "edgar" / "filings" / "2020-QTR1-10-K.zip"
    zip_folder_and_remove(form, zip_path)
    assert zip_path.is_file()
    assert not form.exists()
    import zipfile

    with zipfile.ZipFile(zip_path) as zf:
        names = zf.namelist()
    assert "f1.txt" in names
    filings_root = tmp_path / "fundamentals" / "edgar" / "filings"
    qtr_dir = filings_root / "2020" / "QTR1"
    year_dir = filings_root / "2020"
    assert not qtr_dir.exists()
    assert not year_dir.exists()


def test_extract_quarter_filings_zip_restores_tree(tmp_path: Path) -> None:
    filings = tmp_path / "fundamentals" / "edgar" / "filings"
    form = filings / "2020" / "QTR1" / "10-K"
    form.mkdir(parents=True)
    (form / "0000000000-00-000001.txt").write_text("hello", encoding="utf-8")
    zip_path = quarter_filings_zip_path(form)
    assert zip_path is not None
    zip_folder_and_remove(form, zip_path)
    assert zip_path.is_file()
    n = extract_quarter_filings_zip(filings, "2020", "QTR1", "10-K")
    assert n == 1
    restored = filings / "2020" / "QTR1" / "10-K" / "0000000000-00-000001.txt"
    assert restored.read_text(encoding="utf-8") == "hello"


def test_extract_quarter_filings_zip_one_form_only(tmp_path: Path) -> None:
    filings = tmp_path / "fundamentals" / "edgar" / "filings"
    form_q = filings / "2020" / "QTR1" / "10-Q"
    form_q.mkdir(parents=True)
    (form_q / "x.txt").write_text("b", encoding="utf-8")
    zp = quarter_filings_zip_path(form_q)
    assert zp is not None
    zip_folder_and_remove(form_q, zp)
    n = extract_quarter_filings_zip(filings, "2020", "QTR1", "10-Q")
    assert n == 1
    assert (filings / "2020" / "QTR1" / "10-Q" / "x.txt").read_text() == "b"


def test_zip_folder_and_remove_keeps_quarter_if_other_form_has_txt(tmp_path: Path) -> None:
    filings = tmp_path / "fundamentals" / "edgar" / "filings" / "2020" / "QTR1"
    form_k = filings / "10-K"
    form_q = filings / "10-Q"
    form_k.mkdir(parents=True)
    form_q.mkdir(parents=True)
    (form_k / "a.txt").write_text("x", encoding="utf-8")
    (form_q / "b.txt").write_text("y", encoding="utf-8")
    zip_k = quarter_filings_zip_path(form_k)
    assert zip_k is not None
    zip_folder_and_remove(form_k, zip_k)
    assert zip_k.is_file()
    assert (form_q / "b.txt").is_file()
    assert filings.exists()
    assert filings.parent.exists()
