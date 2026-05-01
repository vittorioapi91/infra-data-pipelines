"""
Quarter filing zip path helpers.

Lives next to ``edgar.py`` (not under ``filings/``) so ``edgar`` can import
``quarter_filings_zip_path`` without executing ``filings/__init__.py``, which would
pull in ``filings_downloader`` and create a circular import with ``edgar``.
"""

from __future__ import annotations

from pathlib import Path
from typing import Optional, Tuple

from .form_type_path import form_type_filesystem_slug


def parse_quarter_form_output_dir(path: Path) -> Optional[Tuple[str, str, str]]:
    """
    If ``path`` looks like ``.../filings/<YYYY>/<QTRn>/<form_type>``, return
    (year, quarter, form_type). Otherwise None.
    """
    try:
        p = path.resolve()
        form_type = p.name
        quarter = p.parent.name
        year = p.parent.parent.name
    except (IndexError, OSError):
        return None
    if not quarter.startswith("QTR") or len(quarter) != 4:
        return None
    if not year.isdigit() or len(year) != 4:
        return None
    if not form_type:
        return None
    return year, quarter, form_type


def quarter_filings_zip_path(form_folder: Path) -> Optional[Path]:
    """Path to the archive zip under the filings root (``.../filings/``), or None if layout is not recognized."""
    parsed = parse_quarter_form_output_dir(form_folder)
    if not parsed:
        return None
    year, quarter, form_type = parsed
    safe = form_type_filesystem_slug(form_type)
    filings_root = form_folder.parent.parent.parent
    return filings_root / f"{year}-{quarter}-{safe}.zip"
