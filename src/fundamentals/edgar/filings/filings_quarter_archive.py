"""
Zip archived quarter/form-type filing folders.

Layout: ``.../fundamentals/edgar/filings/{year}/{QTRn}/{form_type_fs}/*.txt`` where ``form_type_fs``
is ``form_type_filesystem_slug()`` from ``form_type_path`` (e.g. ``10-K/A`` → ``10-K_A``).

Archive (under the env filings root): ``.../fundamentals/edgar/filings/{year}-{QTRn}-{form_type_fs}.zip``

After archiving, if ``{year}/{QTRn}/`` contains no ``*.txt`` files anymore, that quarter directory is removed.
If ``{year}/`` then contains no ``*.txt`` files anymore, the year directory under ``filings/`` is removed too.
"""

from __future__ import annotations

import logging
import shutil
import zipfile
from pathlib import Path

from tqdm import tqdm

from ..form_type_path import form_type_filesystem_slug
from ..quarter_filings_zip_path import parse_quarter_form_output_dir, quarter_filings_zip_path

# Match filings_downloader / download_logger channel for consistent console output
logger = logging.getLogger("edgar_filings")


def _safe_extract_zip_to_dir(zf: zipfile.ZipFile, dest_dir: Path) -> int:
    """
    Extract all non-directory members under ``dest_dir``. Skips ``__MACOSX/``.
    Rejects members whose relative path escapes ``dest_dir`` (zip-slip).
    Returns number of files written.
    """
    dest_dir = dest_dir.resolve()
    n = 0
    for member in zf.namelist():
        if member.endswith("/") or member.startswith("__MACOSX/"):
            continue
        target = (dest_dir / member).resolve()
        try:
            target.relative_to(dest_dir)
        except ValueError as e:
            raise ValueError(
                f"Refusing zip-slip path in archive: {member!r} (would write outside {dest_dir})"
            ) from e
        target.parent.mkdir(parents=True, exist_ok=True)
        with zf.open(member, "r") as src, open(target, "wb") as out:
            shutil.copyfileobj(src, out)
        n += 1
    return n


def extract_quarter_filings_zip(
    filings_root: Path,
    year: str,
    quarter: str,
    form_type: str,
) -> int:
    """
    Extract the single archive ``{year}-{quarter}-{form_type_fs}.zip`` under ``filings_root``
    into ``filings_root/{year}/{quarter}/{form_type_fs}/``, restoring the tree produced
    before :func:`zip_folder_and_remove`. ``form_type`` is the SEC form (e.g. ``10-K`` or ``10-K/A``);
    ``form_type_fs`` comes from :func:`form_type_filesystem_slug` in ``form_type_path``.

    Returns ``1`` if an archive was extracted, ``0`` if the file was missing or unusable.
    """
    filings_root = filings_root.resolve()
    year = str(year).strip()
    quarter = str(quarter).strip()
    ft = str(form_type).strip()
    if not ft:
        logger.warning("extract_quarter_filings_zip: form_type is empty")
        return 0
    slug = form_type_filesystem_slug(ft)
    zip_path = filings_root / f"{year}-{quarter}-{slug}.zip"
    if not zip_path.is_file():
        logger.warning(
            "Quarter zip not found (expected %s next to filings/): %s",
            zip_path.name,
            filings_root,
        )
        return 0

    out_dir = filings_root / year / quarter / slug
    out_dir.mkdir(parents=True, exist_ok=True)
    try:
        with zipfile.ZipFile(zip_path, "r") as zf:
            bad = zf.testzip()
            if bad is not None:
                logger.error("Zip failed integrity check: %s (bad member: %s)", zip_path, bad)
                return 0
            n_files = _safe_extract_zip_to_dir(zf, out_dir)
    except zipfile.BadZipFile as e:
        logger.warning("Not a valid zip file: %s (%s)", zip_path, e)
        return 0
    logger.info(
        "Extracted %s file(s) from %s -> %s",
        n_files,
        zip_path.name,
        out_dir,
    )
    return 1


def prune_filings_dirs_upward_if_no_txt_left(start_dir: Path) -> None:
    """
    From ``filings/.../year/QTRn``, walk upward: remove each directory that has no ``*.txt``
    under it, until we reach ``filings`` or a directory that still contains ``*.txt``.
    """
    d = start_dir.resolve()
    while d.is_dir() and d.name != "filings":
        if any(p.is_file() for p in d.rglob("*.txt")):
            return
        # Allow removing only filings/<YYYY> or filings/<YYYY>/<QTRn>
        if not (
            (d.parent.name == "filings" and d.name.isdigit() and len(d.name) == 4)
            or (
                d.parent.name.isdigit()
                and len(d.parent.name) == 4
                and d.name.startswith("QTR")
                and len(d.name) == 4
            )
        ):
            return
        parent = d.parent
        try:
            shutil.rmtree(d)
        except OSError:
            logger.exception("Failed to remove directory %s", d)
            raise
        logger.info("Removed directory (no .txt remaining): %s", d)
        d = parent


def zip_folder_and_remove(form_folder: Path, zip_path: Path) -> None:
    """
    Write all files under ``form_folder`` into ``zip_path`` (preserving relative paths),
    verify the archive, then delete ``form_folder``.
    """
    form_folder = form_folder.resolve()
    zip_path = zip_path.resolve()
    if not form_folder.is_dir():
        raise FileNotFoundError(f"Cannot archive: not a directory: {form_folder}")
    zip_path.parent.mkdir(parents=True, exist_ok=True)
    if zip_path.exists():
        zip_path.unlink()

    files_to_zip = sorted(p for p in form_folder.rglob("*") if p.is_file())
    with zipfile.ZipFile(
        zip_path, "w", zipfile.ZIP_DEFLATED, compresslevel=8
    ) as zf:
        for fp in tqdm(
            files_to_zip,
            desc=f"Zipping {zip_path.name}",
            unit="file",
            dynamic_ncols=True,
        ):
            arcname = fp.relative_to(form_folder)
            zf.write(fp, arcname=str(arcname))

    with zipfile.ZipFile(zip_path, "r") as zf:
        bad = zf.testzip()
    if bad is not None:
        zip_path.unlink(missing_ok=True)
        raise RuntimeError(f"Zip archive failed integrity check on member: {bad}")

    if zip_path.stat().st_size == 0:
        zip_path.unlink(missing_ok=True)
        raise RuntimeError(f"Zip archive is empty: {zip_path}")

    quarter_dir = form_folder.parent
    shutil.rmtree(form_folder)
    logger.info(
        "Archived quarter filings to %s and removed directory %s",
        zip_path,
        form_folder,
    )
    prune_filings_dirs_upward_if_no_txt_left(quarter_dir)
