"""
NASDAQ Trader symbol directory FTP downloader

Connects to ftp.nasdaqtrader.com/symboldirectory/ and downloads:
- nasdaqlisted.txt
- otherlisted.txt
"""

from ftplib import FTP
from pathlib import Path
from typing import List, Optional

FTP_HOST = "ftp.nasdaqtrader.com"
FTP_DIR = "symboldirectory"
FILES = ("nasdaqlisted.txt", "otherlisted.txt")


def download_symbol_directory(
    output_dir: Optional[str] = None,
    host: str = FTP_HOST,
    remote_dir: str = FTP_DIR,
    files: Optional[tuple] = None,
) -> List[str]:
    """
    Download symbol directory files from NASDAQ Trader FTP.

    Args:
        output_dir: Local directory to save files. Defaults to
            {project_root}/storage/dev/markets/nasdaqtrader.
        host: FTP host (default: ftp.nasdaqtrader.com).
        remote_dir: Remote directory (default: symboldirectory).
        files: Filenames to download (default: nasdaqlisted.txt, otherlisted.txt).

    Returns:
        List of local file paths that were written.
    """
    if files is None:
        files = FILES
    if output_dir is None:
        project_root = Path(__file__).resolve().parent.parent.parent.parent
        output_dir = project_root / "storage" / "dev" / "markets" / "nasdaqtrader"
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    written = []
    with FTP(host) as ftp:
        ftp.login()
        ftp.cwd(remote_dir)
        for filename in files:
            local_path = output_dir / filename
            with open(local_path, "wb") as f:
                ftp.retrbinary("RETR " + filename, f.write)
            written.append(str(local_path))
    return written
