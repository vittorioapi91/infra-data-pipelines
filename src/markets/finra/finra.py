"""
FINRA Query API downloader CLI.

Usage:
  --generate-catalog
    Create a CSV of all groups and datasets (storage/.../finra/catalog.csv).

  --download [--group GROUP]
    Download datasets to storage/.../finra/{group}/{dataset}.csv.
    If --group is set, download only that group; otherwise download all groups (MOCK skipped).

Data source: https://api.finra.org/data/group/{group}/name/{dataset}
Catalog:     GET https://api.finra.org/datasets
"""

from __future__ import annotations

import argparse
import base64
import csv
import os
import sys
from pathlib import Path
from typing import Optional

import requests
from tqdm import tqdm


# Group/dataset from GET https://api.finra.org/datasets?group=fixedIncomeMarket
_DEFAULT_GROUP = "fixedIncomeMarket"
_DEFAULT_DATASET = "corporatesandagenciescappedvolume"  # Trace Corporates and Agencies Capped Volume
_API_BASE = "https://api.finra.org"
_TOKEN_URL = "https://ews.fip.finra.org/fip/rest/ews/oauth2/access_token"


def _get_finra_storage() -> Path:
    """
    Return storage/{env}/markets/finra directory.

    Uses TRADING_AGENT_STORAGE when set, otherwise falls back to
    <project_root>/storage/{ENV}/markets/finra.
    """
    project_root = Path(__file__).resolve().parent.parent.parent.parent
    storage_root = os.getenv("TRADING_AGENT_STORAGE")
    storage_env = os.getenv("ENV", "dev")
    if storage_root:
        return Path(storage_root) / storage_env / "markets" / "finra"
    return project_root / "storage" / storage_env / "markets" / "finra"


def _get_group() -> str:
    """Resolve the FINRA dataset group (e.g. fixedIncomeMarket). Override via FINRA_CORP_AGENCY_GROUP."""
    return os.getenv("FINRA_CORP_AGENCY_GROUP", _DEFAULT_GROUP)


def _get_dataset_name() -> str:
    """
    Resolve the FINRA dataset name for Corporate & Agency bonds.

    Order of precedence:
    1. FINRA_CORP_AGENCY_DATASET environment variable
    2. _DEFAULT_DATASET constant
    """
    return os.getenv("FINRA_CORP_AGENCY_DATASET", _DEFAULT_DATASET)


def _get_finra_access_token() -> str:
    """
    Obtain an OAuth2 access token using FINRA's client-credentials flow.

    Env vars:
      FINRA_API_CLIENT_ID  - Client ID from FINRA console
      FINRA_API_PASSWORD       - Client Secret (per user instruction)
    """
    client_secret = os.getenv("FINRA_API_PASSWORD")
    client_id = os.getenv("FINRA_API_CLIENT_ID")
    if not client_id or not client_secret:
        raise RuntimeError(
            "FINRA_API_CLIENT_ID and FINRA_API_PASSWORD must be set in your environment "
            "(.env.dev, .env.test, .env.prod) to use the FINRA downloader."
        )
    basic = base64.b64encode(f"{client_id}:{client_secret}".encode("utf-8")).decode("ascii")
    headers = {"Authorization": f"Basic {basic}"}
    params = {"grant_type": "client_credentials"}
    resp = requests.post(_TOKEN_URL, headers=headers, params=params, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    token = data.get("access_token")
    if not token:
        raise RuntimeError("FINRA OAuth2 token response did not contain access_token")
    return token


def _get_finra_session() -> requests.Session:
    """
    Create a requests.Session configured with FINRA Query API bearer token.
    """
    token = _get_finra_access_token()
    session = requests.Session()
    session.headers.update({"Authorization": f"Bearer {token}"})
    return session


def list_finra_datasets(group: str) -> list[str]:
    """
    Return dataset names for a FINRA group (e.g. fixedIncomeMarket).

    Uses GET https://api.finra.org/datasets?group={group}. Names are returned
    in API casing; use them as-is for the data endpoint (API accepts lowercase).
    """
    session = _get_finra_session()
    resp = session.get(f"{_API_BASE}/datasets", params={"group": group}, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    return [d["name"] for d in data.get("datasets", [])]


def list_finra_all_datasets() -> list[dict]:
    """
    Return all datasets from the FINRA API (all groups).

    Uses GET https://api.finra.org/datasets (no group filter).
    Each item has at least "group", "name", and optionally "description".
    """
    session = _get_finra_session()
    resp = session.get(f"{_API_BASE}/datasets", timeout=60)
    resp.raise_for_status()
    data = resp.json()
    return data.get("datasets", [])


def download_finra_dataset(
    out_path: Path,
    group: str,
    dataset: str,
    *,
    limit: int = 50000,
    description: Optional[str] = None,
) -> int:
    """
    Download any FINRA Query API dataset into a CSV file.

    Paginates over the FINRA data API using ?limit & ?offset, streaming
    results to disk and showing a tqdm progress bar over rows.

    Args:
        out_path: Path to write the CSV file.
        group: FINRA dataset group (e.g. fixedIncomeMarket, otcMarket).
        dataset: FINRA dataset name (e.g. corporatesandagenciescappedvolume).
        limit: Page size per request (default 50000).
        description: Optional label for the progress bar; default "FINRA {group}/{dataset}".

    Returns:
        Number of data rows written (excluding header).
    """
    url = f"{_API_BASE}/data/group/{group}/name/{dataset}"
    label = description or f"FINRA {group}/{dataset}"

    out_path.parent.mkdir(parents=True, exist_ok=True)

    session = _get_finra_session()
    offset = 0
    total_rows = 0
    header_written = False

    pbar = tqdm(
        desc=label,
        unit="row",
        dynamic_ncols=True,
        mininterval=0.3,
    )

    with out_path.open("w", newline="", encoding="utf-8") as f_out:
        writer: Optional[csv.writer] = None

        while True:
            params = {"limit": limit, "offset": offset}
            resp = session.get(url, params=params, timeout=60)
            resp.raise_for_status()
            text = resp.text
            lines = text.splitlines()
            if not lines:
                break

            if not header_written:
                f_out.write(lines[0] + "\n")
                header_written = True
                data_lines = lines[1:]
                writer = csv.writer(f_out)
            else:
                data_lines = lines[1:] if len(lines) > 1 else []

            if not data_lines:
                break

            reader = csv.reader(data_lines)
            batch_rows = 0
            for row in reader:
                writer.writerow(row)
                batch_rows += 1
            total_rows += batch_rows
            pbar.update(batch_rows)

            offset += limit
            if batch_rows < limit:
                break

    pbar.close()
    return total_rows


def _load_env() -> None:
    try:
        from src.config import load_environment_config
        load_environment_config()
    except Exception:
        try:
            from dotenv import load_dotenv
            project_root = Path(__file__).resolve().parent.parent.parent.parent
            for name in (".env.dev", ".env"):
                p = project_root / name
                if p.exists():
                    load_dotenv(p, override=True)
                    break
        except Exception:
            pass


def _download_group(storage: Path, group: str) -> tuple[int, list[tuple[str, Exception]]]:
    """Download all non-MOCK datasets for one group. Returns (success_count, failed_list)."""
    group_dir = storage / group
    group_dir.mkdir(parents=True, exist_ok=True)
    dataset_names = list_finra_datasets(group)
    dataset_names = [n for n in dataset_names if not n.upper().endswith("MOCK")]
    failed = []
    success = 0
    for name in dataset_names:
        dataset_slug = name.lower()
        out_csv = group_dir / f"{dataset_slug}.csv"
        try:
            download_finra_dataset(
                out_csv,
                group,
                dataset_slug,
                description=f"FINRA {group}/{dataset_slug}",
            )
            print(f"Wrote {out_csv.relative_to(storage)}")
            success += 1
        except Exception as e:
            failed.append((name, e))
            print(f"Failed {name}: {e}", file=sys.stderr)
    return success, failed


def main() -> int:
    parser = argparse.ArgumentParser(
        description="FINRA Query API: generate catalog of groups/datasets or download data.",
    )
    parser.add_argument(
        "--generate-catalog",
        action="store_true",
        help="Create a CSV listing all groups and datasets (storage/.../finra/catalog.csv).",
    )
    parser.add_argument(
        "--download",
        action="store_true",
        help="Download datasets. Use --group to limit to one group; otherwise download all groups (MOCK skipped).",
    )
    parser.add_argument(
        "--group",
        metavar="GROUP",
        help="Limit --download to this group (e.g. fixedIncomeMarket).",
    )
    args = parser.parse_args()

    _load_env()

    if not args.generate_catalog and not args.download:
        parser.error("Specify --generate-catalog and/or --download.")

    storage = _get_finra_storage()
    storage.mkdir(parents=True, exist_ok=True)

    if args.generate_catalog:
        datasets = list_finra_all_datasets()
        catalog_path = storage / "catalog.csv"
        with catalog_path.open("w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["group", "name", "description"])
            for d in datasets:
                w.writerow([
                    d.get("group", ""),
                    d.get("name", ""),
                    d.get("description", ""),
                ])
        print(f"Wrote catalog with {len(datasets)} datasets to {catalog_path}")

    if args.download:
        if args.group:
            groups = [args.group]
        else:
            datasets = list_finra_all_datasets()
            groups = sorted({d["group"] for d in datasets if d.get("group")}, key=str.lower)
        all_failed = []
        total_ok = 0
        for group in groups:
            try:
                ok, failed = _download_group(storage, group)
                total_ok += ok
                all_failed.extend((group, name, e) for name, e in failed)
            except Exception as e:
                print(f"Failed group {group}: {e}", file=sys.stderr)
                all_failed.append((group, None, e))
        if all_failed:
            print(f"\n{len(all_failed)} dataset(s) failed", file=sys.stderr)
            return 1
        print(f"\nDownloaded {total_ok} dataset(s) to {storage}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

