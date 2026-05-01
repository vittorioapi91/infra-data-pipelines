"""
Hyperliquid downloader CLI.

Writes to:
  storage/{ENV}/markets/hyperliquid/
    meta.json
    candles/<coin>/<interval>.csv
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Optional

import pandas as pd

from .hyperliquid_client import HyperliquidClient


def _get_hyperliquid_storage() -> Path:
    """Return storage/{env}/markets/hyperliquid directory.

    Uses TRADING_AGENT_STORAGE as a common storage root (without env) when set.
    """
    project_root = Path(__file__).resolve().parent.parent.parent.parent
    storage_root = os.getenv("TRADING_AGENT_STORAGE")
    storage_env = os.getenv("ENV", "dev")
    if storage_root:
        return Path(storage_root) / storage_env / "markets" / "hyperliquid"
    return project_root / "storage" / storage_env / "markets" / "hyperliquid"


def _utc_ms(dt: datetime) -> int:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


def _parse_dt(s: str) -> datetime:
    """
    Parse an ISO-ish datetime string into UTC datetime.
    Accepts YYYY-MM-DD, YYYY-MM-DDTHH:MM:SS, with optional Z.
    """
    s = s.strip()
    if s.endswith("Z"):
        s = s[:-1]
    if "T" in s:
        dt = datetime.fromisoformat(s)
    else:
        dt = datetime.fromisoformat(s + "T00:00:00")
    return dt.replace(tzinfo=timezone.utc)


def _iter_coins_from_meta(meta: dict) -> Iterable[str]:
    universe = meta.get("universe")
    if isinstance(universe, list):
        for item in universe:
            if isinstance(item, dict) and isinstance(item.get("name"), str):
                yield item["name"]


def download_catalog(client: HyperliquidClient, out_dir: Path) -> dict:
    out_dir.mkdir(parents=True, exist_ok=True)
    meta = client.meta()
    (out_dir / "meta.json").write_text(json.dumps(meta, indent=2, sort_keys=True))
    return meta


def download_candles(
    client: HyperliquidClient,
    *,
    coin: str,
    interval: str,
    start_dt_utc: datetime,
    end_dt_utc: datetime,
    out_dir: Path,
) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    candles = client.candle_snapshot(
        coin=coin,
        interval=interval,
        start_time_ms=_utc_ms(start_dt_utc),
        end_time_ms=_utc_ms(end_dt_utc),
    )
    df = pd.DataFrame(candles)
    # canonical columns from API: t,T,s,i,o,c,h,l,v,n (strings/numbers)
    out_path = out_dir / f"{interval}.csv"
    df.to_csv(out_path, index=False)
    return out_path


def main() -> int:
    parser = argparse.ArgumentParser(description="Download Hyperliquid public market data (no wallet).")
    parser.add_argument(
        "--generate-catalog",
        action="store_true",
        help="Download universe metadata (coin list) to meta.json",
    )
    parser.add_argument("--candles", action="store_true", help="Download candle snapshots to candles/<coin>/<interval>.csv")
    parser.add_argument("--coins", type=str, default=None, help="Comma-separated coin list (e.g. BTC,ETH). Default: all from meta")
    parser.add_argument("--interval", type=str, default="1m", help="Candle interval (default: 1m)")
    parser.add_argument("--start", type=str, default=None, help="UTC start datetime (ISO). Required for --candles.")
    parser.add_argument("--end", type=str, default=None, help="UTC end datetime (ISO). Default: now (for --candles).")
    parser.add_argument("--base-url", type=str, default="https://api.hyperliquid.xyz", help="API base url (default: mainnet)")
    args = parser.parse_args()

    wants_catalog = bool(args.generate_catalog)

    if not args.generate_catalog and not args.candles:
        print(
            "Error: specify at least one of --generate-catalog, --candles",
            file=sys.stderr,
        )
        return 2

    storage = _get_hyperliquid_storage()
    storage.mkdir(parents=True, exist_ok=True)
    client = HyperliquidClient(base_url=args.base_url)

    meta_obj: Optional[dict] = None
    if wants_catalog or (args.coins is None and args.candles):
        meta_obj = download_catalog(client, storage)

    if args.coins:
        coins = [c.strip() for c in args.coins.split(",") if c.strip()]
    else:
        coins = list(_iter_coins_from_meta(meta_obj or {}))
        if not coins:
            print("Error: could not infer coins from meta; pass --coins", file=sys.stderr)
            return 2

    if args.candles:
        if not args.start:
            print("Error: --candles requires --start", file=sys.stderr)
            return 2
        start_dt = _parse_dt(args.start)
        end_dt = _parse_dt(args.end) if args.end else datetime.now(timezone.utc)
        for coin in coins:
            out_path = download_candles(
                client,
                coin=coin,
                interval=args.interval,
                start_dt_utc=start_dt,
                end_dt_utc=end_dt,
                out_dir=storage / "candles" / coin,
            )
            print(f"candles {coin} {args.interval}: {out_path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

