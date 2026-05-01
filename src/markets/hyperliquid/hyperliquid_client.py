"""
Hyperliquid public Info API client (no auth).

Docs (Info endpoint methods):
- meta
- candleSnapshot
- recentTrades
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import requests


@dataclass(frozen=True)
class HyperliquidClient:
    """
    Minimal client for Hyperliquid Info API.

    By default uses mainnet: https://api.hyperliquid.xyz/info
    """

    base_url: str = "https://api.hyperliquid.xyz"
    timeout_sec: int = 30
    max_retries: int = 3
    backoff_sec: float = 1.5

    @property
    def info_url(self) -> str:
        return f"{self.base_url.rstrip('/')}/info"

    def _post_info(self, payload: Dict[str, Any]) -> Any:
        last_err: Optional[Exception] = None
        for attempt in range(self.max_retries + 1):
            try:
                r = requests.post(self.info_url, json=payload, timeout=self.timeout_sec)
                r.raise_for_status()
                return r.json()
            except (requests.Timeout, requests.ConnectionError, requests.HTTPError) as e:
                last_err = e
                if attempt >= self.max_retries:
                    raise
                time.sleep(self.backoff_sec * (attempt + 1))
        if last_err is not None:
            raise last_err
        raise RuntimeError("HyperliquidClient._post_info: exhausted retries")

    def meta(self, dex: str = "") -> Dict[str, Any]:
        """Return universe metadata (markets list, etc.)."""
        out = self._post_info({"type": "meta", "dex": dex})
        if not isinstance(out, dict):
            raise TypeError(f"meta(): expected dict, got {type(out)}")
        return out

    def candle_snapshot(
        self,
        *,
        coin: str,
        interval: str,
        start_time_ms: int,
        end_time_ms: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """
        Return candle snapshots for a coin and interval within [start, end].

        interval: 1m|3m|5m|15m|30m|1h|2h|4h|8h|12h|1d|3d|1w|1M
        """
        req: Dict[str, Any] = {
            "coin": coin,
            "interval": interval,
            "startTime": int(start_time_ms),
        }
        if end_time_ms is not None:
            req["endTime"] = int(end_time_ms)
        out = self._post_info({"type": "candleSnapshot", "req": req})
        if not isinstance(out, list):
            raise TypeError(f"candle_snapshot(): expected list, got {type(out)}")
        return out

    def recent_trades(self, *, coin: str) -> List[Dict[str, Any]]:
        """Return the most recent trades for a coin."""
        out = self._post_info({"type": "recentTrades", "coin": coin})
        if not isinstance(out, list):
            raise TypeError(f"recent_trades(): expected list, got {type(out)}")
        return out

