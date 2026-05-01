"""JSON-serialization helpers for yfinance DataFrames and dicts."""

from typing import Any, Dict, List, Optional

import pandas as pd


def _df_to_records(df: Optional[pd.DataFrame]) -> List[Dict[str, Any]]:
    """Convert DataFrame to list of dicts with ISO dates and JSON-serializable keys/values."""
    if df is None or df.empty:
        return []
    out = []
    for idx, row in df.iterrows():
        idx_str = idx.isoformat() if hasattr(idx, "isoformat") else str(idx)
        rec: Dict[str, Any] = {"_index": idx_str}
        for k, v in row.items():
            key = k.isoformat() if hasattr(k, "isoformat") else str(k)
            if pd.isna(v):
                rec[key] = None
            elif hasattr(v, "isoformat"):
                rec[key] = v.isoformat()
            elif hasattr(v, "item"):
                rec[key] = v.item()
            else:
                rec[key] = v
        out.append(rec)
    return out


def _series_or_dict_to_json_safe(obj: Any) -> Any:
    """Convert Series/dict to JSON-serializable (dates to ISO string, etc.)."""
    if obj is None:
        return None
    if hasattr(obj, "isoformat"):
        return obj.isoformat()
    if hasattr(obj, "item"):
        return obj.item()
    if isinstance(obj, pd.DataFrame):
        return _df_to_records(obj)
    if isinstance(obj, pd.Series):
        return obj.to_dict()
    if isinstance(obj, dict):
        key_str = lambda k: k.isoformat() if hasattr(k, "isoformat") else str(k)
        return {key_str(k): _series_or_dict_to_json_safe(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_series_or_dict_to_json_safe(v) for v in obj]
    return obj
