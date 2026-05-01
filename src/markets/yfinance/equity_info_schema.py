"""
Apply equity_info schema: filter to schema fields and convert values to the requested types.
Schema is read from schema.yaml (field -> type: str, int, float, bool, datetime).
"""

import math
from datetime import datetime
from pathlib import Path
from typing import Any, Dict

import yaml

_SCHEMA_PATH = Path(__file__).resolve().parent / "schema.yaml"


def load_schema(schema_path: Path | None = None) -> Dict[str, str]:
    """Load field -> type mapping from schema.yaml. Returns e.g. {'name': 'str', 'marketCap': 'float'}."""
    path = schema_path or _SCHEMA_PATH
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    if isinstance(data, dict):
        return data
    raise ValueError(f"schema.yaml must be a single mapping (field: type), got {type(data)}")


def _convert(value: Any, type_name: str) -> Any:
    """Convert a single value to the schema type. Returns None for missing or invalid."""
    if value is None or value == "":
        return None
    type_name = type_name.strip().lower()
    if type_name == "str":
        return str(value) if value is not None else None
    if type_name == "int":
        try:
            if isinstance(value, bool):
                return int(value)
            return int(float(value))
        except (TypeError, ValueError):
            return None
    if type_name == "float":
        try:
            if isinstance(value, bool):
                return None
            v = float(value)
            return v if math.isfinite(v) else None  # Infinity/NaN -> null
        except (TypeError, ValueError):
            return None
    if type_name == "bool":
        if isinstance(value, bool):
            return value
        s = str(value).strip().lower()
        if s in ("true", "1", "yes"):
            return True
        if s in ("false", "0", "no"):
            return False
        return None
    if type_name == "datetime":
        try:
            if isinstance(value, (int, float)):
                return datetime.utcfromtimestamp(float(value)).isoformat() + "Z"
            if isinstance(value, str):
                if value.isdigit():
                    return datetime.utcfromtimestamp(int(value)).isoformat() + "Z"
                # ISO-like string (e.g. 2026-02-28T12:22:50.494284)
                try:
                    return datetime.fromisoformat(value.replace("Z", "+00:00")).isoformat()
                except ValueError:
                    return value
        except Exception:
            pass
        return None
    return str(value)


def apply_schema(raw: Dict[str, Any], schema: Dict[str, str] | None = None) -> Dict[str, Any]:
    """
    Return a dict with only schema keys and values converted to the schema types.
    Missing keys in raw become None. Result is JSON-serializable (datetimes as ISO strings).
    """
    if schema is None:
        schema = load_schema()
    out: Dict[str, Any] = {}
    for field, type_name in schema.items():
        value = raw.get(field)
        out[field] = _convert(value, type_name)
    return out
