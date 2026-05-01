"""
Accounting consistency checks for EDGAR scraped JSON.

Dispatches by data shape and form type:
- Legacy (PEM/IMS text) JSON: accession + balance_sheet at top level → 10k_legacy / 10q_legacy
- XBRL adj JSON: period key(s) + balance_sheet under period → 10k / 10q (rules-xbrl)
"""

import importlib.util
from pathlib import Path
from typing import Any, Dict, List, Tuple


def _load_module(rel_name: str, module_name: str):
    """Dynamically load a checks module from a sibling file."""
    spec = importlib.util.spec_from_file_location(
        module_name,
        Path(__file__).resolve().parent / rel_name,
    )
    if spec is None or spec.loader is None:
        raise ImportError(f"Could not load accounting_checks module {rel_name}")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)  # type: ignore[call-arg]
    return mod


_mod_10q = _load_module("10q.py", "_10q")
_mod_10k = _load_module("10k.py", "_10k")
_mod_10q_legacy = _load_module("10q_legacy.py", "_10q_legacy")
_mod_10k_legacy = _load_module("10k_legacy.py", "_10k_legacy")


def _is_legacy_shape(data: Dict[str, Any]) -> bool:
    """True if data is legacy scraped JSON (accession + balance_sheet at top level)."""
    return isinstance(data.get("balance_sheet"), dict) and "accession" in data


def run_all_checks(
    adj_data: Dict[str, Any],
    form_type: str | None = None,
) -> List[Tuple[str, bool, str | None, Dict[str, Any] | None]]:
    """
    Run accounting checks using the appropriate ruleset for the data shape and form type.

    - Legacy shape (accession + balance_sheet at top level): use 10k_legacy or 10q_legacy (rules-legacy).
    - XBRL adj shape (period key with balance_sheet inside): use 10k or 10q (rules-xbrl).
    - For 10-K-like forms (10-K, 20-F, 40-F, etc.), use 10k / 10k_legacy.
    - For 10-Q-like or default, use 10q / 10q_legacy.
    """
    ft = (form_type or "").upper()

    def _is_10k_like(s: str) -> bool:
        if not s:
            return False
        return s.startswith("10-K") or s in {"20-F", "40-F", "10-KT", "10-K/A"}

    if _is_legacy_shape(adj_data):
        if _is_10k_like(ft):
            return _mod_10k_legacy.run_all_checks(adj_data)
        return _mod_10q_legacy.run_all_checks(adj_data)

    if _is_10k_like(ft):
        return _mod_10k.run_all_checks(adj_data)
    return _mod_10q.run_all_checks(adj_data)


__all__ = ["run_all_checks"]
