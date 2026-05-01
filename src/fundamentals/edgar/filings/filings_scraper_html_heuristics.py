"""
Rule-based extraction of forward-looking guidance from SEC EDGAR HTML filings.

Extracts revenue, earnings, and margin guidance using regex patterns and
context checks to distinguish guidance from reported results.

All patterns are loaded from rules-html/*.yaml. The YAML files are the single source of truth.
"""

import re
from typing import Any, Dict, List, Optional


def _get_rules() -> Dict[str, Any]:
    """Load rules from rules-html/*.yaml. Raises if YAML files are missing or invalid."""
    from ..rules_loader import load_all_rules

    rules = load_all_rules()
    if not rules:
        raise RuntimeError(
            "EDGAR guidance rules could not be loaded. "
            "Ensure rules-html/guidance-*.yaml files exist and PyYAML is installed."
        )
    return rules


_RULES = _get_rules()

_REVENUE_RANGE = _RULES["revenue_range"]
_REVENUE_SINGLE = _RULES["revenue_single"]
_EPS_RANGE = _RULES["eps_range"]
_EPS_SINGLE = _RULES["eps_single"]
_NET_INCOME_QUAL = _RULES["net_income_qualitative"]
_NET_INCOME_RANGE = _RULES["net_income_range"]
_GROSS_MARGIN_ADJ_RANGE = _RULES["gross_margin_adj_range"]
_GROSS_MARGIN_ADJ_SINGLE = _RULES["gross_margin_adj_single"]
_GROSS_MARGIN_ADJ_AT_LEAST = _RULES["gross_margin_adj_at_least"]
_GROSS_MARGIN_GAAP_SINGLE = _RULES["gross_margin_gaap_single"]
_GROSS_MARGIN_BOTH = _RULES["gross_margin_both"]
_FY_PERIOD = _RULES["fy_period"]
_Q_PERIOD = _RULES["q_period"]
_REVENUE_METRIC_PATTERNS = _RULES["metric_patterns"]
_EXCLUDE_METRIC_PATTERNS = _RULES["exclude_metric"]
_VS_PRIOR_PATTERNS = _RULES["vs_prior"]
_VS_PRIOR_HISTORICAL = _RULES["vs_prior_historical"]
_RESULTS_CONTEXT_PATTERNS = _RULES["results_context"]
_GUIDANCE_CONTEXT_PATTERNS = _RULES["guidance_context"]


def _parse_magnitude(s: str) -> float:
    if not s:
        return 1e6
    s = (s or "").strip().lower()
    if s in ("billion", "b"):
        return 1e9
    if s in ("million", "m"):
        return 1e6
    return 1e6


def _parse_num(s: str) -> Optional[float]:
    if not s:
        return None
    try:
        return float(s.replace(",", ""))
    except (ValueError, TypeError):
        return None


def _revenue_metric_from_context(text: str, pos: int, end: int, lookback: int = 150, lookahead: int = 80) -> Optional[str]:
    start = max(0, pos - lookback)
    ctx = text[start : min(len(text), end + lookahead)]
    after_match = text[end : min(len(text), end + 35)]
    for exc in _EXCLUDE_METRIC_PATTERNS:
        if exc.search(after_match):
            return None
    candidates: List[tuple] = []
    for metric, pat in _REVENUE_METRIC_PATTERNS:
        best_d = float("inf")
        for m in pat.finditer(ctx):
            match_end = start + m.end()
            d = pos - match_end
            if 0 <= d < best_d:
                best_d = d
        if best_d != float("inf"):
            candidates.append((metric, best_d))
    if not candidates:
        return None
    exome = next((c for c in candidates if c[0] == "exome_and_genome_revenue"), None)
    if exome and exome[1] < 70:
        return "exome_and_genome_revenue"
    return min(candidates, key=lambda x: x[1])[0]


def _vs_prior_from_context(text: str, pos: int, end: int, window: int = 200) -> Optional[str]:
    start = max(0, pos - window)
    ctx = text[start : min(len(text), end + window)]
    for label, pat in _VS_PRIOR_PATTERNS:
        if pat.search(ctx):
            if label in ("raised", "lowered") and _VS_PRIOR_HISTORICAL.search(ctx):
                return "maintained"
            return label
    return None


def _is_forward_looking_context(text: str, start: int, end: int, window: int = 500) -> bool:
    ctx_start = max(0, start - window)
    ctx_end = min(len(text), end + 100)
    ctx = text[ctx_start:ctx_end]
    for pat in _RESULTS_CONTEXT_PATTERNS:
        if pat.search(ctx):
            return False
    for pat in _GUIDANCE_CONTEXT_PATTERNS:
        if pat.search(ctx):
            return True
    return False


def _matches_with_context(pattern: re.Pattern, text: str):
    """Yield regex matches that occur in forward-looking context."""
    for m in pattern.finditer(text):
        if _is_forward_looking_context(text, m.start(), m.end()):
            yield m


def _add_entry(
    result: Dict[str, List],
    category: str,
    seen: set,
    key: Any,
    entry: Dict[str, Any],
    text: str,
    start: int,
    end: int,
    *,
    add_vs_prior: bool = True,
) -> bool:
    """Add entry to result if key not seen. Returns True if added."""
    if key in seen:
        return False
    seen.add(key)
    if add_vs_prior:
        vp = _vs_prior_from_context(text, start, end)
        if vp:
            entry["vs_prior"] = vp
    result[category].append(entry)
    return True


def extract_guidance(text: str) -> Dict[str, Any]:
    """
    Extract structured revenue, earnings, and margin guidance using rule-based patterns.

    Returns dict with revenue, earnings, margins lists.
    """
    result: Dict[str, Any] = {"revenue": [], "earnings": [], "margins": []}
    if not text or len(text) < 50:
        return result

    last_period: Optional[str] = None

    def _period_from_match(m) -> Optional[str]:
        q = m.group(1)
        yr = m.group(2) or m.group(3)
        if yr:
            if q:
                return f"q{q}_{yr}"
            txt = m.group(0).lower()
            if "first" in txt:
                return f"q1_{yr}"
            if "second" in txt:
                return f"q2_{yr}"
            if "third" in txt:
                return f"q3_{yr}"
            if "fourth" in txt:
                return f"q4_{yr}"
        return None

    period_positions: List[tuple] = []
    for m in _FY_PERIOD.finditer(text):
        yr = m.group(1)
        if yr:
            period_positions.append((m.start(), f"FY{yr}"))
    for m in _Q_PERIOD.finditer(text):
        p = _period_from_match(m)
        if p:
            period_positions.append((m.start(), p))
    period_positions.sort(key=lambda x: x[0])

    def _period_at(pos: int) -> Optional[str]:
        prev = None
        for ppos, p in period_positions:
            if ppos <= pos:
                prev = p
            else:
                break
        return prev

    def _period_after(end_pos: int, same_clause_max: int = 45) -> Optional[str]:
        for ppos, p in period_positions:
            if ppos < end_pos:
                continue
            if ppos - end_pos <= same_clause_max:
                return p
            break
        return None

    seen_rev: set = set()
    for m in _matches_with_context(_REVENUE_RANGE, text):
        metric = _revenue_metric_from_context(text, m.start(), m.end())
        if metric is None:
            continue
        low, high = _parse_num(m.group(1)), _parse_num(m.group(2))
        if low is None or high is None:
            continue
        unit = (m.group(3) or "million").strip().lower()
        mult = _parse_magnitude(unit)
        period = _period_after(m.end()) or _period_at(m.start()) or last_period
        low_m, high_m = low * mult / 1e6, high * mult / 1e6
        key = (period, metric, low, high)
        entry = {
            "period": period,
            "metric": metric,
            "value": (low_m + high_m) / 2,
            "type_value": "interval",
            "low": low_m,
            "high": high_m,
            "unit": "million",
            "raw": m.group(0).strip(),
        }
        _add_entry(result, "revenue", seen_rev, key, entry, text, m.start(), m.end())
        last_period = _period_at(m.start())

    for m in _matches_with_context(_REVENUE_SINGLE, text):
        metric = _revenue_metric_from_context(text, m.start(), m.end())
        if metric is None:
            continue
        val = _parse_num(m.group(1))
        if val is None:
            continue
        unit = (m.group(2) or "million").strip().lower()
        mult = _parse_magnitude(unit)
        period = _period_after(m.end()) or _period_at(m.start()) or last_period
        existing = [r for r in result["revenue"] if r.get("period") == period and r.get("metric") == metric]
        if any("low" in r for r in existing):
            continue
        val_m = val * mult / 1e6
        if any(r.get("low") <= val_m <= r.get("high", val_m) for r in existing if "low" in r):
            continue
        key = (period, metric, val)
        entry = {
            "period": period,
            "metric": metric,
            "value": val_m,
            "type_value": "exact",
            "unit": "million",
            "raw": m.group(0).strip(),
        }
        _add_entry(result, "revenue", seen_rev, key, entry, text, m.start(), m.end())

    seen_earn: set = set()
    for m in _matches_with_context(_EPS_RANGE, text):
        low, high = _parse_num(m.group(1)), _parse_num(m.group(2))
        if low is None or high is None:
            continue
        period = _period_at(m.start()) or last_period
        key = ("eps", period, low, high)
        entry = {
            "period": period,
            "type": "eps",
            "value": (low + high) / 2,
            "type_value": "interval",
            "low": low,
            "high": high,
            "raw": m.group(0).strip(),
        }
        _add_entry(result, "earnings", seen_earn, key, entry, text, m.start(), m.end())

    for m in _matches_with_context(_EPS_SINGLE, text):
        val = _parse_num(m.group(1))
        if val is None:
            continue
        period = _period_at(m.start()) or last_period
        key = ("eps", period, val)
        entry = {
            "period": period,
            "type": "eps",
            "value": val,
            "type_value": "exact",
            "raw": m.group(0).strip(),
        }
        _add_entry(result, "earnings", seen_earn, key, entry, text, m.start(), m.end())

    for m in _matches_with_context(_NET_INCOME_QUAL, text):
        qual = (m.group(1) or "").strip().lower()
        period = _period_at(m.start()) or last_period
        key = ("qual", period, qual)
        entry = {
            "period": period,
            "type": "adjusted_net_income" if "adjusted" in m.group(0).lower() else "net_income",
            "type_value": "qualitative",
            "qualitative": qual,
            "raw": m.group(0).strip(),
        }
        _add_entry(result, "earnings", seen_earn, key, entry, text, m.start(), m.end())

    for m in _matches_with_context(_NET_INCOME_RANGE, text):
        low, high = _parse_num(m.group(1)), _parse_num(m.group(2))
        if low is None or high is None:
            continue
        unit = (m.group(3) or "million").strip().lower()
        mult = _parse_magnitude(unit)
        period = _period_at(m.start()) or last_period
        low_m, high_m = low * mult / 1e6, high * mult / 1e6
        key = ("range", period, low, high)
        entry = {
            "period": period,
            "type": "net_income",
            "value": (low_m + high_m) / 2,
            "type_value": "interval",
            "low": low_m,
            "high": high_m,
            "unit": "million",
            "raw": m.group(0).strip(),
        }
        _add_entry(result, "earnings", seen_earn, key, entry, text, m.start(), m.end())

    seen_marg: set = set()
    for m in _matches_with_context(_GROSS_MARGIN_BOTH, text):
        gaap_pct = _parse_num(m.group(1))
        adj_pct = _parse_num(m.group(2))
        if gaap_pct is None or adj_pct is None:
            continue
        period = _period_at(m.start()) or last_period
        for mtype, val in [("gaap_gross_margin", gaap_pct), ("adjusted_gross_margin", adj_pct)]:
            key = (mtype, period, val)
            entry = {
                "period": period,
                "type": mtype,
                "value": val,
                "type_value": "exact",
                "unit": "percent",
                "raw": m.group(0).strip(),
            }
            _add_entry(result, "margins", seen_marg, key, entry, text, m.start(), m.end())

    for m in _matches_with_context(_GROSS_MARGIN_ADJ_RANGE, text):
        low, high = _parse_num(m.group(1)), _parse_num(m.group(2))
        if low is None or high is None:
            continue
        period = _period_at(m.start()) or last_period
        key = ("adjusted_gross_margin", period, low, high)
        entry = {
            "period": period,
            "type": "adjusted_gross_margin",
            "value": (low + high) / 2,
            "type_value": "interval",
            "low": low,
            "high": high,
            "unit": "percent",
            "raw": m.group(0).strip(),
        }
        _add_entry(result, "margins", seen_marg, key, entry, text, m.start(), m.end())

    for m in _matches_with_context(_GROSS_MARGIN_ADJ_SINGLE, text):
        val = _parse_num(m.group(1))
        if val is None:
            continue
        period = _period_at(m.start()) or last_period
        key = ("adjusted_gross_margin", period, val)
        entry = {
            "period": period,
            "type": "adjusted_gross_margin",
            "value": val,
            "type_value": "exact",
            "unit": "percent",
            "raw": m.group(0).strip(),
        }
        _add_entry(result, "margins", seen_marg, key, entry, text, m.start(), m.end())

    for m in _matches_with_context(_GROSS_MARGIN_ADJ_AT_LEAST, text):
        val = _parse_num(m.group(1))
        if val is None:
            continue
        period = _period_at(m.start()) or last_period
        key = ("adjusted_gross_margin", period, "at_least", val)
        entry = {
            "period": period,
            "type": "adjusted_gross_margin",
            "value": val,
            "type_value": "min",
            "unit": "percent",
            "raw": m.group(0).strip(),
        }
        _add_entry(result, "margins", seen_marg, key, entry, text, m.start(), m.end(), add_vs_prior=False)

    for m in _matches_with_context(_GROSS_MARGIN_GAAP_SINGLE, text):
        val = _parse_num(m.group(1))
        if val is None:
            continue
        period = _period_at(m.start()) or last_period
        key = ("gaap_gross_margin", period, val)
        entry = {
            "period": period,
            "type": "gaap_gross_margin",
            "value": val,
            "type_value": "exact",
            "unit": "percent",
            "raw": m.group(0).strip(),
        }
        _add_entry(result, "margins", seen_marg, key, entry, text, m.start(), m.end())

    def _dedupe(items: List[Dict], sig_keys: tuple) -> List[Dict]:
        seen: set = set()
        out: List[Dict] = []
        for item in items:
            sig = tuple(item.get(k) for k in sig_keys)
            if sig not in seen:
                seen.add(sig)
                out.append(item)
        return out

    result["revenue"] = _dedupe(result["revenue"], ("period", "metric", "low", "high", "value"))
    result["earnings"] = _dedupe(result["earnings"], ("period", "type", "low", "high", "qualitative", "value"))
    result["margins"] = _dedupe(result["margins"], ("period", "type", "low", "high", "value", "type_value"))

    def _collapse_margins(items: List[Dict]) -> List[Dict]:
        def _rank(item: Dict) -> int:
            if item.get("type_value") == "interval":
                return 2
            if item.get("type_value") == "exact":
                return 1
            return 0

        by_key: Dict[tuple, Dict] = {}
        for item in items:
            key = (item.get("period"), item.get("type"))
            existing = by_key.get(key)
            if existing is None or _rank(item) > _rank(existing):
                by_key[key] = item
        return list(by_key.values())

    result["margins"] = _collapse_margins(result["margins"])

    def _collapse_by_period_metric(items: List[Dict]) -> List[Dict]:
        by_key: Dict[tuple, Dict] = {}
        for item in items:
            key = (item.get("period"), item.get("metric"))
            existing = by_key.get(key)
            if existing is None:
                by_key[key] = item
            elif "low" in item and "low" not in existing:
                by_key[key] = item
        return list(by_key.values())

    result["revenue"] = _collapse_by_period_metric(result["revenue"])
    return result
