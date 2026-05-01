"""
Load EDGAR HTML rules from YAML files in rules-html/.

Returns compiled regex patterns and structured config for guidance extraction.
"""

import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

_RULES_DIR = Path(__file__).resolve().parent / "rules-html"


def _load_yaml(name: str) -> Optional[Dict[str, Any]]:
    """Load a YAML file from rules-html/."""
    try:
        import yaml
    except ImportError:
        return None
    path = _RULES_DIR / name
    if not path.exists():
        return None
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def _compile(pat: str, flags: Optional[str] = None) -> re.Pattern:
    f = re.I if flags and "I" in flags else 0
    return re.compile(pat, f)


def load_guidance_detection() -> Tuple[Tuple[str, ...], Tuple[str, ...], Tuple[re.Pattern, ...]]:
    """Load document-level guidance detection (description_keywords, content_keywords, content_patterns)."""
    data = _load_yaml("8k_guidance-detection.yaml")
    if not data:
        return (), (), ()

    desc = tuple(data.get("description_keywords") or [])
    content_kw = tuple(data.get("content_keywords") or [])
    patterns = tuple(
        _compile(p) for p in (data.get("content_patterns") or [])
    )
    return desc, content_kw, patterns


def load_guidance_context() -> Dict[str, Any]:
    """Load period, vs_prior, results_context, guidance_context patterns."""
    data = _load_yaml("8k_guidance-context.yaml")
    if not data:
        return {}

    out: Dict[str, Any] = {}

    periods = data.get("periods") or {}
    if periods:
        out["fy_period"] = _compile(periods.get("fy", {}).get("regex", ""), "I")
        out["q_period"] = _compile(periods.get("quarter", {}).get("regex", ""), "I")

    vs = data.get("vs_prior") or []
    if isinstance(vs, list):
        out["vs_prior"] = [
            (p.get("label"), _compile(p.get("regex", ""), "I"))
            for p in vs
            if isinstance(p, dict) and p.get("label") != "historical"
        ]
        hist = next((p for p in vs if isinstance(p, dict) and p.get("label") == "historical"), None)
        out["vs_prior_historical"] = _compile(hist.get("regex", ""), "I") if hist else None

    out["results_context"] = tuple(
        _compile(p, "I") for p in (data.get("results_context") or [])
    )
    out["guidance_context"] = tuple(
        _compile(p, "I") for p in (data.get("guidance_context") or [])
    )
    return out


def load_revenue_rules() -> Dict[str, Any]:
    """Load revenue extraction patterns and metric/exclude rules."""
    data = _load_yaml("8k_guidance-revenues.yaml")
    if not data:
        return {}

    out: Dict[str, Any] = {}
    pats = data.get("patterns") or {}
    if "revenue_range" in pats:
        out["revenue_range"] = _compile(pats["revenue_range"].get("regex", ""), "I")
    if "revenue_single" in pats:
        out["revenue_single"] = _compile(pats["revenue_single"].get("regex", ""), "I")

    metrics = data.get("metric_patterns") or []
    out["metric_patterns"] = [
        (m["name"], _compile(m["regex"], "I"))
        for m in metrics
        if isinstance(m, dict) and "name" in m and "regex" in m
    ]
    out["exclude_metric"] = tuple(
        _compile(p, "I") for p in (data.get("exclude_after_match") or [])
    )
    return out


def load_earnings_rules() -> Dict[str, re.Pattern]:
    """Load earnings extraction patterns (eps_range, eps_single, net_income_*)."""
    data = _load_yaml("8k_guidance-earnings.yaml")
    if not data:
        return {}

    out: Dict[str, re.Pattern] = {}
    for key in ("eps_range", "eps_single", "net_income_qualitative", "net_income_range"):
        p = (data.get("patterns") or {}).get(key)
        if p and isinstance(p, dict) and p.get("regex"):
            out[key] = _compile(p["regex"], p.get("flags", "I"))
    return out


def load_margins_rules() -> Dict[str, re.Pattern]:
    """Load margin extraction patterns."""
    data = _load_yaml("8k_guidance-margins.yaml")
    if not data:
        return {}

    out: Dict[str, re.Pattern] = {}
    mapping = {
        "gross_margin_adj_range": "gross_margin_adj_range",
        "gross_margin_adj_single": "gross_margin_adj_single",
        "gross_margin_adj_at_least": "gross_margin_adj_at_least",
        "gross_margin_gaap_single": "gross_margin_gaap_single",
        "gross_margin_both": "gross_margin_both",
    }
    for yaml_key, out_key in mapping.items():
        p = (data.get("patterns") or {}).get(yaml_key)
        if p and isinstance(p, dict) and p.get("regex"):
            out[out_key] = _compile(p["regex"], p.get("flags", "I"))
    return out


def has_rules() -> bool:
    """Return True if YAML rules exist and can be loaded."""
    try:
        import yaml  # noqa: F401
    except ImportError:
        return False
    return (_RULES_DIR / "8k_guidance-detection.yaml").exists()


def load_all_rules() -> Optional[Dict[str, Any]]:
    """
    Load all extraction rules from YAML. Returns None if any required file is missing.
    Returns a dict with keys matching the pattern vars used by filings_scraper_html_heuristics.
    """
    required_files = ("8k_guidance-context.yaml", "8k_guidance-revenues.yaml", "8k_guidance-earnings.yaml", "8k_guidance-margins.yaml")
    for name in required_files:
        if not (_RULES_DIR / name).exists():
            return None

    ctx = load_guidance_context()
    rev = load_revenue_rules()
    earn = load_earnings_rules()
    marg = load_margins_rules()

    rules: Dict[str, Any] = {
        "fy_period": ctx.get("fy_period"),
        "q_period": ctx.get("q_period"),
        "vs_prior": tuple(ctx.get("vs_prior", [])),
        "vs_prior_historical": ctx.get("vs_prior_historical"),
        "results_context": ctx.get("results_context", ()),
        "guidance_context": ctx.get("guidance_context", ()),
        "revenue_range": rev.get("revenue_range"),
        "revenue_single": rev.get("revenue_single"),
        "metric_patterns": tuple(rev.get("metric_patterns", [])),
        "exclude_metric": rev.get("exclude_metric", ()),
        "eps_range": earn.get("eps_range"),
        "eps_single": earn.get("eps_single"),
        "net_income_qualitative": earn.get("net_income_qualitative"),
        "net_income_range": earn.get("net_income_range"),
        "gross_margin_adj_range": marg.get("gross_margin_adj_range"),
        "gross_margin_adj_single": marg.get("gross_margin_adj_single"),
        "gross_margin_adj_at_least": marg.get("gross_margin_adj_at_least"),
        "gross_margin_gaap_single": marg.get("gross_margin_gaap_single"),
        "gross_margin_both": marg.get("gross_margin_both"),
    }
    # All single-pattern keys must be non-None
    for k in ("fy_period", "q_period", "vs_prior_historical", "revenue_range", "revenue_single",
              "eps_range", "eps_single", "net_income_qualitative", "net_income_range",
              "gross_margin_adj_range", "gross_margin_adj_single", "gross_margin_adj_at_least",
              "gross_margin_gaap_single", "gross_margin_both"):
        if rules.get(k) is None:
            return None
    return rules
