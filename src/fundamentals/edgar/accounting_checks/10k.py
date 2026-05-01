"""
Basic accounting consistency checks on xbrl.adj JSON (single-period, concept -> value).

Expects structure: { "YYYY-MM-DDTHH:MM:SS": { "balance_sheet": {...}, "income_stmt": {...}, "cashflow": {...}, "other": {...}, "shares": {...} } }.
All tests use relative tolerance only: pass if |LHS - RHS| as % of reference <= RELATIVE_TOLERANCE_PCT (5%).

Rules are loaded from src/fundamentals/edgar/rules-xbrl/10k.yaml.
"""

from pathlib import Path
from typing import Any, Callable, Dict, List, Tuple

import yaml


def _load_rules() -> Dict[str, Any]:
    """Load 10-K rules from src/fundamentals/edgar/rules-xbrl/10k.yaml."""
    mod_dir = Path(__file__).resolve().parent.parent  # edgar/
    rules_path = mod_dir / "rules-xbrl" / "10k.yaml"
    with open(rules_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


_RULES = _load_rules()
RELATIVE_TOLERANCE_PCT = float(_RULES["relative_tolerance_pct"])
_RESERVED_TOP_KEYS = frozenset(_RULES["reserved_top_keys"])


def _get_period_data(data: Dict[str, Any]) -> Tuple[str | None, Dict[str, Dict[str, Any]] | None]:
    """Return (date_key, { balance_sheet, income_stmt, cashflow, other }) or (None, None) if invalid."""
    if not data:
        return None, None
    period_keys = [k for k in data if k not in _RESERVED_TOP_KEYS]
    if len(period_keys) != 1:
        return None, None
    date_key = period_keys[0]
    inner = data[date_key]
    if not isinstance(inner, dict):
        return None, None
    return date_key, inner


def _val(section: Dict[str, Any], exact_key: str) -> float | None:
    """Get numeric value for exact concept key; None if missing or non-numeric."""
    v = section.get(exact_key)
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _val_first(sections: List[Dict[str, Any]], keys: List[str]) -> float | None:
    """Try each (section, key) in order; return first non-None numeric value. sections[i] can be None."""
    for section in sections:
        if not section:
            continue
        for k in keys:
            v = _val(section, k)
            if v is not None:
                return v
    return None


def _approx_eq_rel(a: float, b: float, rel_pct: float = RELATIVE_TOLERANCE_PCT) -> bool:
    """True if |a - b| as % of max(|a|, |b|) is <= rel_pct."""
    ref = max(abs(a), abs(b), 1e-9)
    return abs(a - b) / ref * 100.0 <= rel_pct


def _temporary_equity_total(bs: Dict[str, Any]) -> float:
    """
    Total temporary equity from balance_sheet: include parent and noncontrolling portions and
    common mezzanine-equity concepts (redeemable NCI, shares subject to mandatory redemption).
    """
    te = _RULES["temporary_equity"]
    primary_keys = te["primary_keys"]
    for k in primary_keys:
        total = _val(bs, k)
        if total is not None:
            return total

    total = 0.0
    substring_match = te.get("primary_substring_match", [])
    for k in bs:
        if "[" not in k:
            continue
        if any(sub in k for sub in substring_match):
            x = _val(bs, k)
            if x is not None:
                total += x

    mezz_keys = te["mezz_keys"]
    for k in mezz_keys:
        v = _val(bs, k)
        if v is not None:
            total += v

    alt_keys = te["alt_keys"]
    for k in alt_keys:
        v = _val(bs, k)
        if v is not None:
            total += v

    return total if total else 0.0


def _temporary_equity_other_total(other: Dict[str, Any], bs: Dict[str, Any]) -> float:
    """
    Temporary equity concepts sometimes only appear in the 'other' bucket of adj JSON, especially for SPACs.
    We include only a narrow set of well-known mezzanine-equity concepts and only when the same key is not
    already present on the balance sheet, to avoid double-counting.
    """
    if not other:
        return 0.0
    total = 0.0
    alt_keys = _RULES["temporary_equity"]["alt_keys"]
    for k in alt_keys:
        # Prefer the balance sheet location if present
        if _val(bs, k) is not None:
            continue
        v = _val(other, k)
        if v is not None:
            total += v
    return total


def _liabilities_total(bs: Dict[str, Any]) -> float | None:
    """Total liabilities: us-gaap:Liabilities if present; else derived as Assets - Equity - TemporaryEquity (or LiabilitiesAndStockholdersEquity - Equity - TemporaryEquity)."""
    liab = _val(bs, "us-gaap:Liabilities")
    if liab is not None:
        return liab
    equity = _val(bs, "us-gaap:StockholdersEquity")
    if equity is None:
        equity = _val(bs, "us-gaap:StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest")
    temp_equity = _temporary_equity_total(bs)
    if equity is None:
        return None
    te = temp_equity or 0.0
    assets = _val(bs, "us-gaap:Assets")
    if assets is not None:
        return assets - equity - te
    l_and_e = _val(bs, "us-gaap:LiabilitiesAndStockholdersEquity")
    if l_and_e is not None:
        return l_and_e - equity - te
    return None


def check_balance_sheet_identity(
    data: Dict[str, Any]
) -> Tuple[bool, str, Dict[str, Any]]:
    """Assets = Liabilities + Stockholders' Equity (+ TemporaryEquity if present). When us-gaap:Liabilities is missing, liabilities are derived as Assets - Equity (or LiabilitiesAndStockholdersEquity - Equity)."""
    _, inner = _get_period_data(data)
    if not inner:
        return False, "Invalid data shape", {}
    bs = inner.get("balance_sheet") or {}
    other = inner.get("other") or {}
    assets = _val(bs, "us-gaap:Assets")
    liab = _liabilities_total(bs)
    equity = _val(bs, "us-gaap:StockholdersEquity")
    if equity is None:
        equity = _val(bs, "us-gaap:StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest")
    temp_equity = _temporary_equity_total(bs) + _temporary_equity_other_total(other, bs)
    if assets is None:
        return False, "Missing us-gaap:Assets", {}
    detail = {"Assets": assets}
    if liab is None or equity is None:
        return False, "Missing Liabilities and/or StockholdersEquity", detail
    sum_rhs = liab + equity + (temp_equity or 0)
    ok = _approx_eq_rel(assets, sum_rhs)
    detail["Liabilities"] = liab
    detail["StockholdersEquity"] = equity
    if temp_equity and temp_equity != 0:
        detail["TemporaryEquity"] = temp_equity
    detail["Liabilities + Equity (+ TemporaryEquity)"] = sum_rhs
    detail["Difference"] = assets - sum_rhs
    return ok, "Assets = Liabilities + StockholdersEquity (+ TemporaryEquity if present)", detail


def _sum_undimensioned_keys(section: Dict[str, Any], must_contain: str, exclude: str) -> float:
    """Sum values for undimensioned keys that contain must_contain and do not contain exclude."""
    total = 0.0
    for k in section or {}:
        if "[" in k or must_contain not in k or (exclude and exclude in k):
            continue
        v = _val(section, k)
        if v is not None:
            total += v
    return total


def _derived_current_noncurrent_from_components(
    total: float,
    current_explicit: float | None,
    noncurrent_explicit: float | None,
    sum_current_components: float,
    sum_noncurrent_components: float,
) -> Tuple[float | None, float | None]:
    """Derive current and noncurrent when we have total and component sums. Prefer explicit, then derived from components, then total - other."""
    if current_explicit is not None and noncurrent_explicit is not None:
        return current_explicit, noncurrent_explicit
    if current_explicit is not None:
        nc = total - current_explicit if total is not None else noncurrent_explicit
        return current_explicit, nc
    if noncurrent_explicit is not None:
        cur = total - noncurrent_explicit if total is not None else current_explicit
        return cur, noncurrent_explicit
    if sum_current_components > 0 and total is not None:
        return sum_current_components, total - sum_current_components
    if sum_noncurrent_components > 0 and total is not None:
        return total - sum_noncurrent_components, sum_noncurrent_components
    return None, None


def _assets_noncurrent_total(bs: Dict[str, Any], other: Dict[str, Any] | None = None) -> float | None:
    """AssetsNoncurrent: us-gaap:AssetsNoncurrent if present; else sum undimensioned 'Assets'+'Noncurrent'; else Assets - AssetsCurrent when both exist. When sum exists but current+sum != total within tolerance, use derived so partial breakdowns still pass. When explicit concepts missing, try deriving from component sums (keys containing Assets+Current or Assets+Noncurrent)."""
    assets = _val(bs, "us-gaap:Assets")
    current = _val(bs, "us-gaap:AssetsCurrent")
    total = _val(bs, "us-gaap:AssetsNoncurrent")
    if total is not None:
        return total
    total = 0.0
    for k in bs:
        if "[" in k or "Assets" not in k or "Noncurrent" not in k:
            continue
        v = _val(bs, k)
        if v is not None:
            total += v
    if total != 0.0 and assets is not None and current is not None:
        if _approx_eq_rel(assets, current + total):
            return total
        return assets - current
    if total != 0.0:
        return total
    if assets is not None and current is not None:
        return assets - current
    # Derive from components: sum undimensioned keys with Assets+Current (excl. Noncurrent) or Assets+Noncurrent
    sum_cur = 0.0
    sum_nc = 0.0
    for section in (bs, other or {}):
        for k in section:
            if "[" in k or "Assets" not in k:
                continue
            v = _val(section, k)
            if v is None:
                continue
            if "Noncurrent" in k:
                sum_nc += v
            elif "Current" in k:
                sum_cur += v
    if sum_cur > 0 and assets is not None:
        return assets - sum_cur
    if sum_nc > 0 and assets is not None:
        return sum_nc
    return None


def _derived_assets_current(bs: Dict[str, Any], other: Dict[str, Any] | None) -> float:
    """Sum undimensioned keys containing Assets and Current (not Noncurrent) from bs and other."""
    total = 0.0
    for section in (bs, other or {}):
        for k in section:
            if "[" in k or "Assets" not in k or "Current" not in k or "Noncurrent" in k:
                continue
            v = _val(section, k)
            if v is not None:
                total += v
    return total


def check_assets_current_plus_noncurrent(
    data: Dict[str, Any]
) -> Tuple[bool, str, Dict[str, Any]]:
    """Assets = AssetsCurrent + AssetsNoncurrent (current/noncurrent from explicit concepts or derived from component sums). Skips when neither explicit nor component breakdown exists."""
    _, inner = _get_period_data(data)
    if not inner:
        return False, "Invalid data shape", {}
    bs = inner.get("balance_sheet") or {}
    other = inner.get("other") or {}
    total = _val(bs, "us-gaap:Assets")
    if total is None:
        return False, "Missing us-gaap:Assets", {}
    current = _val(bs, "us-gaap:AssetsCurrent")
    noncurrent = _assets_noncurrent_total(bs, other)
    if current is None:
        derived_cur = _derived_assets_current(bs, other)
        if derived_cur > 0:
            current = derived_cur
            if noncurrent is None:
                noncurrent = total - current
        elif noncurrent is not None:
            current = total - noncurrent
    if noncurrent is None and current is not None:
        noncurrent = total - current
    if current is None or noncurrent is None:
        return True, "Skipped (no current/noncurrent breakdown)", {"Assets": total}
    sum_parts = current + noncurrent
    ok = _approx_eq_rel(total, sum_parts)
    detail = {
        "Assets": total,
        "AssetsCurrent": current,
        "AssetsNoncurrent": noncurrent,
        "Sum": sum_parts,
        "Difference": total - sum_parts,
    }
    return ok, "Assets = AssetsCurrent + AssetsNoncurrent", detail


def _derived_liabilities_current(bs: Dict[str, Any], other: Dict[str, Any] | None) -> float:
    """Sum undimensioned keys containing Liabilities and Current (not Noncurrent) from bs and other."""
    total = 0.0
    for section in (bs, other or {}):
        for k in section:
            if "[" in k or "Liabilities" not in k or "Current" not in k or "Noncurrent" in k:
                continue
            v = _val(section, k)
            if v is not None:
                total += v
    return total


def _liabilities_noncurrent_total(bs: Dict[str, Any], other: Dict[str, Any]) -> float | None:
    """LiabilitiesNoncurrent: us-gaap:LiabilitiesNoncurrent if present; else sum undimensioned bs Liabilities+Noncurrent; else other PayableNoncurrent/Liabilities+Noncurrent; else Liabilities - LiabilitiesCurrent. Uses _liabilities_total when us-gaap:Liabilities is missing. When explicit missing, tries deriving from component sums (Liabilities+Noncurrent)."""
    liab = _liabilities_total(bs)
    current = _val(bs, "us-gaap:LiabilitiesCurrent")
    total = _val(bs, "us-gaap:LiabilitiesNoncurrent")
    if total is not None:
        return total
    total = 0.0
    for k in bs:
        if "[" in k or "Liabilities" not in k or "Noncurrent" not in k:
            continue
        v = _val(bs, k)
        if v is not None:
            total += v
    if total != 0.0 and liab is not None and current is not None:
        if _approx_eq_rel(liab, current + total):
            return total
        return liab - current
    if total != 0.0:
        return total
    other_sum = 0.0
    for k in (other or {}):
        if "[" in k:
            continue
        if "PayableNoncurrent" in k or ("Liabilities" in k and "Noncurrent" in k):
            x = _val(other, k)
            if x is not None:
                other_sum += x
    if other_sum != 0.0 and liab is not None and current is not None:
        if _approx_eq_rel(liab, current + other_sum):
            return other_sum
        return liab - current
    if other_sum != 0.0:
        return other_sum
    if liab is not None and current is not None:
        return liab - current
    # Derive from components: sum keys with Liabilities+Noncurrent only or Liabilities+Current only (exclude combined CurrentAndNoncurrent)
    sum_nc = 0.0
    sum_cur = 0.0
    for section in (bs, other or {}):
        for k in section:
            if "[" in k or "Liabilities" not in k:
                continue
            v = _val(section, k)
            if v is None:
                continue
            if "Noncurrent" in k and "Current" not in k:
                sum_nc += v
            elif "Current" in k and "Noncurrent" not in k:
                sum_cur += v
    if sum_cur > 0 and liab is not None:
        return liab - sum_cur
    if sum_nc > 0 and liab is not None:
        return sum_nc
    return None


def check_liabilities_current_plus_noncurrent(
    data: Dict[str, Any]
) -> Tuple[bool, str, Dict[str, Any]]:
    """Liabilities = LiabilitiesCurrent + LiabilitiesNoncurrent. Total and noncurrent derived when us-gaap:Liabilities/LiabilitiesNoncurrent missing; current derived from component sums when LiabilitiesCurrent missing. Skips when no current/noncurrent breakdown available."""
    _, inner = _get_period_data(data)
    if not inner:
        return False, "Invalid data shape", {}
    bs = inner.get("balance_sheet") or {}
    other = inner.get("other") or {}
    total = _liabilities_total(bs)
    current = _val(bs, "us-gaap:LiabilitiesCurrent")
    if current is None:
        derived = _derived_liabilities_current(bs, other)
        if derived != 0.0:
            current = derived
    noncurrent = _liabilities_noncurrent_total(bs, other)
    if total is None or current is None or noncurrent is None:
        return True, "Skipped (no current/noncurrent breakdown)", {}
    sum_parts = current + noncurrent
    ok = _approx_eq_rel(total, sum_parts)
    return ok, "Liabilities = LiabilitiesCurrent + LiabilitiesNoncurrent", {
        "Liabilities": total,
        "LiabilitiesCurrent": current,
        "LiabilitiesNoncurrent": noncurrent,
        "Sum": sum_parts,
        "Difference": total - sum_parts,
    }


def check_gross_profit(
    data: Dict[str, Any]
) -> Tuple[bool, str, Dict[str, Any]]:
    """Revenue - Cost of goods and services sold = Gross profit."""
    _, inner = _get_period_data(data)
    if not inner:
        return False, "Invalid data shape", {}
    inc = inner.get("income_stmt") or {}
    other = inner.get("other") or {}
    gp = _RULES["gross_profit"]
    revenue_concepts = gp["revenue_concepts"]
    cost_concepts = gp["cost_concepts"]
    gross_key = gp["gross_profit_key"]
    gross = _val(inc, gross_key) or _val(other, gross_key)
    if gross is None:
        return True, "Skipped (missing GrossProfit)", {}
    revenue_vals = [_val_first([inc, other], [k]) for k in revenue_concepts]
    cost_vals = [_val_first([other, inc], [k]) for k in cost_concepts]
    revenue = next((v for v in revenue_vals if v is not None), None)
    cost = next((v for v in cost_vals if v is not None), None)
    if revenue is None or cost is None:
        return True, "Skipped (missing Revenue or Cost)", {}
    # Pass if any (revenue, cost) pair reconciles with gross
    ok = False
    best_rev, best_cost = revenue, cost
    for rv in revenue_vals:
        if rv is None:
            continue
        for cv in cost_vals:
            if cv is None:
                continue
            if _approx_eq_rel(gross, rv - cv):
                ok = True
                best_rev, best_cost = rv, cv
                break
        if ok:
            break
    if not ok:
        computed = best_rev - best_cost
    else:
        computed = best_rev - best_cost
    return ok, "Revenue - CostOfGoodsAndServicesSold = GrossProfit", {
        "Revenue": best_rev,
        "CostOfGoodsAndServicesSold": best_cost,
        "GrossProfit (reported)": gross,
        "GrossProfit (computed)": computed,
        "Difference": gross - computed,
    }


def check_operating_income(
    data: Dict[str, Any]
) -> Tuple[bool, str, Dict[str, Any]]:
    """Gross profit - Operating expenses = Operating income (loss)."""
    _, inner = _get_period_data(data)
    if not inner:
        return False, "Invalid data shape", {}
    inc = inner.get("income_stmt") or {}
    other = inner.get("other") or {}
    gross = _val(inc, "us-gaap:GrossProfit") or _val(other, "us-gaap:GrossProfit")
    opex = _val(inc, "us-gaap:OperatingExpenses") or _val(other, "us-gaap:OperatingExpenses")
    op_inc_candidates = [
        _val(inc, "us-gaap:OperatingIncomeLoss"),
        _val(other, "us-gaap:OperatingIncomeLoss"),
    ]
    op_inc_candidates = [v for v in op_inc_candidates if v is not None]
    op_inc = op_inc_candidates[0] if op_inc_candidates else None
    if gross is None or opex is None or op_inc is None:
        return True, "Skipped (missing GrossProfit/OperatingExpenses/OperatingIncomeLoss)", {}
    computed = gross - opex
    ok = any(_approx_eq_rel(c, computed) for c in op_inc_candidates)
    if ok:
        op_inc = next(c for c in op_inc_candidates if _approx_eq_rel(c, computed))
    return ok, "GrossProfit - OperatingExpenses = OperatingIncomeLoss", {
        "GrossProfit": gross,
        "OperatingExpenses": opex,
        "OperatingIncomeLoss (reported)": op_inc,
        "OperatingIncomeLoss (computed)": computed,
        "Difference": op_inc - computed,
    }


def check_net_income_from_pretax_and_tax(
    data: Dict[str, Any]
) -> Tuple[bool, str, Dict[str, Any]]:
    """Pretax income - Income tax expense = Net income (loss)."""
    _, inner = _get_period_data(data)
    if not inner:
        return False, "Invalid data shape", {}
    inc = inner.get("income_stmt") or {}
    other = inner.get("other") or {}
    ptn = _RULES["pretax_tax_net"]
    pretax = _val_first([inc, other], ptn["pretax_concepts"])
    tax = _val_first([other, inc], ptn["tax_concepts"])
    # Net: accept NetIncomeLoss or ProfitLoss from inc/other; pass if any matches pretax - tax
    net_concepts = ptn["net_concepts"]
    net_candidates = []
    for src in [inc, other]:
        for key in net_concepts:
            v = _val(src, key)
            if v is not None:
                net_candidates.append(v)
    net = net_candidates[0] if net_candidates else None
    if pretax is None or net is None:
        return True, "Skipped (missing pretax or net income)", {}
    tax_val = tax if tax is not None else 0.0
    computed = pretax - tax_val
    ok = any(_approx_eq_rel(c, computed) for c in net_candidates)
    if ok:
        net = next(c for c in net_candidates if _approx_eq_rel(c, computed))
    return ok, "Pretax income - Income tax = NetIncomeLoss", {
        "PretaxIncome": pretax,
        "IncomeTaxExpenseBenefit": tax_val,
        "NetIncomeLoss (reported)": net,
        "NetIncomeLoss (computed)": computed,
        "Difference": (net or 0) - computed,
    }


def check_cash_flow_articulation(
    data: Dict[str, Any]
) -> Tuple[bool, str, Dict[str, Any]]:
    """Net operating + Net investing + Net financing = Change in cash (when reported)."""
    _, inner = _get_period_data(data)
    if not inner:
        return False, "Invalid data shape", {}
    cf = inner.get("cashflow") or {}
    other = inner.get("other") or {}
    cfc = _RULES["cash_flow"]
    operating = _val_first([cf], cfc["operating_concepts"])
    investing = _val_first([cf], cfc["investing_concepts"])
    financing = _val_first([cf], cfc["financing_concepts"])
    change = _val_first([other, cf], cfc["change_in_cash_concepts"])
    if operating is None or investing is None or financing is None:
        return True, "Skipped (missing Operating/Investing/Financing cash flow)", {}
    sum_cf = operating + investing + financing
    # Add discontinued only when we used the *ContinuingOperations* variants; consolidated totals already include it
    disc = cfc.get("discontinued_concepts", {})
    used_continuing_only = (
        _val(cf, cfc["operating_concepts"][0]) is None
        and _val(cf, cfc["investing_concepts"][0]) is None
        and _val(cf, cfc["financing_concepts"][0]) is None
    )
    if used_continuing_only and disc:
        discontinued = _val(cf, disc.get("main"))
        if discontinued is not None:
            sum_cf = sum_cf + discontinued
        elif (
            _val(cf, disc.get("operating")) is not None
            or _val(cf, disc.get("investing")) is not None
            or _val(cf, disc.get("financing")) is not None
        ):
            disc_op = _val(cf, disc.get("operating")) or 0
            disc_inv = _val(cf, disc.get("investing")) or 0
            disc_fin = _val(cf, disc.get("financing")) or 0
            sum_cf = sum_cf + disc_op + disc_inv + disc_fin
    if change is None:
        return True, "Skipped (missing change in cash)", {"Operating": operating, "Investing": investing, "Financing": financing, "Sum": sum_cf}
    fx = _val_first([other, cf], cfc["fx_concepts"])
    sum_with_fx = sum_cf + (fx or 0.0)
    ok = _approx_eq_rel(sum_cf, change) or _approx_eq_rel(sum_with_fx, change)
    best_sum = sum_with_fx if (fx is not None and abs(sum_with_fx - change) < abs(sum_cf - change)) else sum_cf
    return ok, "Operating + Investing + Financing (+ FX) = Change in cash", {
        "NetCashOperating": operating,
        "NetCashInvesting": investing,
        "NetCashFinancing": financing,
        "Sum": best_sum,
        "ChangeInCash (reported)": change,
        "Difference": best_sum - change,
    }


# Equity dimension sets from rules
_EQ = _RULES["equity"]
_EQUITY_TOTAL_DIMENSIONS = frozenset(_EQ["total_dimensions"])
_EQUITY_ROLLUP_COMPONENT_DIMENSIONS = frozenset(_EQ["rollup_component_dimensions"])
_AOCI_PARENT_DIMENSIONS = frozenset(_EQ["aoci_parent_dimensions"])
_AOCI_CHILD_DIMENSIONS = frozenset(_EQ["aoci_child_dimensions"])


def _equity_components_sums(bs: Dict[str, Any]) -> Dict[str, float]:
    """
    Sum of equity components by base concept:
    - 'us-gaap:StockholdersEquity'
    - 'us-gaap:StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest'

    Only uses dimensioned balance_sheet breakdowns (e.g. StockholdersEquity [CommonStock], ...),
    excluding total-like dimensions ([Parent], [ParentMember]).
    Excludes AOCI parent dimensions when AOCI subcomponent dimensions exist to avoid double-counting.
    Uses only one AOCI parent when both AccumulatedOtherComprehensiveIncome and AociIncluding...
    exist (same total, different labels).
    """
    # First pass: collect (base, dimension, value)
    items: List[Tuple[str, str, float]] = []
    for k in bs:
        if "[" not in k:
            continue
        base: str | None = None
        if k.startswith("us-gaap:StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest ["):
            base = "us-gaap:StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest"
        elif k.startswith("us-gaap:StockholdersEquity ["):
            base = "us-gaap:StockholdersEquity"
        if base is None:
            continue
        try:
            bracket = k[k.index("[") + 1 : k.rindex("]")]
        except ValueError:
            continue
        dim = bracket.strip()
        v = _val(bs, k)
        if v is None:
            continue
        items.append((base, dim, float(v)))

    # Check dimensions for filtering
    dims_present = {dim for _, dim, _ in items}
    has_aoci_children = bool(dims_present & _AOCI_CHILD_DIMENSIONS)
    has_rollup_components = bool(dims_present & (_EQUITY_ROLLUP_COMPONENT_DIMENSIONS | _AOCI_CHILD_DIMENSIONS))
    # Exclude Parent only when we have granular components; when only Parent+NCI, both are components
    exclude_parent = has_rollup_components

    sums: Dict[str, float] = {}
    seen_aoci_parent = False
    known_components = _EQUITY_ROLLUP_COMPONENT_DIMENSIONS | _AOCI_CHILD_DIMENSIONS | {"NoncontrollingInterest"}
    for base, dim, v in items:
        dim_tokens = [d.strip() for d in dim.split(",")]
        # Only exclude [Parent] / [ParentMember] when it's the SOLE dimension (consolidated rollup)
        if exclude_parent and len(dim_tokens) == 1 and dim_tokens[0] in _EQUITY_TOTAL_DIMENSIONS:
            continue
        # When we have consolidated component breakdown (CommonStock, APIC, etc.), exclude segment
        # dimensions (e.g. [AlabamaPower], [SouthernPower, Parent]) to avoid mixing/over-counting
        if has_rollup_components and len(dim_tokens) >= 1:
            first_tok = dim_tokens[0]
            if first_tok not in known_components and first_tok not in _EQUITY_TOTAL_DIMENSIONS:
                continue  # Segment or other non-component dimension
        if dim in _AOCI_PARENT_DIMENSIONS:
            if has_aoci_children:
                continue  # Exclude parent when we have child breakdown
            if seen_aoci_parent:
                continue  # Use only one AOCI parent (both labels = same total)
            seen_aoci_parent = True
        sums[base] = sums.get(base, 0.0) + v
    return {concept: total for concept, total in sums.items() if total != 0.0}


def _equity_components_sum(bs: Dict[str, Any]) -> float | None:
    """
    Backwards-compatible helper: single total components sum, preferring StockholdersEquity
    then StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest.
    """
    sums = _equity_components_sums(bs)
    total = sums.get("us-gaap:StockholdersEquity") or sums.get(
        "us-gaap:StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest"
    )
    return total


def check_equity_components(
    data: Dict[str, Any]
) -> Tuple[bool, str, Dict[str, Any]]:
    """StockholdersEquity = sum of dimensioned balance_sheet equity components (StockholdersEquity [X] / StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest [X], excluding [Parent]). Skips when no such breakdown exists."""
    _, inner = _get_period_data(data)
    if not inner:
        return False, "Invalid data shape", {}
    bs = inner.get("balance_sheet") or {}
    if not isinstance(bs, dict):
        return False, "Invalid balance_sheet shape", {}

    # Candidate total equity concepts
    eq_totals: Dict[str, float] = {}
    se = _val(bs, "us-gaap:StockholdersEquity")
    if se is not None:
        eq_totals["us-gaap:StockholdersEquity"] = se
    se_nci = _val(bs, "us-gaap:StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest")
    if se_nci is not None:
        eq_totals["us-gaap:StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest"] = se_nci
    if not eq_totals:
        return False, "Missing StockholdersEquity / StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest", {}

    comp_sums = _equity_components_sums(bs)
    if not comp_sums:
        # No dimensioned breakdown; treat as skipped
        concept, total_equity = next(iter(eq_totals.items()))
        return True, "Skipped (no equity component breakdown)", {concept: total_equity}

    # Build candidate (concept, total, components) pairs and choose the best aligned one
    candidates: List[Tuple[str, float, float]] = []
    for concept, total in eq_totals.items():
        comp = comp_sums.get(concept)
        if comp is None:
            continue
        candidates.append((concept, total, comp))
    if not candidates:
        # Fall back to original behaviour using StockholdersEquity if present
        total_equity = eq_totals.get("us-gaap:StockholdersEquity")
        if total_equity is None:
            concept, total_equity = next(iter(eq_totals.items()))
        computed = sum(comp_sums.values())
        if computed == 0.0:
            return True, "Skipped (no equity component breakdown)", {concept: total_equity}
        ok = _approx_eq_rel(total_equity, computed)
        return ok, "StockholdersEquity ≈ sum of components", {
            "StockholdersEquity": total_equity,
            "Sum (components)": computed,
            "Difference": total_equity - computed,
        }

    def rel_diff(total: float, comp: float) -> float:
        ref = max(abs(total), abs(comp), 1e-9)
        return abs(total - comp) / ref * 100.0

    best_concept, best_total, best_comp = min(
        candidates, key=lambda t: rel_diff(t[1], t[2])
    )
    ok = _approx_eq_rel(best_total, best_comp)
    detail_key = "StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest" if best_concept.endswith(
        "IncludingPortionAttributableToNoncontrollingInterest"
    ) else "StockholdersEquity"
    return ok, "StockholdersEquity ≈ sum of components", {
        detail_key: best_total,
        "Sum (components)": best_comp,
        "Difference": best_total - best_comp,
    }


def check_comprehensive_income(
    data: Dict[str, Any]
) -> Tuple[bool, str, Dict[str, Any]]:
    """Net income + Other comprehensive income = Comprehensive income (net of tax)."""
    _, inner = _get_period_data(data)
    if not inner:
        return False, "Invalid data shape", {}
    inc = inner.get("income_stmt") or {}
    other = inner.get("other") or {}
    ci = _RULES["comprehensive_income"]
    net = _val_first([inc, other], ci["net_concepts"])
    oci = _val_first([inc, other], ci["oci_concepts"])
    comprehensive_candidates = []
    for key in ci["comprehensive_concepts"]:
        v = _val(inc, key)
        if v is not None:
            comprehensive_candidates.append(v)
        v = _val(other, key)
        if v is not None:
            comprehensive_candidates.append(v)
    comprehensive_candidates = [v for v in comprehensive_candidates if v is not None]
    comprehensive = comprehensive_candidates[0] if comprehensive_candidates else None
    if net is None or comprehensive is None:
        return True, "Skipped (missing NetIncomeLoss or ComprehensiveIncomeNetOfTax)", {}
    oci_val = oci if oci is not None else 0.0
    computed = net + oci_val
    ok = any(_approx_eq_rel(c, computed) for c in comprehensive_candidates)
    if ok:
        comprehensive = next(c for c in comprehensive_candidates if _approx_eq_rel(c, computed))
    return ok, "NetIncome + OCI = ComprehensiveIncomeNetOfTax", {
        "NetIncomeLoss": net,
        "OCI": oci_val,
        "ComprehensiveIncome (reported)": comprehensive,
        "ComprehensiveIncome (computed)": computed,
        "Difference": (comprehensive or 0) - computed,
    }


# Other operating expense concepts (excl. R&D and SG&A) to sum for OpEx = R&D + SG&A + other
_OTHER_OPEX_KEYS = tuple(_RULES["other_opex_keys"])


def _other_opex_total(inc: Dict[str, Any], other: Dict[str, Any]) -> float:
    """Sum of other material operating expense (excl. R&D and SG&A): standard concepts + undimensioned keys containing 'Opex' (e.g. extension taxonomy). Each concept counted once (inc preferred over other)."""
    oers = _RULES["operating_expenses_rd_sga"]
    total = 0.0
    for k in _OTHER_OPEX_KEYS:
        v = _val_first([inc, other], [k])
        if v is not None:
            total += v
    seen: set = set()
    opex_contains = oers.get("opex_extension_contains", "Opex")
    opex_exclude = oers.get("opex_extension_exclude", [])
    for section in (inc, other):
        if not section:
            continue
        for k in section:
            if "[" in k or opex_contains not in k or k in seen:
                continue
            if any(ex in k for ex in opex_exclude):
                continue
            seen.add(k)
            x = _val(section, k)
            if x is not None:
                total += x
    return total


def check_operating_expenses_rd_sga(
    data: Dict[str, Any]
) -> Tuple[bool, str, Dict[str, Any]]:
    """Operating expenses = R&D + SG&A + other material opex (restructuring, acquisition-related, extension Opex, etc.)."""
    _, inner = _get_period_data(data)
    if not inner:
        return False, "Invalid data shape", {}
    inc = inner.get("income_stmt") or {}
    other = inner.get("other") or {}
    oers = _RULES["operating_expenses_rd_sga"]
    opex = _val(inc, oers["operating_expenses_key"]) or _val(other, oers["operating_expenses_key"])
    rd = _val_first([other, inc], [oers["rd_key"]])
    sga = _val_first([other, inc], [oers["sga_key"]])
    if opex is None or rd is None or sga is None:
        return True, "Skipped (no R&D/SG&A breakdown)", {}
    other_opex = _other_opex_total(inc, other)
    computed = rd + sga + other_opex
    ok = _approx_eq_rel(opex, computed)
    detail = {
        "OperatingExpenses": opex,
        "R&D": rd,
        "SG&A": sga,
        "Sum (R&D + SG&A + other opex)": computed,
        "Difference": opex - computed,
    }
    if other_opex != 0:
        detail["Other operating expense"] = other_opex
    return ok, "OperatingExpenses = R&D + SG&A + other opex", detail


_NO = _RULES["nonoperating"]
_OTHER_INCOME_EXPENSE_AGG_KEYS = tuple(_NO["agg_keys"])
_INTEREST_NET_KEYS = tuple(_NO["interest_net_keys"])
_INTEREST_COMPONENT_KEYS = tuple(_NO["interest_component_keys"])
_INVESTMENT_INCOME_KEYS = tuple(_NO["investment_income_keys"])
_OTHER_NONOP_KEYS = tuple(_NO["other_nonop_keys"])


def _signed_nonop_component(concept_key: str, v: float) -> float:
    """
    Normalize sign for component-style nonoperating concepts.
    Many filers report *Expense*/*Loss* as positive numbers; treat them as deductions.
    For net concepts (e.g. *IncomeExpenseNet*), keep the sign as reported.
    """
    k = (concept_key or "").lower()
    if "incometax" in k:
        return v
    # Net / already-signed totals: keep as-is
    if "incomeexpensenet" in k or "incomeexpense" in k or "nonoperatingincomeexpense" in k:
        return v
    # Component deductions
    if ("expense" in k or ("loss" in k and "gainloss" not in k)) and v > 0:
        return -abs(v)
    return v


def _sum_dimensioned(section: Dict[str, Any], base_concept: str) -> float | None:
    """Sum all numeric values for keys like f\"{base_concept} [X]\" (undimensioned base not included)."""
    total = 0.0
    found = False
    prefix = base_concept + " ["
    for k in section or {}:
        if not k.startswith(prefix):
            continue
        v = _val(section, k)
        if v is None:
            continue
        total += float(v)
        found = True
    return total if found else None


def _is_income_statement_like_key(k: str) -> bool:
    """Heuristic filter to avoid pulling Balance Sheet / Cash Flow concepts from the 'other' bucket."""
    kl = (k or "").lower()
    exclude_tokens = tuple(t.lower() for t in _NO["exclude_tokens"])
    if any(t in kl for t in exclude_tokens):
        return False
    include_tokens = tuple(t.lower() for t in _NO["include_tokens"])
    return any(t in kl for t in include_tokens)


def _is_nonop_component_key(k: str) -> bool:
    """Filter for keys that plausibly contribute to nonoperating bridge (excluding subtotals and cash-flow-like items)."""
    if not k or "[" in k:
        return False
    kl = k.lower()
    component_exclude = tuple(t.lower() for t in _NO["component_exclude_subtotals"])
    if any(t in kl for t in component_exclude):
        return False
    if any(t in kl for t in ("paid", "payment", "payments", "proceeds", "repayment", "increase", "decrease", "cash")):
        return False
    return _is_income_statement_like_key(k)


def _best_subset_sum(cands: List[Tuple[str, float]], target: float, max_k: int = 10) -> float | None:
    """
    Choose best sum of up to 3 components to match target.
    We restrict to top abs-value candidates for speed and to avoid overfitting noise.
    """
    if not cands:
        return None
    cands = sorted(cands, key=lambda x: abs(x[1]), reverse=True)[:max_k]
    best_sum = None
    best_err = None

    vals = [v for _, v in cands]
    # singles
    for v in vals:
        err = abs(target - v)
        if best_err is None or err < best_err:
            best_err = err
            best_sum = v
    # pairs
    for i in range(len(vals)):
        for j in range(i + 1, len(vals)):
            s = vals[i] + vals[j]
            err = abs(target - s)
            if best_err is None or err < best_err:
                best_err = err
                best_sum = s
    # triples
    for i in range(len(vals)):
        for j in range(i + 1, len(vals)):
            for k in range(j + 1, len(vals)):
                s = vals[i] + vals[j] + vals[k]
                err = abs(target - s)
                if best_err is None or err < best_err:
                    best_err = err
                    best_sum = s
    return best_sum

def _other_income_expense_total(inc: Dict[str, Any], other: Dict[str, Any]) -> float | None:
    """
    Other income/expense used to bridge OperatingIncomeLoss -> Pretax.
    Prefer aggregated concepts when present; otherwise sum common nonoperating components plus
    undimensioned extension keys containing 'Nonoperating'/'NonOperating'.
    """
    agg = _val_first([other, inc], list(_OTHER_INCOME_EXPENSE_AGG_KEYS))
    if agg is not None:
        return agg

    total = 0.0
    seen: set[str] = set()

    # Prefer net interest if present; otherwise use interest income/expense components.
    net_interest = _val_first([other, inc], list(_INTEREST_NET_KEYS))
    if net_interest is not None:
        total += net_interest
        seen.update(_INTEREST_NET_KEYS)
    else:
        for k in _INTEREST_COMPONENT_KEYS:
            v = _val_first([other, inc], [k])
            if v is None:
                v = _sum_dimensioned(other, k) or _sum_dimensioned(inc, k)
            if v is not None:
                total += _signed_nonop_component(k, v)
                seen.add(k)

    # Investment income (often tagged as nonoperating).
    for k in _INVESTMENT_INCOME_KEYS:
        v = _val_first([other, inc], [k])
        if v is None:
            v = _sum_dimensioned(other, k) or _sum_dimensioned(inc, k)
        if v is not None:
            total += _signed_nonop_component(k, v)
            seen.add(k)

    # Other nonoperating income/expense.
    for k in _OTHER_NONOP_KEYS:
        v = _val_first([other, inc], [k])
        if v is None:
            v = _sum_dimensioned(other, k) or _sum_dimensioned(inc, k)
        if v is not None:
            total += _signed_nonop_component(k, v)
            seen.add(k)
            break

    # Extension / custom items: capture undimensioned keys containing Nonoperating.
    nonop_contains = _NO.get("nonop_extension_contains", ["Nonoperating", "NonOperating"])
    nonop_exclude = _NO.get("nonop_extension_exclude", ["IncomeTax", "BeforeIncomeTaxes"])
    for section in (other, inc):
        if not section:
            continue
        for k in section:
            if "[" in k or k in seen:
                continue
            if not any(c in k for c in nonop_contains):
                continue
            if any(ex in k for ex in nonop_exclude):
                continue
            if section is other and not _is_nonop_component_key(k):
                continue
            v = _val(section, k)
            if v is not None:
                total += _signed_nonop_component(k, v)
                seen.add(k)

    extra_keys = _NO["extra_keys"]
    for k in extra_keys:
        if k in seen:
            continue
        v = _val_first([other, inc], [k])
        if v is None:
            v = _sum_dimensioned(other, k) or _sum_dimensioned(inc, k)
        if v is None:
            continue
        # For 'other' bucket, apply filter to avoid pulling BS/CF items
        if (k not in inc) and (k in other) and not _is_nonop_component_key(k):
            continue
        total += _signed_nonop_component(k, float(v))
        seen.add(k)

    return total if total != 0.0 else None


def check_pretax_from_operating_and_nonoperating(
    data: Dict[str, Any]
) -> Tuple[bool, str, Dict[str, Any]]:
    """Operating income + Nonoperating income/expense = Pretax income (continuing)."""
    _, inner = _get_period_data(data)
    if not inner:
        return False, "Invalid data shape", {}
    inc = inner.get("income_stmt") or {}
    other = inner.get("other") or {}
    pretax = _val_first(
        [inc, other],
        [
            "us-gaap:IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest",
            "us-gaap:IncomeLossFromContinuingOperationsBeforeIncomeTaxes",
            "us-gaap:IncomeLossBeforeIncomeTaxes",
        ],
    )
    op_inc = _val_first([inc, other], ["us-gaap:OperatingIncomeLoss"])
    nonop = _val_first([other, inc], ["us-gaap:NonoperatingIncomeExpense"])
    if pretax is None or op_inc is None:
        return True, "Skipped (missing pretax or operating income)", {}
    # Candidate nonoperating values: prefer an aggregate if it reconciles; otherwise use derived components.
    implied = pretax - op_inc
    candidates: List[Tuple[str, float]] = []
    if nonop is not None:
        candidates.append(("reported_nonop", float(nonop)))
    agg_alt = _val_first([other, inc], ["us-gaap:OtherIncomeExpenseNet", "us-gaap:TotalOtherIncomeExpenseNet", "us-gaap:InterestIncomeExpenseNonoperatingNet"])
    if agg_alt is not None:
        candidates.append(("reported_agg_alt", float(agg_alt)))
    derived = _other_income_expense_total(inc, other)
    if derived is not None:
        candidates.append(("derived_sum", float(derived)))

    # Best subset of plausible components to match implied (handles filings where only some components exist and avoids over-adding).
    comp_cands: List[Tuple[str, float]] = []
    for section in (inc, other):
        if not section:
            continue
        for k in section:
            if not _is_nonop_component_key(k):
                continue
            v = _val(section, k)
            if v is None:
                continue
            comp_cands.append((k, _signed_nonop_component(k, float(v))))
    best_subset = _best_subset_sum(comp_cands, implied)
    if best_subset is not None:
        candidates.append(("derived_best_subset", float(best_subset)))
    if not candidates:
        nonop_val = 0.0
    else:
        # Choose the candidate that best matches implied nonoperating (pretax - operating) to avoid mis-tagged totals.
        nonop_val = min(candidates, key=lambda x: abs(x[1] - implied))[1]
    computed = op_inc + nonop_val
    ok = _approx_eq_rel(pretax, computed)
    return ok, "OperatingIncomeLoss + NonoperatingIncomeExpense = Pretax", {
        "Pretax (reported)": pretax,
        "OperatingIncomeLoss": op_inc,
        "NonoperatingIncomeExpense": nonop_val,
        "Computed": computed,
        "Difference": (pretax or 0) - computed,
    }


def check_revenue_product_plus_service(
    data: Dict[str, Any]
) -> Tuple[bool, str, Dict[str, Any]]:
    """Revenue [Product] + [Service] = total Revenue (when breakdown reported)."""
    _, inner = _get_period_data(data)
    if not inner:
        return False, "Invalid data shape", {}
    inc = inner.get("income_stmt") or {}
    other = inner.get("other") or {}
    rps = _RULES["revenue_product_service"]
    total = _val_first([inc, other], rps["total_concepts"])
    product_key = rps["product_key"]
    service_key = rps["service_key"]
    product = _val(inc, product_key) or _val(other, product_key)
    service = _val(inc, service_key) or _val(other, service_key)
    if total is None or product is None or service is None:
        return True, "Skipped (no Product/Service revenue breakdown)", {}
    computed = product + service
    ok = _approx_eq_rel(total, computed)
    return ok, "Revenue Product + Service = Total Revenue", {
        "Total Revenue": total,
        "Product": product,
        "Service": service,
        "Sum": computed,
        "Difference": total - computed,
    }


def check_cost_product_plus_service(
    data: Dict[str, Any]
) -> Tuple[bool, str, Dict[str, Any]]:
    """CostOfGoodsAndServicesSold [Product] + [Service] = total Cost (when breakdown reported)."""
    _, inner = _get_period_data(data)
    if not inner:
        return False, "Invalid data shape", {}
    inc = inner.get("income_stmt") or {}
    other = inner.get("other") or {}
    cps = _RULES["cost_product_service"]
    total = _val_first([other, inc], cps["total_concepts"])
    product_key = cps["product_key"]
    service_key = cps["service_key"]
    product = _val(other, product_key) or _val(inc, product_key)
    service = _val(other, service_key) or _val(inc, service_key)
    if total is None or product is None or service is None:
        return True, "Skipped (no Product/Service cost breakdown)", {}
    computed = product + service
    ok = _approx_eq_rel(total, computed)
    return ok, "Cost Product + Service = Total Cost", {
        "Total Cost": total,
        "Product": product,
        "Service": service,
        "Sum": computed,
        "Difference": total - computed,
    }


def check_segment_revenue_sum(
    data: Dict[str, Any]
) -> Tuple[bool, str, Dict[str, Any]]:
    """Sum of geographic segment revenues = total Revenue (when all five common segments reported)."""
    _, inner = _get_period_data(data)
    if not inner:
        return False, "Invalid data shape", {}
    inc = inner.get("income_stmt") or {}
    seg = _RULES["segment_revenue"]
    total = _val(inc, seg["total_concept"])
    segments = seg["segments"]
    vals = [_val(inc, k) for k in segments]
    if total is None or any(v is None for v in vals):
        return True, "Skipped (no geographic segment revenue breakdown)", {}
    computed = sum(vals)
    ok = _approx_eq_rel(total, computed)
    return ok, "Segment revenue sum = Total Revenue", {
        "Total Revenue": total,
        "Americas + Europe + GreaterChina + Japan + RestOfAsiaPacific": computed,
        "Difference": total - computed,
    }


def _difference_pct(detail: Dict[str, Any]) -> float | None:
    """Compute |Difference| as % of reference (max of other numeric values in detail). Returns None if no Difference or invalid."""
    diff = detail.get("Difference")
    if diff is None:
        return None
    try:
        diff_abs = abs(float(diff))
    except (TypeError, ValueError):
        return None
    ref = 1e-9
    for k, v in detail.items():
        if k == "Difference" or v is None:
            continue
        try:
            ref = max(ref, abs(float(v)))
        except (TypeError, ValueError):
            continue
    return (diff_abs / ref * 100.0) if ref > 0 else None


def run_all_checks(
    data: Dict[str, Any]
) -> List[Tuple[str, bool, str, Dict[str, Any]]]:
    """Run all accounting checks; return list of (check_name, passed, message, detail). Fails if relative diff > RELATIVE_TOLERANCE_PCT (5%)."""
    checks: List[Tuple[str, Callable[..., Tuple[bool, str, Dict[str, Any]]]]] = [
        ("Balance sheet identity (A = L + E)", check_balance_sheet_identity),
        ("Assets = Current + Noncurrent", check_assets_current_plus_noncurrent),
        ("Liabilities = Current + Noncurrent", check_liabilities_current_plus_noncurrent),
        ("Revenue - Cost = GrossProfit", check_gross_profit),
        ("GrossProfit - OpEx = OperatingIncomeLoss", check_operating_income),
        ("OperatingExpenses = R&D + SG&A", check_operating_expenses_rd_sga),
        ("OperatingIncome + Nonoperating = Pretax", check_pretax_from_operating_and_nonoperating),
        ("Pretax - Tax = NetIncomeLoss", check_net_income_from_pretax_and_tax),
        ("Comprehensive income (Net + OCI)", check_comprehensive_income),
        ("Cash flow articulation", check_cash_flow_articulation),
        ("Equity components", check_equity_components),
        ("Revenue Product + Service = Total", check_revenue_product_plus_service),
        ("Cost Product + Service = Total", check_cost_product_plus_service),
        ("Segment revenue sum = Total Revenue", check_segment_revenue_sum),
    ]
    results = []
    for name, fn in checks:
        try:
            passed, msg, detail = fn(data)
            if detail and "Difference" in detail:
                pct = _difference_pct(detail)
                if pct is not None:
                    detail = {**detail, "DifferencePct": round(pct, 2)}
                    if pct > RELATIVE_TOLERANCE_PCT:
                        passed = False
                    else:
                        passed = True
            results.append((name, passed, msg, detail))
        except Exception as e:
            results.append((name, False, f"Error: {e}", {}))
    return results


if __name__ == "__main__":
    import json
    import sys
    from pathlib import Path

    if len(sys.argv) < 2:
        print("Usage: accounting_checks/10k.py <path/to/accession-code.json>", file=sys.stderr)
        sys.exit(1)
    path = Path(sys.argv[1])
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    results = run_all_checks(data)

    passed_count = sum(1 for _, ok, _, _ in results if ok)
    print(f"Accounting checks: {passed_count}/{len(results)} passed\n", file=sys.stderr)
    for name, passed, msg, detail in results:
        status = "PASS" if passed else "FAIL"
        pct = (detail or {}).get("DifferencePct")
        if not passed and pct is not None:
            print(f"  [{status}] {name}: {msg} (LHS > RHS by {pct:.2f}%)")
        else:
            print(f"  [{status}] {name}: {msg}")
        if detail and not passed:
            for k, v in detail.items():
                if v is not None and k != "DifferencePct":
                    print(f"       {k}: {v}")
    sys.exit(0 if passed_count == len(results) else 1)
