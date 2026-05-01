"""
Accounting consistency checks on legacy (PEM/IMS text) scraped JSON for 10-K-like forms.

Expects structure: { accession, form_type, period, company, balance_sheet, income_statement, cash_flow_statement }.
Rules are loaded from src/fundamentals/edgar/rules-legacy/10k_legacy.yaml.
"""

from pathlib import Path
from typing import Any, Callable, Dict, List, Tuple

import yaml


def _load_rules() -> Dict[str, Any]:
    """Load 10-K legacy rules from src/fundamentals/edgar/rules-legacy/10k_legacy.yaml."""
    mod_dir = Path(__file__).resolve().parent.parent  # edgar/
    rules_path = mod_dir / "rules-legacy" / "10k_legacy.yaml"
    with open(rules_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


_RULES = _load_rules()
RELATIVE_TOLERANCE_PCT = float(_RULES["relative_tolerance_pct"])


def _to_float(v: Any) -> float | None:
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _val_legacy(section: Dict[str, Any], candidate_keys: List[str]) -> float | None:
    """
    Get first numeric value from section for exact key or key that starts with a candidate
    (e.g. "Total Net Sales And Revenues" matches "Total Net Sales And Revenues [Net Sales And Revenues From]").
    """
    if not section or not candidate_keys:
        return None
    for c in candidate_keys:
        v = section.get(c)
        if _to_float(v) is not None:
            return _to_float(v)
    for k, v in section.items():
        if _to_float(v) is None:
            continue
        for c in candidate_keys:
            if k == c or k.startswith(c + " ") or k.startswith(c + " ["):
                return _to_float(v)
    return None


def _approx_eq_rel(a: float, b: float, rel_pct: float = RELATIVE_TOLERANCE_PCT) -> bool:
    ref = max(abs(a), abs(b), 1e-9)
    return abs(a - b) / ref * 100.0 <= rel_pct


def check_balance_sheet_identity(data: Dict[str, Any]) -> Tuple[bool, str, Dict[str, Any]]:
    """Total Assets = Total Liabilities And Shareholders' Equity."""
    bs = data.get("balance_sheet") or {}
    cfg = _RULES["balance_sheet"]
    assets = _val_legacy(bs, cfg["assets_keys"])
    l_and_e = _val_legacy(bs, cfg["liabilities_and_equity_keys"])
    if assets is None:
        return False, "Missing Total Assets", {}
    if l_and_e is None:
        return False, "Missing Total Liabilities And Shareholders' Equity", {"Assets": assets}
    ok = _approx_eq_rel(assets, l_and_e)
    return ok, "Total Assets = Total Liabilities And Shareholders' Equity", {
        "Assets": assets,
        "LiabilitiesAndEquity": l_and_e,
        "Difference": assets - l_and_e,
    }


def check_gross_profit(data: Dict[str, Any]) -> Tuple[bool, str, Dict[str, Any]]:
    """Revenue - Cost = Gross profit (when reported)."""
    inc = data.get("income_statement") or {}
    cfg = _RULES["income_statement"]
    revenue = _val_legacy(inc, cfg["revenue_keys"])
    cost = _val_legacy(inc, cfg["cost_keys"])
    gross = _val_legacy(inc, cfg["gross_profit_keys"])
    if gross is None:
        return True, "Skipped (missing Gross Profit)", {}
    if revenue is None or cost is None:
        return True, "Skipped (missing Revenue or Cost)", {}
    computed = revenue - cost
    ok = _approx_eq_rel(gross, computed)
    return ok, "Revenue - Cost = Gross Profit", {
        "Revenue": revenue,
        "Cost": cost,
        "GrossProfit (reported)": gross,
        "GrossProfit (computed)": computed,
        "Difference": gross - computed,
    }


def check_operating_income(data: Dict[str, Any]) -> Tuple[bool, str, Dict[str, Any]]:
    """Revenue - Total Costs And Expenses = Operating margin (when reported)."""
    inc = data.get("income_statement") or {}
    cfg = _RULES["income_statement"]
    revenue = _val_legacy(inc, cfg["revenue_keys"])
    cost = _val_legacy(inc, cfg["cost_keys"])
    operating = _val_legacy(inc, cfg["operating_income_keys"])
    if operating is None or revenue is None or cost is None:
        return True, "Skipped (missing Revenue, Cost, or Operating Margin)", {}
    computed = revenue - cost
    ok = _approx_eq_rel(operating, computed)
    return ok, "Revenue - Total Costs And Expenses = Operating Margin", {
        "Revenue": revenue,
        "TotalCostsAndExpenses": cost,
        "OperatingMargin (reported)": operating,
        "OperatingMargin (computed)": computed,
        "Difference": operating - computed,
    }


def check_net_income_from_pretax_and_tax(data: Dict[str, Any]) -> Tuple[bool, str, Dict[str, Any]]:
    """Pretax - Tax = Net income (when reported)."""
    inc = data.get("income_statement") or {}
    cfg = _RULES["income_statement"]
    pretax = _val_legacy(inc, cfg["pretax_keys"])
    tax = _val_legacy(inc, cfg["tax_keys"])
    net = _val_legacy(inc, cfg["net_income_keys"])
    if pretax is None or net is None:
        return True, "Skipped (missing pretax or net income)", {}
    tax_val = tax if tax is not None else 0.0
    computed = pretax - tax_val
    ok = _approx_eq_rel(net, computed)
    return ok, "Pretax - Tax = Net Income", {
        "Pretax": pretax,
        "Tax": tax_val,
        "NetIncome (reported)": net,
        "NetIncome (computed)": computed,
        "Difference": net - computed,
    }


def check_cash_flow_articulation(data: Dict[str, Any]) -> Tuple[bool, str, Dict[str, Any]]:
    """Operating + Investing + Financing = Change in cash (when reported)."""
    cf = data.get("cash_flow_statement") or {}
    cfg = _RULES["cash_flow"]
    operating = _val_legacy(cf, cfg["operating_keys"])
    investing = _val_legacy(cf, cfg["investing_keys"])
    financing = _val_legacy(cf, cfg["financing_keys"])
    change = _val_legacy(cf, cfg["change_in_cash_keys"])
    if operating is None or investing is None or financing is None:
        return True, "Skipped (missing Operating/Investing/Financing cash flow)", {}
    sum_cf = operating + investing + financing
    if change is None:
        return True, "Skipped (missing change in cash)", {
            "Operating": operating,
            "Investing": investing,
            "Financing": financing,
            "Sum": sum_cf,
        }
    ok = _approx_eq_rel(sum_cf, change)
    return ok, "Operating + Investing + Financing = Change in cash", {
        "Operating": operating,
        "Investing": investing,
        "Financing": financing,
        "Sum": sum_cf,
        "ChangeInCash (reported)": change,
        "Difference": sum_cf - change,
    }


def _difference_pct(detail: Dict[str, Any]) -> float | None:
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
    data: Dict[str, Any],
) -> List[Tuple[str, bool, str, Dict[str, Any]]]:
    """Run legacy 10-K accounting checks; return list of (check_name, passed, message, detail)."""
    checks: List[Tuple[str, Callable[..., Tuple[bool, str, Dict[str, Any]]]]] = [
        ("Balance sheet identity (A = L + E)", check_balance_sheet_identity),
        ("Revenue - Cost = Gross Profit", check_gross_profit),
        ("Operating income/margin", check_operating_income),
        ("Pretax - Tax = Net Income", check_net_income_from_pretax_and_tax),
        ("Cash flow articulation", check_cash_flow_articulation),
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

    if len(sys.argv) < 2:
        print("Usage: accounting_checks/10k_legacy.py <path/to/legacy.json>", file=sys.stderr)
        sys.exit(1)
    path = Path(sys.argv[1])
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    results = run_all_checks(data)
    passed_count = sum(1 for _, ok, _, _ in results if ok)
    print(f"Legacy 10-K accounting checks: {passed_count}/{len(results)} passed\n", file=sys.stderr)
    for name, passed, msg, detail in results:
        status = "PASS" if passed else "FAIL"
        pct = (detail or {}).get("DifferencePct")
        if not passed and pct is not None:
            print(f"  [{status}] {name}: {msg} (diff {pct:.2f}%)")
        else:
            print(f"  [{status}] {name}: {msg}")
        if detail and not passed:
            for k, v in detail.items():
                if v is not None and k != "DifferencePct":
                    print(f"       {k}: {v}")
    sys.exit(0 if passed_count == len(results) else 1)
