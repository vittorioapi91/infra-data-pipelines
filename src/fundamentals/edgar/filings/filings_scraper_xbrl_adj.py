"""
Convert XBRL JSON to adjusted structure (single period, concept -> value). Writes accession-code.json.

Accepts raw xbrl.json (contextRef + top-level contexts): runs inline-context + flatten in memory, then pivot.

Output: { date: { balance_sheet: { concept: value }, income_stmt, cashflow, other } }.
Top-level keys: ticker, exchange; inside each date key: balance_sheet, income_stmt, cashflow, other, shares (aggregate).
"""

import contextlib
import copy
import io
import json
import logging
import re
from calendar import monthrange
from collections import Counter
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

# xbrl fact array keys (raw and adj)
_FACT_ARRAY_KEYS = ("balance_sheet", "income_statement", "cash_flow", "other")
_STATEMENT_KEY_MAP = {
    "balance_sheet": "balance_sheet",
    "income_statement": "income_stmt",
    "cash_flow": "cashflow",
    "other": "other",
}


def _xbrl_inline_contexts(data: Dict[str, Any]) -> Dict[str, Any]:
    """Replace every contextRef with full context object from top-level contexts; remove contexts key."""
    data = copy.deepcopy(data)
    contexts = data.pop("contexts", None)
    if not contexts:
        return data
    for key in _FACT_ARRAY_KEYS:
        if key in data and isinstance(data[key], list):
            for item in data[key]:
                ref = item.pop("contextRef", None)
                if ref is None:
                    continue
                if isinstance(ref, str) and ref in contexts:
                    item["context"] = copy.deepcopy(contexts[ref])
                else:
                    item["contextRef"] = ref
    return data


def _xbrl_flatten_context(data: Dict[str, Any]) -> Dict[str, Any]:
    """Bring context's keys (entity, period, dimensions) to fact level; entity -> entity_scheme/identifier, period -> period_*; drop scale/format."""
    _ALLOWED_CONTEXT_KEYS = ("entity", "period", "dimensions")
    _ALLOWED_ENTITY_KEYS = ("scheme", "identifier")
    _ALLOWED_PERIOD_KEYS = ("instant", "startDate", "endDate")

    def suffix_after_slash(s: str) -> str:
        return s.rsplit("/", 1)[-1] if "/" in s else s

    data = copy.deepcopy(data)
    for key in _FACT_ARRAY_KEYS:
        if key not in data or not isinstance(data[key], list):
            continue
        for item in data[key]:
            item.pop("scale", None)
            item.pop("format", None)
            ctx = item.pop("context", None)
            if ctx is None:
                continue
            for k in ctx:
                if k in _ALLOWED_CONTEXT_KEYS:
                    item[k] = ctx[k]
            if "entity" in item:
                entity = item.pop("entity")
                if isinstance(entity, dict):
                    for k, v in entity.items():
                        if k in _ALLOWED_ENTITY_KEYS:
                            if k == "scheme" and isinstance(v, str):
                                v = suffix_after_slash(v)
                            item[f"entity_{k}"] = v
            if "period" in item:
                period = item.pop("period")
                if isinstance(period, dict):
                    for k, v in period.items():
                        if k in _ALLOWED_PERIOD_KEYS:
                            item[f"period_{k}"] = v
    return data


def _ensure_adj_shape(data: Dict[str, Any]) -> Dict[str, Any]:
    """If data is raw xbrl.json (contextRef + contexts), run inline + flatten to produce adj-shaped data in memory."""
    if "contexts" not in data:
        return data
    first_arr = data.get("balance_sheet") or data.get("income_statement") or data.get("cash_flow") or data.get("other")
    if not first_arr or not isinstance(first_arr, list) or not first_arr:
        return data
    if "contextRef" not in first_arr[0]:
        return data
    data = _xbrl_inline_contexts(data)
    data = _xbrl_flatten_context(data)
    return data


def _end_of_month(date_str: str) -> str:
    """Normalize date to last day of month (e.g. 2024-09-29 -> 2024-09-30). Keeps time suffix if present."""
    if "T" in date_str:
        date_part, time_part = date_str.split("T", 1)
        suffix = "T" + time_part
    else:
        date_part = date_str
        suffix = "T00:00:00"
    y, m, d = map(int, date_part.split("-"))
    last = monthrange(y, m)[1]
    return f"{y:04d}-{m:02d}-{last:02d}{suffix}"


def _period_key(fact: Dict[str, Any]) -> str | None:
    """Single period key for a fact: period_instant or period_endDate, normalized to end-of-month YYYY-MM-DDTHH:MM:SS."""
    instant = fact.get("period_instant")
    if instant and isinstance(instant, str):
        raw = instant if "T" in instant else f"{instant}T00:00:00"
        return _end_of_month(raw)
    end = fact.get("period_endDate")
    if end and isinstance(end, str):
        raw = end if "T" in end else f"{end}T00:00:00"
        return _end_of_month(raw)
    return None


def _row_key(fact: Dict[str, Any]) -> Tuple[str, str]:
    """(concept, dimensions_key) for grouping. dimensions_key is stable string for dimensions dict."""
    concept = fact.get("concept") or ""
    dims = fact.get("dimensions")
    if not dims or not isinstance(dims, dict):
        return (concept, "")
    parts = sorted(f"{a}:{v}" for a, v in dims.items())
    return (concept, "|".join(parts))


def _strip_trailing_member(s: str) -> str:
    """Remove trailing 'Member' from dimension member names."""
    if s.endswith("Member"):
        return s[: -len("Member")]
    return s


def _index_label(concept: str, dimensions_key: str) -> str:
    """_index value: concept; if dimensions, append a short suffix from dimensions_key (trailing 'Member' stripped)."""
    if not dimensions_key:
        return concept
    parts = dimensions_key.split("|")
    suffixes = []
    for p in parts:
        if ":" in p:
            _, rest = p.split(":", 1)
            if ":" in rest:
                _, member = rest.rsplit(":", 1)
                suffixes.append(_strip_trailing_member(member))
            else:
                suffixes.append(_strip_trailing_member(rest))
        else:
            suffixes.append(_strip_trailing_member(p))
    return f"{concept} [{', '.join(suffixes)}]" if suffixes else concept


# Quarter end month-day for DocumentFiscalPeriodFocus -> end of quarter (calendar)
_QUARTER_END_MAP = {"Q1": "03-31", "Q2": "06-30", "Q3": "09-30", "Q4": "12-31", "FY": "12-31"}


def _get_document_period_end(data: Dict[str, Any]) -> Optional[str]:
    """
    Document period end from DEI: dei:DocumentPeriodEndDate, or from
    dei:DocumentFiscalYearFocus + dei:DocumentFiscalPeriodFocus (end of that quarter).
    Returns YYYY-MM-DD or None. Safe to call on raw xbrl.json (other array has concept/value).
    """
    doc_end: Optional[str] = None
    year_focus: Optional[int] = None
    period_focus: Optional[str] = None
    for fact in data.get("other") or []:
        if not isinstance(fact, dict):
            continue
        concept = (fact.get("concept") or "").strip()
        val = fact.get("value")
        if concept == "dei:DocumentPeriodEndDate" and val is not None:
            s = str(val).strip()
            if len(s) >= 10 and s[:10].replace("-", "").isdigit():
                doc_end = s[:10]
                return doc_end
        if concept == "dei:DocumentFiscalYearFocus" and val is not None:
            try:
                year_focus = int(val)
            except (TypeError, ValueError):
                pass
        if concept == "dei:DocumentFiscalPeriodFocus" and val is not None:
            period_focus = str(val).strip().upper()
    if year_focus is not None and period_focus:
        end_md = _QUARTER_END_MAP.get(period_focus)
        if end_md:
            return f"{year_focus:04d}-{end_md}"
    return None


def _filing_period_end(data: Dict[str, Any], document_period_end: Optional[str] = None) -> str | None:
    """
    Primary period from balance sheet: if document_period_end (YYYY-MM-DD) is set, pick the
    period closest to that date; otherwise use the period with the most facts (modal).
    """
    periods: List[str] = []
    arr = data.get("balance_sheet")
    if isinstance(arr, list):
        for fact in arr:
            p = _period_key(fact)
            if p:
                periods.append(p)
    if not periods:
        for key in ("income_statement", "cash_flow"):
            arr = data.get(key)
            if isinstance(arr, list):
                for fact in arr:
                    p = _period_key(fact)
                    if p:
                        periods.append(p)
    if not periods:
        return None
    if document_period_end:
        try:
            dt_target = datetime.strptime(document_period_end[:10], "%Y-%m-%d")
        except (ValueError, TypeError):
            dt_target = None
        if dt_target is not None:
            def _period_date(p: str) -> Optional[datetime]:
                try:
                    return datetime.strptime(p[:10], "%Y-%m-%d")
                except (ValueError, TypeError):
                    return None
            dated = [(p, _period_date(p)) for p in periods]
            dated = [(p, d) for p, d in dated if d is not None]
            if dated:
                on_or_after = [(p, d) for p, d in dated if d >= dt_target]
                if on_or_after:
                    return min(on_or_after, key=lambda x: (x[1] - dt_target).days)[0]
                return max(dated, key=lambda x: x[1])[0]
    # Fallback: modal period
    counts = Counter(periods)
    return counts.most_common(1)[0][0]


def _filing_period_candidates(
    data: Dict[str, Any], document_period_end: Optional[str] = None
) -> List[str]:
    """
    All unique periods from balance_sheet (then income_statement, cash_flow), ordered by preference:
    on-or-after document_period_end (closest first), then before (most recent first).
    """
    periods: List[str] = []
    seen: set = set()
    for key in ("balance_sheet", "income_statement", "cash_flow"):
        arr = data.get(key)
        if not isinstance(arr, list):
            continue
        for fact in arr:
            p = _period_key(fact)
            if p and p not in seen:
                seen.add(p)
                periods.append(p)
    if not periods:
        return []
    if not document_period_end:
        counts = Counter(periods)
        return [p for p, _ in counts.most_common()]
    try:
        dt_target = datetime.strptime(document_period_end[:10], "%Y-%m-%d")
    except (ValueError, TypeError):
        return list(dict.fromkeys(periods))
    def _period_date(p: str) -> Optional[datetime]:
        try:
            return datetime.strptime(p[:10], "%Y-%m-%d")
        except (ValueError, TypeError):
            return None
    dated = [(p, _period_date(p)) for p in periods]
    dated = [(p, d) for p, d in dated if d is not None]
    if not dated:
        return list(dict.fromkeys(periods))
    on_or_after = sorted(
        [(p, d) for p, d in dated if d >= dt_target],
        key=lambda x: (x[1] - dt_target).days,
    )
    before = sorted(
        [(p, d) for p, d in dated if d < dt_target],
        key=lambda x: (dt_target - x[1]).days,
    )
    return [p for p, _ in on_or_after] + [p for p, _ in before]


def is_essentially_empty_adj(adj_result: Dict[str, Any]) -> bool:
    """True if the full adj output (with ticker/exchange/period key) has no usable balance sheet for its single period."""
    period_keys = [k for k in adj_result if k not in ("ticker", "exchange")]
    if not period_keys:
        return True
    inner = adj_result.get(period_keys[0])
    if not isinstance(inner, dict):
        return True
    return _is_adj_result_essentially_empty(inner)


def _is_adj_result_essentially_empty(out: Dict[str, Any]) -> bool:
    """
    True if the single-period output (balance_sheet, income_stmt, ...) has no usable balance sheet:
    missing us-gaap:Assets or us-gaap:StockholdersEquity (or non-numeric), or fewer than 3 numeric values.
    """
    bs = out.get("balance_sheet")
    if not bs or not isinstance(bs, dict):
        return True
    assets = bs.get("us-gaap:Assets")
    equity = bs.get("us-gaap:StockholdersEquity")
    try:
        has_assets = assets is not None and float(assets) == float(assets)
    except (TypeError, ValueError):
        has_assets = False
    try:
        has_equity = equity is not None and float(equity) == float(equity)
    except (TypeError, ValueError):
        has_equity = False
    if not has_assets and not has_equity:
        return True
    numeric_count = 0
    for v in bs.values():
        try:
            if v is not None and float(v) == float(v):
                numeric_count += 1
        except (TypeError, ValueError):
            pass
    return numeric_count < 3


def _is_currency_unit(unit_ref: str | None, units: Dict[str, Any]) -> bool:
    """True if unit_ref is a pure-currency unit (iso4217:* in numerator, empty denominator)."""
    if not unit_ref or not isinstance(unit_ref, str):
        return False
    if not units:
        return unit_ref.lower() == "usd"
    unit_def = units.get(unit_ref)
    if not unit_def or not isinstance(unit_def, dict):
        return False
    measures = unit_def.get("measures")
    if not measures or not isinstance(measures, dict):
        return False
    numer = measures.get("numerator")
    denom = measures.get("denominator")
    if not isinstance(numer, list) or len(numer) != 1:
        return False
    if denom and len(denom) > 0:
        return False
    measure = numer[0]
    if not isinstance(measure, str):
        return False
    return "iso4217:" in measure.lower()


def _pivot_fact_list(
    facts: List[Dict[str, Any]],
    units: Dict[str, Any] | None = None,
    filing_period: str | None = None,
) -> List[Dict[str, Any]] | Dict[str, Any]:
    """Group facts by (concept, dimensions); only currency facts; only filing_period when set."""
    units = units or {}
    groups: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for fact in facts:
        if not _is_currency_unit(fact.get("unitRef"), units):
            continue
        concept = fact.get("concept")
        if concept is None:
            continue
        period = _period_key(fact)
        if period is None:
            continue
        if filing_period is not None and period != filing_period:
            continue
        value = fact.get("value")
        if _is_error_or_non_numeric_value(value):
            continue
        rk = _row_key(fact)
        if rk not in groups:
            groups[rk] = {"_index": _index_label(rk[0], rk[1])}
        groups[rk][period] = value

    rows = list(groups.values())
    if not rows:
        return rows
    if filing_period is not None:
        return {r["_index"]: r.get(filing_period) for r in rows}
    all_dates = set()
    for r in rows:
        all_dates.update(k for k in r if k != "_index")
    sorted_dates = sorted(all_dates, reverse=True)
    for r in rows:
        for d in sorted_dates:
            if d not in r:
                r[d] = None
    out = []
    for r in rows:
        out.append({"_index": r["_index"], **{d: r[d] for d in sorted_dates}})
    return out


# DEI values that indicate an iXBRL transform error (invalid; do not use as ticker/exchange)
_DEI_ERROR_VALUES = frozenset({"(ixTransformValueError)", "(ix:transformValueError)"})


def _is_error_or_non_numeric_value(val: Any) -> bool:
    """True if value should not be emitted in adj (error string or non-numeric)."""
    if val is None:
        return True
    s = str(val).strip()
    if not s:
        return True
    if s in _DEI_ERROR_VALUES or (s.startswith("(ix") and "Error" in s and ")" in s):
        return True
    try:
        float(val)
    except (TypeError, ValueError):
        return True
    return False


def _is_invalid_dei_value(val: Any) -> bool:
    """True if value looks like an error string and should not be used for ticker/exchange."""
    if val is None:
        return True
    s = str(val).strip()
    if not s:
        return True
    if s in _DEI_ERROR_VALUES:
        return True
    if s.startswith("(ix") and "Error" in s and ")" in s:
        return True
    return False


# Share concepts: instant (outstanding) vs duration (quarter activity)
# Includes MLP/partnership unit concepts (PAA, etc.)
_SHARES_OUTSTANDING_CONCEPTS = (
    "dei:EntityCommonStockSharesOutstanding",
    "us-gaap:CommonStockSharesOutstanding",
    "us-gaap:CommonStockSharesIssued",  # fallback when outstanding not reported
    "us-gaap:LimitedPartnersCapitalAccountUnitsOutstanding",
    "us-gaap:PartnershipUnitsOutstanding",
)
_BUYBACKS_SHARES_CONCEPTS = (
    "us-gaap:StockRepurchasedDuringPeriodShares",
    "us-gaap:TreasuryStockSharesAcquired",
)
_DILUTION_SHARES_CONCEPTS = (
    "us-gaap:StockIssuedDuringPeriodSharesNewIssues",
    "us-gaap:StockIssuedDuringPeriodShares",
    "us-gaap:StockIssuedDuringPeriodSharesShareBasedCompensation",
    "us-gaap:StockIssuedDuringPeriodSharesAcquisitions",
)


def _extract_share_aggregate(
    data: Dict[str, Any],
    filing_period: str,
) -> Dict[str, Any]:
    """
    Extract share count, buybacks, dilution from XBRL facts (shares unit).
    Returns { shares_outstanding, buybacks_shares, dilution_shares } with int or None.
    Prefers quarter duration over YTD when multiple facts match (shortest duration).
    """
    result: Dict[str, Any] = {
        "shares_outstanding": None,
        "buybacks_shares": None,
        "dilution_shares": None,
    }
    if not filing_period:
        return result
    try:
        target_end = datetime.strptime(filing_period[:10], "%Y-%m-%d")
    except (ValueError, TypeError):
        return result

    def _is_share_unit(unit_ref: Any, units: Dict) -> bool:
        if not unit_ref:
            return False
        s = str(unit_ref).lower()
        if s == "shares" or s == "units":
            return True
        if units and unit_ref in units:
            u = units.get(unit_ref)
            if isinstance(u, dict) and "measures" in u:
                return "share" in str(u).lower() or "unit" in str(u).lower()
        return "share" in s or "unit" in s

    units = data.get("units") or {}
    outstanding_candidates: List[Tuple[datetime, float]] = []
    buybacks_candidates: List[Tuple[datetime, float]] = []
    dilution_candidates: List[Tuple[datetime, float]] = []

    for key in _FACT_ARRAY_KEYS:
        arr = data.get(key)
        if not isinstance(arr, list):
            continue
        for fact in arr:
            if not _is_share_unit(fact.get("unitRef"), units):
                continue
            concept = (fact.get("concept") or "").strip()
            value = fact.get("value")
            if _is_error_or_non_numeric_value(value):
                continue
            try:
                v = float(value)
            except (TypeError, ValueError):
                continue
            period = _period_key(fact)
            if not period:
                continue
            try:
                p_end = datetime.strptime(period[:10], "%Y-%m-%d")
            except (ValueError, TypeError):
                continue
            if p_end != target_end:
                continue
            instant = fact.get("period_instant")
            start = fact.get("period_startDate")
            if concept in _SHARES_OUTSTANDING_CONCEPTS:
                if instant or (start is None):
                    prio = _SHARES_OUTSTANDING_CONCEPTS.index(concept)
                    outstanding_candidates.append((prio, p_end, v))
            elif concept in _BUYBACKS_SHARES_CONCEPTS:
                start_dt = None
                if start:
                    try:
                        start_dt = datetime.strptime(start[:10], "%Y-%m-%d")
                    except (ValueError, TypeError):
                        pass
                buybacks_candidates.append((start_dt or datetime.min, v))
            elif concept in _DILUTION_SHARES_CONCEPTS:
                start_dt = None
                if start:
                    try:
                        start_dt = datetime.strptime(start[:10], "%Y-%m-%d")
                    except (ValueError, TypeError):
                        pass
                dilution_candidates.append((start_dt or datetime.min, v))

    if outstanding_candidates:
        best_out = min(outstanding_candidates, key=lambda x: x[0])
        result["shares_outstanding"] = int(round(best_out[2]))
    if buybacks_candidates:
        best = max(buybacks_candidates, key=lambda x: x[0])
        result["buybacks_shares"] = int(round(best[1]))
    if dilution_candidates:
        best_period = max(d[0] for d in dilution_candidates)
        total_dilution = sum(d[1] for d in dilution_candidates if d[0] == best_period)
        result["dilution_shares"] = int(round(total_dilution))

    return result


_MKT_CAP_THRESHOLD = 1e9  # $1B


def _get_market_cap(ticker: str) -> Optional[float]:
    """Return market cap from yfinance Ticker.info, or None if unavailable."""
    if not ticker or not isinstance(ticker, str) or not ticker.strip():
        return None
    t = ticker.strip()
    if t in ("N/A", "dei:NoTradingSymbolFlag"):
        return None
    try:
        import yfinance as yf
        with contextlib.redirect_stderr(io.StringIO()), contextlib.redirect_stdout(io.StringIO()):
            info = yf.Ticker(t).info or {}
        mc = info.get("marketCap")
        if mc is not None:
            try:
                return float(mc)
            except (TypeError, ValueError):
                pass
    except Exception:
        pass
    return None


def _is_etf(ticker: str) -> bool:
    """True if ticker is an ETF (quoteType from yfinance). ETFs excluded from pipeline."""
    try:
        import yfinance as yf
        with contextlib.redirect_stderr(io.StringIO()), contextlib.redirect_stdout(io.StringIO()):
            qt = (yf.Ticker(ticker).info or {}).get("quoteType")
        return str(qt or "").upper() == "ETF"
    except Exception:
        return False


def has_no_trading_symbol_flag(data: Dict[str, Any]) -> bool:
    """
    True if dei:NoTradingSymbolFlag is present and truthy in XBRL "other" facts.
    When true, the filer has no trading symbol; skip yfinance search and full pipeline.
    """
    for fact in data.get("other") or []:
        if not isinstance(fact, dict):
            continue
        if (fact.get("concept") or "").strip() != "dei:NoTradingSymbolFlag":
            continue
        val = fact.get("value")
        if val is True or val in (1, "1"):
            return True
        if isinstance(val, str) and val.lower() == "true":
            return True
    return False


def extract_dei_entity_registrant_name(data: Dict[str, Any]) -> Optional[str]:
    """
    Extract company name from XBRL "other" facts (dei:EntityRegistrantName).
    Returns first value found, or None.
    """
    for fact in data.get("other") or []:
        if not isinstance(fact, dict):
            continue
        if (fact.get("concept") or "").strip() == "dei:EntityRegistrantName":
            val = fact.get("value")
            if val is not None and not _is_invalid_dei_value(val):
                return str(val).strip()
    return None


def _is_valid_ticker_candidate(s: str) -> bool:
    """True if string looks like a ticker symbol (2-5 alphanumeric, no obvious garbage)."""
    if not s or len(s) < 2 or len(s) > 5:
        return False
    if not s.isalnum():
        return False
    if s.upper() in ("NONE", "N/A", "NA"):
        return False
    return True


def extract_dei_from_txt_fallback(txt_path: Path) -> Tuple[Optional[str], Optional[str]]:
    """
    Extract ticker and exchange from raw .txt.
    Primary: dei:TradingSymbol and dei:SecurityExchangeName XBRL tags.
    Fallback: FILENAME (ticker-YYYYMMDD.htm), title, DESCRIPTION (TICKER YYYY...) when XBRL tags missing.
    Returns (ticker, exchange).
    """
    ticker: Optional[str] = None
    exchange: Optional[str] = None
    try:
        content = txt_path.read_text(encoding="utf-8", errors="replace")
        # Primary: XBRL dei:TradingSymbol
        m = re.search(r"<(\w+):TradingSymbol[^>]*>([^<]+)</\1:TradingSymbol>", content)
        if m and m.group(2).strip() and "(ix" not in m.group(2):
            ticker = m.group(2).strip()
        # Primary: XBRL dei:SecurityExchangeName
        m = re.search(r"<(\w+):SecurityExchangeName[^>]*>([^<]+)</\1:SecurityExchangeName>", content)
        if m and m.group(2).strip() and "(ix" not in m.group(2):
            exchange = m.group(2).strip()

        # Fallback: ticker from FILENAME (e.g. bwp-20250930.htm) or title
        if not ticker or not _is_valid_ticker_candidate(ticker):
            for pattern in (
                r"<FILENAME>([a-zA-Z]+)-\d{8}\.htm",
                r"<title>([a-zA-Z]+)-\d{8}</title>",
                r"<DESCRIPTION>\s*([A-Z]{2,5})\s+\d{4}",
            ):
                m = re.search(pattern, content)
                if m:
                    cand = m.group(1).strip().upper()
                    if _is_valid_ticker_candidate(cand):
                        ticker = cand
                        break
    except (OSError, UnicodeDecodeError) as e:
        logger.debug("extract_dei_from_txt_fallback failed for %s: %s", txt_path, e)
    return ticker, exchange


def extract_dei_ticker_exchange(data: Dict[str, Any]) -> Tuple[Optional[str], Optional[str]]:
    """
    Extract ticker and exchange from XBRL data's "other" facts.
    Ticker: dei:TradingSymbol only (no other ticker source in .txt/XBRL).
    Exchange: dei:SecurityExchangeName.
    Returns (ticker, exchange); uses first value found for each. Skips values that look like iXBRL error strings.
    Safe to call on raw or adj-shaped data.
    """
    ticker: Optional[str] = None
    exchange: Optional[str] = None
    for fact in data.get("other") or []:
        if not isinstance(fact, dict):
            continue
        concept = fact.get("concept") or ""
        val = fact.get("value")
        if _is_invalid_dei_value(val):
            continue
        if concept == "dei:TradingSymbol" and ticker is None:
            ticker = str(val).strip()
        elif concept == "dei:SecurityExchangeName" and exchange is None:
            exchange = str(val).strip()
    return ticker, exchange


def xbrl_to_adj(
    data: Dict[str, Any],
    ticker: Optional[str] = None,
    exchange: Optional[str] = None,
    accession: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Convert XBRL JSON (raw or adj-shaped) to adj output: { date: { balance_sheet: { concept: value }, ... } }.
    Optionally include top-level "ticker" and "exchange" (reserved; consumers can ignore).
    Accepts raw xbrl.json (contexts/contextRef); runs inline+flatten in memory first.
    If the preferred period yields an essentially empty balance sheet, tries the next candidate periods.
    """
    document_period_end = _get_document_period_end(data)
    data = _ensure_adj_shape(data)
    # Preserve all DEI facts (for downstream metadata needs).
    # We store all observed values per concept as a list to avoid dropping
    # duplicates across contexts or different securities/classes.
    dei: Dict[str, List[str]] = {}
    for fact in data.get("other") or []:
        if not isinstance(fact, dict):
            continue
        concept = (fact.get("concept") or "").strip()
        if not concept.startswith("dei:"):
            continue
        val = fact.get("value")
        if val is None or _is_invalid_dei_value(val):
            continue
        sval = str(val).strip()
        # Normalize checkbox glyphs sometimes used in iXBRL rendering.
        # In DEI these are effectively boolean flags.
        if sval == "☒":
            sval = "true"
        elif sval == "☐":
            sval = "false"
        if not sval:
            continue
        dei.setdefault(concept, [])
        if sval not in dei[concept]:
            dei[concept].append(sval)
    units = data.get("units", {})
    candidates = _filing_period_candidates(data, document_period_end=document_period_end)
    if not candidates:
        return {}
    for filing_period in candidates:
        out: Dict[str, Any] = {}
        for src_key, out_key in _STATEMENT_KEY_MAP.items():
            arr = data.get(src_key)
            if isinstance(arr, list):
                out[out_key] = _pivot_fact_list(arr, units, filing_period)
            else:
                out[out_key] = {} if filing_period else []
        if not _is_adj_result_essentially_empty(out):
            shares_agg = _extract_share_aggregate(data, filing_period)
            if any(v is not None for v in shares_agg.values()):
                out["shares"] = shares_agg
            result: Dict[str, Any] = {}
            if ticker is not None:
                result["ticker"] = ticker
            if exchange is not None:
                result["exchange"] = exchange
            if dei:
                result["dei"] = dei
            result[filing_period] = out
            return result
    filing_period = candidates[0]
    out = {}
    for src_key, out_key in _STATEMENT_KEY_MAP.items():
        arr = data.get(src_key)
        if isinstance(arr, list):
            out[out_key] = _pivot_fact_list(arr, units, filing_period)
        else:
            out[out_key] = {} if filing_period else []
    shares_agg = _extract_share_aggregate(data, filing_period)
    if any(v is not None for v in shares_agg.values()):
        out["shares"] = shares_agg
    result = {}
    if ticker is not None:
        result["ticker"] = ticker
    if exchange is not None:
        result["exchange"] = exchange
    if dei:
        result["dei"] = dei
    result[filing_period] = out
    return result


def _accession_from_path(path: Path) -> str:
    """Derive accession code from input path (e.g. 0000320193-25-000073.xbrl.json -> 0000320193-25-000073)."""
    stem = path.stem
    if stem.endswith(".xbrl"):
        return stem[: -len(".xbrl")]
    return stem


if __name__ == "__main__":
    import json
    import sys

    if len(sys.argv) >= 2 and Path(sys.argv[1]).is_dir():
        # Regenerate empty .json from .xbrl.json in directory: python -m ... <dir>
        dir_path = Path(sys.argv[1])
        regenerated = 0
        for json_path in sorted(dir_path.glob("*.json")):
            if json_path.name.endswith(".xbrl.json") or json_path.name.endswith(".html.json"):
                continue
            try:
                adj = json.loads(json_path.read_text(encoding="utf-8"))
            except Exception:
                continue
            if not is_essentially_empty_adj(adj):
                continue
            xbrl_path = dir_path / json_path.name.replace(".json", ".xbrl.json")
            if not xbrl_path.exists():
                continue
            try:
                data = json.loads(xbrl_path.read_text(encoding="utf-8"))
                ticker, exchange = extract_dei_ticker_exchange(data)
                acc = _accession_from_path(xbrl_path)
                result = xbrl_to_adj(data, ticker=ticker, exchange=exchange, accession=acc)
                if not is_essentially_empty_adj(result):
                    json_path.write_text(json.dumps(result, indent=2, ensure_ascii=False), encoding="utf-8")
                    regenerated += 1
                    print(json_path.name, file=sys.stderr)
            except Exception as e:
                print(f"{json_path.name}: {e}", file=sys.stderr)
        print(f"Regenerated {regenerated} empty adj file(s)", file=sys.stderr)
        sys.exit(0)

    default_path = (
        Path(__file__).resolve().parent.parent.parent.parent
        / "storage/dev/fundamentals/edgar/filings/2025/QTR3/10-Q/0000320193-25-000073.xbrl.json"
    )
    path = Path(sys.argv[1]) if len(sys.argv) > 1 else default_path
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    accession = _accession_from_path(path)
    result = xbrl_to_adj(data, accession=accession)
    # Adj output: same directory as input, accession.json
    if path.parent.name == accession:
        out_path = path.parent.parent / f"{accession}.json"
    else:
        out_path = path.parent / f"{accession}.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2, ensure_ascii=False)
    print(f"Wrote: {out_path}", file=sys.stderr)
