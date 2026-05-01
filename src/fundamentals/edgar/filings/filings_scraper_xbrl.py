"""
Scrape SEC EDGAR filings: XBRL facts.

FilingsScraperInlineXBRL extracts structured iXBRL facts using Arelle.
"""

import json
import logging
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

try:
    from arelle.api.Session import Session
    from arelle.RuntimeOptions import RuntimeOptions
    from arelle import XbrlConst
    from arelle.ModelDtsObject import ModelResource
except ImportError:
    raise ImportError("arelle-release is required. Install with: pip install arelle-release[EFM]")

logger = logging.getLogger(__name__)

# Exceptions that can occur when traversing Arelle model objects (varying XBRL structure)
_XBRL_COLLECT_ERRORS = (AttributeError, KeyError, TypeError, ValueError, IndexError, LookupError)

# Filters applied per form type when apply_filters=True.
# 10-Q: skip redundant name, id (from 10-Q scrape workflow).
FILTERS_BY_FORM_TYPE: Dict[str, set] = {
    "10-Q": {"skip_name", "skip_id"},
    "10-K": {"skip_name", "skip_id"},
    "8-K": {"skip_name", "skip_id"},
}
_DEFAULT_FILTERS = FILTERS_BY_FORM_TYPE["10-Q"]

# Concept name patterns for statement classification (us-gaap local names)
_BALANCE_SHEET_PATTERNS = (
    "Assets", "Liabilities", "Equity", "StockholdersEquity", "MembersEquity",
    "LiabilitiesAndStockholdersEquity", "AssetsCurrent", "LiabilitiesCurrent",
)
_INCOME_STATEMENT_PATTERNS = (
    "Revenues", "Revenue", "NetSales", "CostOfRevenue", "GrossProfit",
    "OperatingExpenses", "OperatingIncomeLoss", "NetIncomeLoss", "IncomeLoss",
    "ProfitLoss", "ComprehensiveIncome",
)
_CASH_FLOW_PATTERNS = (
    "CashProvidedByUsedIn", "NetCashProvidedByUsedIn", "CashFlow",
    "ProceedsFrom", "PaymentsTo", "CashAndCashEquivalentsPeriodIncreaseDecrease",
)


class FilingsScraperInlineXBRL:
    """
    Scraper for extracting XBRL facts from SEC EDGAR iXBRL filings.

    Uses Arelle for schema-aware parsing.
    Arelle uses shared global state: only one Session per process; use
    process pools for parallelism.
    """

    def __init__(self, validate: bool = False, apply_filters: bool = True):
        """
        Args:
            validate: Whether to run XBRL validation (slower, produces validation logs).
            apply_filters: If True (production), skip redundant fields (name, id). If False (tests), include all.
        """
        self.validate = validate
        self.apply_filters = apply_filters

    def scrape_filing(
        self,
        path: Union[str, Path],
        form_type: Optional[str] = None,
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        Parse an iXBRL filing and return facts grouped by statement.

        Uses Arelle presentation API when available; falls back to concept-name
        heuristics for Balance Sheet, Income Statement, Cash Flow.

        Args:
            path: Path to the iXBRL file (.htm, .html, .txt with inline XBRL).
            form_type: Form type (10-K, 10-Q, 8-K, etc.) for filter selection.
                Inferred from parent dir name if not provided.

        Returns:
            Dict with keys: balance_sheet, income_statement, cash_flow, other.
        """
        path = Path(path).resolve()
        self._scrape_form_type = form_type or self._infer_form_type(path)
        if not path.exists():
            raise FileNotFoundError(f"Filing not found: {path}")

        # Quick guard: ensure the entrypoint looks like inline XBRL before invoking Arelle.
        # This avoids confusing errors when pointing the scraper at non-iXBRL content.
        self._validate_inline_xbrl(path)

        options = RuntimeOptions(
            entrypointFile=str(path),
            internetConnectivity="online",
            keepOpen=True,
            validate=self.validate,
            logLevel="WARNING",
            logCodeFilter=r"(?!xmlSchema:syntax)(.+)",  # suppress xmlSchema:syntax (e.g. Entity 'nbsp' not defined)
            logFile="logToBuffer",  # buffer logs instead of printing to stderr
        )

        by_stmt: Dict[str, List[Dict[str, Any]]] = {
            "balance_sheet": [],
            "income_statement": [],
            "cash_flow": [],
            "other": [],
        }
        calculations: List[Dict[str, Any]] = []
        concept_labels: Dict[str, str] = {}
        concept_references: Dict[str, List[Dict[str, Any]]] = {}
        definition_relationships: List[Dict[str, Any]] = []
        contexts: Dict[str, Dict[str, Any]] = {}
        units: Dict[str, Dict[str, Any]] = {}
        footnotes: Dict[str, Dict[str, Any]] = {}
        fact_footnote_links: List[Dict[str, Any]] = []

        with Session() as session:
                session.run(options)
                for model_xbrl in session.get_models():
                    if not hasattr(model_xbrl, "facts"):
                        continue
                    # Collect labels, calculations, references, definition links (taxonomy-level)
                    self._collect_labels(model_xbrl, concept_labels)
                    self._collect_calculations(model_xbrl, calculations)
                    self._collect_references(model_xbrl, concept_references)
                    self._collect_definition_relationships(model_xbrl, definition_relationships)
                    # Collect contexts, units, footnotes (instance-level definitions)
                    self._collect_contexts(model_xbrl, contexts)
                    self._collect_units(model_xbrl, units)
                    self._collect_footnotes(model_xbrl, footnotes, fact_footnote_links)
                    # Try presentation API; fallback to concept heuristics
                    concept_to_stmt = self._get_concept_to_statement(model_xbrl)
                    for fact in model_xbrl.facts:
                        # Skip text blocks (overlap with HTML narrative scraper)
                        if self._is_text_block_fact(fact):
                            continue
                        fd = self._fact_to_dict(fact, concept_labels)
                        stmt = concept_to_stmt.get(
                            fd.get("concept"),
                            self._infer_statement_from_concept(fd.get("concept", "")),
                        )
                        by_stmt[stmt].append(fd)

        out: Dict[str, Any] = {
            "balance_sheet": by_stmt["balance_sheet"],
            "income_statement": by_stmt["income_statement"],
            "cash_flow": by_stmt["cash_flow"],
            "other": by_stmt["other"],
        }
        if calculations:
            out["calculations"] = calculations
        if concept_labels:
            out["concept_labels"] = concept_labels
        if concept_references:
            out["concept_references"] = concept_references
        if definition_relationships:
            out["definition_relationships"] = definition_relationships
        if contexts:
            out["contexts"] = contexts
        if units:
            out["units"] = units
        if footnotes:
            out["footnotes"] = footnotes
        if fact_footnote_links:
            out["fact_footnote_links"] = fact_footnote_links

        return out

    @staticmethod
    def _validate_inline_xbrl(path: Path) -> None:
        """
        Best-effort validation that the entrypoint contains inline XBRL markup.

        Checks for:
        - inline XBRL namespace declarations; and
        - presence of ix:* inline facts (nonFraction / nonNumeric).
        """
        try:
            # Read a prefix of the file to avoid loading huge filings entirely into memory.
            # Many filings declare namespaces and ix:* tags near the top.
            with path.open("r", encoding="utf-8", errors="ignore") as f:
                head = f.read(512_000)
        except OSError as e:
            raise RuntimeError(f"Unable to read filing for inline XBRL validation: {path}") from e

        lower = head.lower()
        has_ns = (
            "http://www.xbrl.org/2008/inlinexbrl" in lower
            or "http://www.xbrl.org/2013/inlinexbrl" in lower
        )
        has_ix_tags = ("<ix:nonfraction" in lower) or ("<ix:nonnumeric" in lower) or ("<ix:" in lower)

        if not (has_ns and has_ix_tags):
            raise RuntimeError(
                f"Filing does not appear to be inline XBRL (missing ix namespace or ix:* tags): {path}"
            )

    def _get_concept_to_statement(self, model_xbrl) -> Dict[str, str]:
        """Use presentation linkbase when available; return empty dict to fallback."""
        mapping: Dict[str, str] = {}
        try:
            rs = model_xbrl.relationshipSet(XbrlConst.parentChild)
            if not rs or not rs.linkRoleUris:
                return mapping
            # EFM roleTypes map linkrole -> tableCode (BalanceSheet, StatementOfOperations, etc.)
            role_types = getattr(model_xbrl, "roleTypes", None) or {}
            for role_uri in rs.linkRoleUris:
                for rt in role_types.get(role_uri, ()):
                    code = getattr(rt, "tableCode", None) or ""
                    if "BalanceSheet" in code or "Balance" in code:
                        stmt = "balance_sheet"
                    elif "Operations" in code or "Income" in code:
                        stmt = "income_statement"
                    elif "CashFlow" in code or "Cash" in code:
                        stmt = "cash_flow"
                    else:
                        continue
                    rel_set = model_xbrl.relationshipSet(XbrlConst.parentChild, role_uri)
                    for root in getattr(rel_set, "rootConcepts", ()) or []:
                        self._collect_concepts_from_tree(rel_set, root, stmt, mapping)
        except _XBRL_COLLECT_ERRORS as e:
            logger.debug("_get_concept_to_statement failed: %s", e)
        return mapping

    def _collect_concepts_from_tree(self, rel_set, concept, stmt: str, mapping: Dict[str, str]) -> None:
        """Recursively add concept qnames to mapping."""
        if concept is None:
            return
        mapping[str(concept.qname)] = stmt
        for rel in rel_set.fromModelObject(concept):
            self._collect_concepts_from_tree(rel_set, rel.toModelObject, stmt, mapping)

    @staticmethod
    def _iter_relationships(rs, *, require_tgt_model_resource: bool = False):
        """Yield (rel, from_obj, to_obj) for each relationship in rs.modelRelationships."""
        for rel in getattr(rs, "modelRelationships", ()) or []:
            src = getattr(rel, "fromModelObject", None)
            tgt = getattr(rel, "toModelObject", None)
            if src is None or tgt is None:
                continue
            if require_tgt_model_resource and not isinstance(tgt, ModelResource):
                continue
            yield rel, src, tgt

    def _is_text_block_fact(self, fact) -> bool:
        """True if fact is a text block (overlaps with HTML narrative scraper)."""
        concept = getattr(fact, "concept", None)
        name = ""
        if concept is not None:
            if getattr(concept, "isTextBlock", False):
                return True
            qn = getattr(concept, "qname", None)
            if qn:
                name = str(qn).split(":")[-1] if ":" in str(qn) else str(qn)
        if not name:
            qn = getattr(fact, "qname", None) or getattr(fact, "name", None)
            if qn:
                name = str(qn).split(":")[-1] if ":" in str(qn) else str(qn)
        return bool(name and ("TextBlock" in name or "DisclosureTextBlock" in name))

    def _collect_labels(self, model_xbrl, out: Dict[str, str]) -> None:
        """Populate concept_labels from element-label / concept-label linkbases."""
        try:
            for arcrole in (XbrlConst.elementLabel, XbrlConst.conceptLabel):
                rs = model_xbrl.relationshipSet(arcrole)
                if not rs:
                    continue
                for _rel, src, tgt in self._iter_relationships(rs, require_tgt_model_resource=True):
                    qn = str(src.qname) if hasattr(src, "qname") else None
                    if qn and qn not in out:
                        text = getattr(tgt, "textValue", None) or getattr(tgt, "text", None)
                        if text and isinstance(text, str) and text.strip():
                            out[qn] = text.strip()
        except _XBRL_COLLECT_ERRORS as e:
            logger.debug("_collect_labels failed: %s", e)

    def _collect_calculations(self, model_xbrl, out: List[Dict[str, Any]]) -> None:
        """Populate calculations from summation-item linkbase(s)."""
        try:
            for arcrole in (XbrlConst.summationItem, XbrlConst.summationItem11):
                rs = model_xbrl.relationshipSet(arcrole)
                if not rs:
                    continue
                for rel, parent, child in self._iter_relationships(rs):
                    w = getattr(rel, "weightDecimal", None) or getattr(rel, "weight", None)
                    entry = {
                        "from": str(parent.qname) if hasattr(parent, "qname") else None,
                        "to": str(child.qname) if hasattr(child, "qname") else None,
                        "weight": float(w) if w is not None else None,
                    }
                    if entry["from"] and entry["to"] and entry not in out:
                        out.append(entry)
        except _XBRL_COLLECT_ERRORS as e:
            logger.debug("_collect_calculations failed: %s", e)

    def _collect_references(self, model_xbrl, out: Dict[str, List[Dict[str, Any]]]) -> None:
        """Populate concept_references from concept-reference linkbase."""
        try:
            rs = model_xbrl.relationshipSet(XbrlConst.conceptReference)
            if not rs:
                return
            for _rel, src, tgt in self._iter_relationships(rs, require_tgt_model_resource=True):
                qn = str(src.qname) if hasattr(src, "qname") else None
                if not qn:
                    continue
                ref_parts: Dict[str, str] = {}
                for part in getattr(tgt, "iterchildren", lambda: ())():
                    if hasattr(part, "localName") and hasattr(part, "text"):
                        ref_parts[part.localName] = (part.text or "").strip()
                if ref_parts:
                    if qn not in out:
                        out[qn] = []
                    if ref_parts not in out[qn]:
                        out[qn].append(ref_parts)
        except _XBRL_COLLECT_ERRORS as e:
            logger.debug("_collect_references failed: %s", e)

    def _collect_definition_relationships(
        self, model_xbrl, out: List[Dict[str, Any]]
    ) -> None:
        """Populate definition links: general-special, dimension-domain, domain-member."""
        try:
            arcrole_map = {
                XbrlConst.generalSpecial: "general_special",
                XbrlConst.dimensionDomain: "dimension_domain",
                XbrlConst.domainMember: "domain_member",
            }
            seen: set = set()
            for arcrole, kind in arcrole_map.items():
                rs = model_xbrl.relationshipSet(arcrole)
                if not rs:
                    continue
                for _rel, src, tgt in self._iter_relationships(rs):
                    from_qn = str(src.qname) if hasattr(src, "qname") else None
                    to_qn = str(tgt.qname) if hasattr(tgt, "qname") else None
                    if from_qn and to_qn:
                        key = (kind, from_qn, to_qn)
                        if key not in seen:
                            seen.add(key)
                            out.append({
                                "arcrole": kind,
                                "from": from_qn,
                                "to": to_qn,
                            })
        except _XBRL_COLLECT_ERRORS as e:
            logger.debug("_collect_definition_relationships failed: %s", e)

    def _collect_contexts(
        self, model_xbrl, out: Dict[str, Dict[str, Any]]
    ) -> None:
        """Populate contexts: id -> {entity, period, dimensions}."""
        try:
            ctxs = getattr(model_xbrl, "contexts", None) or {}
            for ctx_id, ctx in ctxs.items():
                if ctx_id in out:
                    continue
                entity = getattr(ctx, "entityIdentifier", None)
                entity_dict: Optional[Dict[str, str]] = None
                if entity and isinstance(entity, (tuple, list)) and len(entity) >= 2:
                    entity_dict = {"scheme": str(entity[0]), "identifier": str(entity[1])}
                inst = getattr(ctx, "instant", None)
                start = getattr(ctx, "startDatetime", None)
                end = getattr(ctx, "endDatetime", None)
                period: Dict[str, Optional[str]] = {}
                if inst is not None:
                    period["instant"] = inst.isoformat() if hasattr(inst, "isoformat") else str(inst)
                else:
                    period["instant"] = None
                if start is not None:
                    period["startDate"] = start.isoformat()[:10] if hasattr(start, "isoformat") else str(start)[:10]
                else:
                    period["startDate"] = None
                if end is not None:
                    period["endDate"] = end.isoformat()[:10] if hasattr(end, "isoformat") else str(end)[:10]
                else:
                    period["endDate"] = None
                dims = getattr(ctx, "qnameDims", None) or {}
                dimensions: Dict[str, str] = {}
                for dim_qn, dim_val in dims.items():
                    member = getattr(dim_val, "memberQname", None)
                    if member is not None:
                        dimensions[str(dim_qn)] = str(member)
                entry: Dict[str, Any] = {}
                if entity_dict:
                    entry["entity"] = entity_dict
                entry["period"] = period
                if dimensions:
                    entry["dimensions"] = dimensions
                out[str(ctx_id)] = entry
        except _XBRL_COLLECT_ERRORS as e:
            logger.debug("_collect_contexts failed: %s", e)

    def _collect_units(self, model_xbrl, out: Dict[str, Dict[str, Any]]) -> None:
        """Populate units: id -> {measures: {numerator: [...], denominator: [...]}}."""
        try:
            unit_dict = getattr(model_xbrl, "units", None) or {}
            for unit_id, unit in unit_dict.items():
                if str(unit_id) in out:
                    continue
                measures = getattr(unit, "measures", None)
                num_list: List[str] = []
                denom_list: List[str] = []
                if measures and isinstance(measures, (tuple, list)) and len(measures) >= 2:
                    num_tup, denom_tup = measures[0], measures[1]
                    if num_tup:
                        num_list = [str(m) for m in num_tup]
                    if denom_tup:
                        denom_list = [str(m) for m in denom_tup]
                out[str(unit_id)] = {
                    "measures": {"numerator": num_list, "denominator": denom_list}
                }
        except _XBRL_COLLECT_ERRORS as e:
            logger.debug("_collect_units failed: %s", e)

    def _collect_footnotes(
        self,
        model_xbrl,
        out: Dict[str, Dict[str, Any]],
        fact_links: List[Dict[str, Any]],
    ) -> None:
        """Populate footnotes and fact-to-footnote links from factFootnote relationships."""
        try:
            rs = model_xbrl.relationshipSet(XbrlConst.factFootnote)
            if not rs:
                return
            seen_footnote_ids: set = set()
            for _rel, fact, fn in self._iter_relationships(rs):
                fn_id = getattr(fn, "id", None) or getattr(fn, "footnoteID", None)
                if fn_id and fn_id not in seen_footnote_ids:
                    seen_footnote_ids.add(fn_id)
                    text = getattr(fn, "text", None) or getattr(fn, "textValue", None) or ""
                    if hasattr(text, "strip"):
                        text = str(text).strip()
                    else:
                        text = str(text)
                    out[str(fn_id)] = {"text": text}
                    if hasattr(fn, "role") and fn.role:
                        out[str(fn_id)]["role"] = str(fn.role)
                link: Dict[str, Any] = {}
                if fact is not None:
                    qn = getattr(fact, "qname", None)
                    if qn:
                        link["concept"] = str(qn)
                    link["contextRef"] = getattr(fact, "contextRef", None)
                    link["unitRef"] = getattr(fact, "unitRef", None)
                if fn_id:
                    link["footnoteId"] = str(fn_id)
                if link:
                    fact_links.append(link)
        except _XBRL_COLLECT_ERRORS as e:
            logger.debug("_collect_footnotes failed: %s", e)

    def _infer_statement_from_concept(self, concept: str) -> str:
        """Infer statement from concept name (e.g. us-gaap:Revenues)."""
        local = concept.split(":")[-1] if concept else ""
        for p in _BALANCE_SHEET_PATTERNS:
            if p in local:
                return "balance_sheet"
        for p in _INCOME_STATEMENT_PATTERNS:
            if p in local:
                return "income_statement"
        for p in _CASH_FLOW_PATTERNS:
            if p in local:
                return "cash_flow"
        return "other"

    def _coerce_value(self, val: Any) -> Any:
        """Coerce string to int/float when appropriate; use float (not Decimal) for JSON."""
        if val is None or not isinstance(val, str):
            return val
        s = val.strip()
        if not s:
            return None
        try:
            if "." in s:
                return float(Decimal(s))
            return int(s)
        except (ValueError, InvalidOperation):
            return val

    def _infer_form_type(self, path: Path) -> str:
        """Infer form type from path (e.g. .../10-K/xxx.txt -> 10-K)."""
        parent = path.parent.name
        return parent if parent in FILTERS_BY_FORM_TYPE else "10-Q"

    def _active_filters(self) -> set:
        """Filters to apply for current form type when apply_filters=True."""
        if not self.apply_filters:
            return set()
        return FILTERS_BY_FORM_TYPE.get(
            getattr(self, "_scrape_form_type", "10-Q"),
            _DEFAULT_FILTERS,
        )

    def _fact_to_dict(self, fact, concept_labels: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
        """Convert Arelle ModelFact to a simple dict."""
        concept_labels = concept_labels or {}
        result: Dict[str, Any] = {}
        name_from_attrib: Optional[str] = None
        filters = self._active_filters()
        if hasattr(fact, "attrib") and fact.attrib:
            for k, v in fact.attrib.items():
                if k.startswith("{") and "}" in k:  # skip namespace/URL-style keys (e.g. {http://...}nil)
                    continue
                if k == "name":
                    name_from_attrib = v
                    if "skip_name" in filters:
                        continue  # redundant with concept
                elif k == "id" and "skip_id" in filters:
                    continue  # discard in production
                else:
                    result[k] = v
        qname = getattr(fact, "qname", None)
        concept = str(qname) if qname else name_from_attrib
        result["concept"] = concept
        label = concept_labels.get(concept)
        if label is None:
            concept_obj = getattr(fact, "concept", None)
            if concept_obj is not None and hasattr(concept_obj, "label"):
                try:
                    label = concept_obj.label(lang="en")
                except _XBRL_COLLECT_ERRORS as e:
                    logger.debug("label lookup failed for %s: %s", concept_obj, e)
        if label:
            result["label"] = label
        xval = getattr(fact, "xValue", None)
        sval = getattr(fact, "value", None)
        raw = xval if xval is not None else (sval if sval is not None else str(fact))
        # When iXBRL transform fails, Arelle can set value to "(ixTransformValueError)"; use raw text if available
        if isinstance(raw, str) and "(ixTransformValueError)" in raw:
            fallback = getattr(fact, "rawValue", None) or getattr(fact, "stringValue", None) or getattr(fact, "text", None)
            if fallback is not None:
                fallback = str(fallback).strip()
            if fallback:
                raw = fallback
        v = raw if not isinstance(raw, str) else self._coerce_value(raw)
        result["value"] = float(v) if isinstance(v, Decimal) else v
        return result

