# Accounting consistency checks

This document describes the internal consistency checks run on scraped financial data. Two shapes are supported:

1. **XBRL adj** — single-period data (one date key → `balance_sheet`, `income_stmt`, `cashflow`, `other`). Produced by `filings_scraper_xbrl_adj.py` as `accession-code.xbrl.adj.json`. Rules: `rules-xbrl/10k.yaml`, `10q.yaml`; implementation: `10k.py`, `10q.py`.
2. **Legacy (PEM/IMS text)** — top-level `accession`, `balance_sheet`, `income_statement`, `cash_flow_statement`. Produced by `filings_scraper_legacy.py`. Rules: `rules-legacy/10k_legacy.yaml`, `10q_legacy.yaml`; implementation: `10k_legacy.py`, `10q_legacy.py`.

`run_all_checks(data, form_type=...)` dispatches by data shape (legacy vs XBRL) and form type (10-K vs 10-Q). Checks use a **relative tolerance** (default 5%) for comparisons.

---

## 1. Balance sheet identity (A = L + E)

**Identity:** Total assets must equal total liabilities plus stockholders’ equity.

**Formula:**
```
Assets = Liabilities + Stockholders' Equity
```
Equivalently, assets must equal the reported **Liabilities and Stockholders’ Equity** total.

**Concepts used:**

| Concept | Section | Description |
|--------|---------|-------------|
| `us-gaap:Assets` | balance_sheet | Total assets |
| `us-gaap:Liabilities` | balance_sheet | Total liabilities |
| `us-gaap:StockholdersEquity` | balance_sheet | Total stockholders’ equity |
| `us-gaap:LiabilitiesAndStockholdersEquity` | balance_sheet | Total liabilities and equity (alternative) |

**Notes:** Fundamental accounting equation. If only `LiabilitiesAndStockholdersEquity` is present (no separate Liabilities/Equity), the check uses Assets vs that total.

---

## 2. Assets = Current + Noncurrent

**Identity:** Total assets must equal the sum of current and noncurrent assets.

**Formula:**
```
Assets = AssetsCurrent + AssetsNoncurrent
```

**Concepts used:**

| Concept | Section | Description |
|--------|---------|-------------|
| `us-gaap:Assets` | balance_sheet | Total assets |
| `us-gaap:AssetsCurrent` | balance_sheet | Current assets |
| `us-gaap:AssetsNoncurrent` | balance_sheet | Noncurrent assets |

---

## 3. Liabilities = Current + Noncurrent

**Identity:** Total liabilities must equal the sum of current and noncurrent liabilities.

**Formula:**
```
Liabilities = LiabilitiesCurrent + LiabilitiesNoncurrent
```

**Concepts used:**

| Concept | Section | Description |
|--------|---------|-------------|
| `us-gaap:Liabilities` | balance_sheet | Total liabilities |
| `us-gaap:LiabilitiesCurrent` | balance_sheet | Current liabilities |
| `us-gaap:LiabilitiesNoncurrent` | balance_sheet | Noncurrent liabilities |

---

## 4. Revenue − Cost = Gross profit

**Identity:** Revenue minus cost of goods and services sold must equal gross profit.

**Formula:**
```
Revenue − CostOfGoodsAndServicesSold = GrossProfit
```

**Concepts used:**

| Concept | Section | Description |
|--------|---------|-------------|
| `us-gaap:RevenueFromContractWithCustomerExcludingAssessedTax` | income_stmt | Revenue (total, no segment dimension) |
| `us-gaap:CostOfGoodsAndServicesSold` | other | Cost of goods and services sold |
| `us-gaap:GrossProfit` | income_stmt | Gross profit |

**Notes:** Cost is read from `other` because it often appears there in the XBRL layout; revenue and gross profit from `income_stmt`.

---

## 5. Gross profit − Operating expenses = Operating income

**Identity:** Gross profit minus operating expenses must equal operating income (loss).

**Formula:**
```
GrossProfit − OperatingExpenses = OperatingIncomeLoss
```

**Concepts used:**

| Concept | Section | Description |
|--------|---------|-------------|
| `us-gaap:GrossProfit` | income_stmt | Gross profit |
| `us-gaap:OperatingExpenses` | income_stmt | Total operating expenses |
| `us-gaap:OperatingIncomeLoss` | income_stmt | Operating income (loss) |

---

## 6. Pretax income − Tax = Net income

**Identity:** Income from continuing operations before tax minus income tax expense/benefit must equal net income (loss).

**Formula:**
```
IncomeLossFromContinuingOperationsBeforeIncomeTaxes... − IncomeTaxExpenseBenefit = NetIncomeLoss
```

**Concepts used:**

| Concept | Section | Description |
|--------|---------|-------------|
| `us-gaap:IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest` | income_stmt | Pretax income (continuing) |
| `us-gaap:IncomeTaxExpenseBenefit` | other | Income tax expense (positive) or benefit (negative) |
| `us-gaap:NetIncomeLoss` | income_stmt | Net income (loss) |

**Notes:** Tax benefit is typically negative; the formula uses pretax − tax so that expense reduces net income.

---

## 7. Comprehensive income (Net + OCI)

**Identity:** Net income plus other comprehensive income (OCI) must equal comprehensive income (net of tax).

**Formula:**
```
NetIncomeLoss + OtherComprehensiveIncomeLossNetOfTaxPortionAttributableToParent = ComprehensiveIncomeNetOfTax
```

**Concepts used:**

| Concept | Section | Description |
|--------|---------|-------------|
| `us-gaap:NetIncomeLoss` | income_stmt | Net income (loss) |
| `us-gaap:OtherComprehensiveIncomeLossNetOfTaxPortionAttributableToParent` | income_stmt | OCI, net of tax, parent portion |
| `us-gaap:ComprehensiveIncomeNetOfTax` | income_stmt | Total comprehensive income |

**Notes:** If OCI is missing, it is treated as zero for the computed total.

---

## 8. Cash flow articulation

**Identity:** The sum of net cash from operating, investing, and financing activities must equal the reported change in cash (and equivalents).

**Formula:**
```
NetCashProvidedByUsedInOperatingActivities
  + NetCashProvidedByUsedInInvestingActivities
  + NetCashProvidedByUsedInFinancingActivities
  = Change in cash (period increase/decrease including FX)
```

**Concepts used:**

| Concept | Section | Description |
|--------|---------|-------------|
| `us-gaap:NetCashProvidedByUsedInOperatingActivities` | cashflow | Net cash from operations |
| `us-gaap:NetCashProvidedByUsedInInvestingActivities` | cashflow | Net cash from investing |
| `us-gaap:NetCashProvidedByUsedInFinancingActivities` | cashflow | Net cash from financing |
| `us-gaap:CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalentsPeriodIncreaseDecreaseIncludingExchangeRateEffect` | other | Change in cash and equivalents (including restricted, FX) |

**Notes:** Single-period data only; we do not verify beginning/ending cash against balance sheet here.

---

## 9. Equity components

**Identity:** Stockholders’ equity must equal the sum of its main components (common stock + APIC, retained earnings, accumulated other comprehensive income). Other components (e.g. noncontrolling interest) may exist and can cause a residual.

**Formula:**
```
StockholdersEquity ≈ CommonStocksIncludingAdditionalPaidInCapital
  + RetainedEarningsAccumulatedDeficit
  + AccumulatedOtherComprehensiveIncomeMember (equity component)
```

**Concepts used:**

| Concept | Section | Description |
|--------|---------|-------------|
| `us-gaap:StockholdersEquity` | balance_sheet | Total stockholders’ equity |
| `us-gaap:CommonStocksIncludingAdditionalPaidInCapital` | other | Common stock and APIC (or dimensioned `StockholdersEquity [CommonStockIncludingAdditionalPaidInCapital]` from balance_sheet) |
| `us-gaap:RetainedEarningsAccumulatedDeficit` | other | Retained earnings (or `StockholdersEquity [RetainedEarnings]`) |
| `us-gaap:StockholdersEquity [AccumulatedOtherComprehensiveIncome]` | balance_sheet | AOCI component of equity |

**Notes:** Some filers report only dimensioned equity components; the check uses `other` totals when present, otherwise the dimensioned balance_sheet members. A small difference is allowed (tolerance) and may reflect other equity line items (e.g. NCI, treasury stock).

---

## 10. Operating expenses = R&D + SG&A

**Identity:** When a filer reports both components, total operating expenses must equal R&D plus selling, general and administrative expense.

**Formula:**
```
OperatingExpenses = ResearchAndDevelopmentExpense + SellingGeneralAndAdministrativeExpense
```

**Concepts used:**

| Concept | Section | Description |
|--------|---------|-------------|
| `us-gaap:OperatingExpenses` | income_stmt | Total operating expenses |
| `us-gaap:ResearchAndDevelopmentExpense` | other | R&D expense |
| `us-gaap:SellingGeneralAndAdministrativeExpense` | other | SG&A expense |

**Notes:** Skipped (pass) when any of the three concepts is missing (e.g. filer does not break out R&D/SG&A).

---

## 11. Operating income + Nonoperating = Pretax

**Identity:** Operating income (loss) plus nonoperating income/expense must equal income from continuing operations before tax.

**Formula:**
```
OperatingIncomeLoss + NonoperatingIncomeExpense = IncomeLossFromContinuingOperationsBeforeIncomeTaxes...
```

**Concepts used:**

| Concept | Section | Description |
|--------|---------|-------------|
| `us-gaap:OperatingIncomeLoss` | income_stmt | Operating income (loss) |
| `us-gaap:NonoperatingIncomeExpense` | other | Nonoperating income (positive) or expense (negative) |
| `us-gaap:IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest` | income_stmt | Pretax income (continuing) |

**Notes:** If `NonoperatingIncomeExpense` is missing, it is treated as zero. Skipped when pretax or operating income is missing.

---

## 12. Revenue (Product + Service) = Total revenue

**Identity:** When revenue is broken out by product and service, the sum must equal total revenue.

**Formula:**
```
RevenueFromContractWithCustomerExcludingAssessedTax [Product]
  + RevenueFromContractWithCustomerExcludingAssessedTax [Service]
  = RevenueFromContractWithCustomerExcludingAssessedTax
```

**Concepts used:**

| Concept | Section | Description |
|--------|---------|-------------|
| `us-gaap:RevenueFromContractWithCustomerExcludingAssessedTax` | income_stmt | Total revenue |
| `us-gaap:RevenueFromContractWithCustomerExcludingAssessedTax [Product]` | income_stmt | Product revenue |
| `us-gaap:RevenueFromContractWithCustomerExcludingAssessedTax [Service]` | income_stmt | Service revenue |

**Notes:** Skipped when the filer does not report Product/Service breakdown.

---

## 13. Cost (Product + Service) = Total cost

**Identity:** When cost of goods and services is broken out by product and service, the sum must equal total cost.

**Formula:**
```
CostOfGoodsAndServicesSold [Product] + CostOfGoodsAndServicesSold [Service]
  = CostOfGoodsAndServicesSold
```

**Concepts used:**

| Concept | Section | Description |
|--------|---------|-------------|
| `us-gaap:CostOfGoodsAndServicesSold` | other | Total cost of goods and services sold |
| `us-gaap:CostOfGoodsAndServicesSold [Product]` | other | Product cost |
| `us-gaap:CostOfGoodsAndServicesSold [Service]` | other | Service cost |

**Notes:** Skipped when the filer does not report Product/Service cost breakdown.

---

## 14. Segment revenue sum = Total revenue

**Identity:** When revenue is reported for the five common geographic segments (Americas, Europe, Greater China, Japan, Rest of Asia Pacific), their sum must equal total revenue.

**Formula:**
```
Revenue [AmericasSegment] + [EuropeSegment] + [GreaterChinaSegment]
  + [JapanSegment] + [RestOfAsiaPacificSegment] = Total Revenue
```

**Concepts used:**

| Concept | Section | Description |
|--------|---------|-------------|
| `us-gaap:RevenueFromContractWithCustomerExcludingAssessedTax` | income_stmt | Total revenue |
| `us-gaap:RevenueFromContractWithCustomerExcludingAssessedTax [AmericasSegment]` | income_stmt | Americas segment revenue |
| (similarly Europe, GreaterChina, Japan, RestOfAsiaPacific segment suffixes) | income_stmt | Segment revenues |

**Notes:** Skipped when any of the five segment concepts or total revenue is missing. Applicable to filers that use this geographic breakdown (e.g. Apple).

---

## Data format

Input is a single JSON object with exactly **one** top-level key (the period end date, e.g. `"2025-06-30T00:00:00"`). The value is an object with four sections:

- **balance_sheet** — instant (point-in-time) balance sheet concepts → numeric values.
- **income_stmt** — income statement (and comprehensive income) concepts → numeric values.
- **cashflow** — cash flow statement concepts → numeric values.
- **other** — mixed (e.g. cost, tax, balance sheet detail, cash flow change) → numeric values.

Concept keys are US-GAAP (or extension) QNames, e.g. `us-gaap:Assets`. Dimensioned concepts include a suffix with the dimension member name; trailing `Member` is stripped (e.g. ` [Product]`, ` [AmericasSegment]`). Only **exact** key matches are used; section and key must match the tables above for each check.

---

## Running the checks

From the repo root:

```bash
# XBRL adj (10-Q or 10-K)
python src/fundamentals/edgar/accounting_checks/10q.py <path/to/accession.xbrl.adj.json>
python src/fundamentals/edgar/accounting_checks/10k.py <path/to/accession.xbrl.adj.json>

# Legacy scraped JSON (10-Q or 10-K)
python src/fundamentals/edgar/accounting_checks/10q_legacy.py <path/to/accession.json>
python src/fundamentals/edgar/accounting_checks/10k_legacy.py <path/to/accession.json>
```

The path argument is required; there is no default. Exit code `0` if all checks pass, `1` otherwise. Failed checks print a short message and the `detail` dict (reported vs computed, difference).

Programmatic use (dispatches by data shape and form type):

```python
from src.fundamentals.edgar.accounting_checks import run_all_checks

results = run_all_checks(data, form_type="10-Q")  # or "10-K"
# list of (check_name, passed, message, detail)
```
