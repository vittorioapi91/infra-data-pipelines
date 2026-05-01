# EDGAR HTML rules

YAML-based patterns for scraping narrative content and forward-looking guidance from SEC EDGAR HTML filings.

## Files

| File | Purpose |
|------|---------|
| `8k_guidance-detection.yaml` | Document-level heuristics: which DOCUMENT blocks to process for guidance |
| `8k_guidance-earnings.yaml` | EPS, net income, qualitative earnings extraction patterns |
| `8k_guidance-revenues.yaml` | Revenue range/single patterns; metric labels; exclude rules |
| `8k_guidance-margins.yaml` | Adjusted/GAAP gross margin extraction patterns |
| `8k_guidance-context.yaml` | Period detection (FY/Q), vs_prior (raised/lowered), context (forward-looking vs reported) |

## Usage

The document-level rules (`8k_guidance-detection.yaml`) are loaded automatically when available. The extraction patterns in the other files define the regex and metadata; the heuristics in `filings_scraper_html_heuristics.py` use equivalent hardcoded patterns as fallback.

To add or change rules, edit the YAML and restart the scraper. Regex strings use standard Python regex syntax; `flags: I` means case-insensitive.
