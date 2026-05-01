#!/usr/bin/env bash
# Run EDGAR download + scrape for 10-Q from 2001 QTR1 to current quarter.
# Usage: from repo root, ./scripts/edgar_run_2001_to_today.sh
# Or: bash scripts/edgar_run_2001_to_today.sh

set -e
cd "$(dirname "$0")/.."
FORM_TYPE="${EDGAR_FORM_TYPE:-10-K}"
END_YEAR="${EDGAR_END_YEAR:-2025}"
START_YEAR="${EDGAR_START_YEAR:-1993}"

for year in $(seq "$START_YEAR" "$END_YEAR"); do
  for quarter in QTR1 QTR2 QTR3 QTR4; do
    echo ""
    echo "=== $year $quarter $FORM_TYPE ==============="
    python -m src.fundamentals.edgar --download-raw-quarter-filings --year "$year" --quarter "$quarter" --form-type "$FORM_TYPE" --archive-to-zip || true
  done
done
echo "Done."