# YFinance data type schemas

- One schema file per data type. Storage paths: `storage/<env>/markets/yfinance/<type>/<symbol>.json`.

| Type | Schema file | Shape (per symbol) |
|------|-------------|--------------------|
| equity_info | equity_info.yaml | Single object (Ticker.info) |
| analyst_recommendations | analyst_recommendations.yaml | Array of records |
| eps_revisions | eps_revisions.yaml | Array of records |
| revenue_estimates | revenue_estimates.yaml | Array of records |
| analyst_price_targets | analyst_price_targets.yaml | Single object or null |
| earnings_calendar | earnings_calendar.yaml | Array of records |
| earnings_history | earnings_history.yaml | Array of records |
| financial_statements | financial_statements.yaml | Object (income_stmt, balance_sheet, cashflow, etc.) |
| calendar | calendar.yaml | Object or null |
| sec_filings | sec_filings.yaml | Array of records |
