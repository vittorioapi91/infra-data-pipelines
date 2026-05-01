# iShares DB Views

SQL views for the ishares PostgreSQL database.

## sector_location_weights

Groups holdings by `etf_ticker`, `sector`, and `location`; sums weight (%) per group.

**Requires** the `holdings_detailed` table with columns: `etf_ticker`, `sector`, `location`, `weight`.

### Apply

```bash
# After upload, apply the view
psql -d ishares -f src/markets/ishares/views-sql/sector_location_weights.sql
```

Or run the SQL directly from Python after `upload_holdings()`.
