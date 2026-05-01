-- View: sector_location_weights
-- Groups detailed holdings by ETF ticker, sector, and location; sums weight per group.
-- Uses holdings_detailed: etf_ticker, sector, location, weight (decimal).

DROP VIEW IF EXISTS sector_location_weights;

CREATE VIEW sector_location_weights AS
SELECT
    etf_ticker,
    sector_norm AS sector,
    location_norm AS location,
    SUM(COALESCE(weight, 0)) AS weight_pct
FROM (
    SELECT
        etf_ticker,
        COALESCE(NULLIF(TRIM(sector), ''), 'Unknown') AS sector_norm,
        COALESCE(NULLIF(TRIM(location), ''), 'Unknown') AS location_norm,
        weight
    FROM holdings_detailed
    WHERE etf_ticker IS NOT NULL
      AND TRIM(COALESCE(etf_ticker, '')) <> ''
) sub
GROUP BY etf_ticker, sector_norm, location_norm;
