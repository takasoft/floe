-- gold.regional_safety_summary
-- Regional-level safety dashboard sliced by event type. Reads directly from
-- silver.safety_events_enriched (skipping the per-DA gold layer) — this is
-- the second branch of the fan-out from silver.

CREATE DYNAMIC TABLE gold.regional_safety_summary
  LAG = '15 minutes'
  REFRESH_MODE = 'FULL'
  AS
  SELECT
    region,
    event_type,
    severity,
    COUNT(*)                  AS event_count,
    COUNT(DISTINCT da_id)     AS distinct_das_involved,
    MIN(occurred_at)          AS first_seen_at,
    MAX(occurred_at)          AS last_seen_at
  FROM silver.safety_events_enriched
  GROUP BY region, event_type, severity;
