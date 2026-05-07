-- gold.delivery_quality_daily
-- Per-day defect rate, per region/station. Demonstrates Floe's sliding-window
-- freshness contract: only the last 14 day-partitions are kept fresh, and they
-- must be re-touched at most every 2 days.
--
-- Defect data trickles in late (a delivery on day-7 might receive a complaint
-- on day-0). Floe re-runs the in-window partitions when either:
--   1. Upstream commits arrive (defects, deliveries), OR
--   2. The freshness deadline lapses (PARTITION_FRESHNESS = 2 days).
--
-- Partitions older than 14 days are NOT touched by this DIT — they remain at
-- whatever value they had at last refresh. This matches the real-world
-- contract: "old data is allowed to be stale; new data must be fresh."

CREATE DYNAMIC TABLE gold.delivery_quality_daily
  PARTITION BY (delivery_date)
  PARTITION_WINDOW = '14 days'
  PARTITION_FRESHNESS = '2 days'
  REFRESH_MODE = 'INCREMENTAL'
  AS
  SELECT
    d.delivery_date,
    d.region,
    d.station_id,
    COUNT(*)                                                         AS total_deliveries,
    COUNT(f.defect_id)                                               AS defect_count,
    ROUND(100.0 * COUNT(f.defect_id) / COUNT(*), 2)                  AS defect_rate_pct,
    SUM(CASE WHEN f.severity = 'HIGH' THEN 1 ELSE 0 END)             AS high_severity_defects
  FROM silver.deliveries_enriched d
  LEFT JOIN bronze.delivery_defects f
    ON d.delivery_id = f.delivery_id
  GROUP BY d.delivery_date, d.region, d.station_id;
