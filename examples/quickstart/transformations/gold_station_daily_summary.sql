-- gold.station_daily_summary
-- Station-level rollup of per-DA daily performance. Demonstrates a 3-hop
-- pipeline: bronze → silver → gold (da_daily) → gold (station_daily).
-- A change to bronze.raw_deliveries will cascade through all three layers.

CREATE DYNAMIC TABLE gold.station_daily_summary
  LAG = '20 minutes'
  REFRESH_MODE = 'FULL'
  AS
  SELECT
    region,
    station_id,
    delivery_date,
    COUNT(DISTINCT da_id)            AS active_das,
    SUM(total_deliveries)            AS total_deliveries,
    SUM(on_time_deliveries)          AS on_time_deliveries,
    ROUND(
      100.0 * SUM(on_time_deliveries) / NULLIF(SUM(total_deliveries), 0), 2
    )                                AS station_on_time_pct,
    SUM(safety_events)               AS safety_events,
    SUM(high_severity_events)        AS high_severity_events
  FROM gold.da_daily_performance
  GROUP BY region, station_id, delivery_date;
