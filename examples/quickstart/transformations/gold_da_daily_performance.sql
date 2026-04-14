-- gold.da_daily_performance
-- Per-DA per-day KPIs: deliveries, on-time rate, average delivery time, and
-- safety event counts. Demonstrates fan-in: this DIT joins TWO upstream
-- silver tables and uses a CTE to pre-aggregate safety events.
-- FULL refresh because aggregations span the whole table.

CREATE DYNAMIC TABLE gold.da_daily_performance
  LAG = '15 minutes'
  REFRESH_MODE = 'FULL'
  AS
  WITH safety_per_day AS (
    SELECT
      da_id,
      event_date,
      COUNT(*)                                          AS safety_events,
      SUM(CASE WHEN severity = 'HIGH' THEN 1 ELSE 0 END) AS high_severity_events
    FROM silver.safety_events_enriched
    GROUP BY da_id, event_date
  ),
  deliveries_per_day AS (
    SELECT
      da_id,
      da_name,
      region,
      station_id,
      delivery_date,
      COUNT(*)                                                                AS total_deliveries,
      SUM(CASE WHEN on_time THEN 1 ELSE 0 END)                                AS on_time_deliveries,
      ROUND(100.0 * SUM(CASE WHEN on_time THEN 1 ELSE 0 END) / COUNT(*), 2)   AS on_time_pct,
      ROUND(AVG(delivery_seconds) / 60.0, 1)                                  AS avg_delivery_minutes
    FROM silver.deliveries_enriched
    GROUP BY da_id, da_name, region, station_id, delivery_date
  )
  SELECT
    d.da_id,
    d.da_name,
    d.region,
    d.station_id,
    d.delivery_date,
    d.total_deliveries,
    d.on_time_deliveries,
    d.on_time_pct,
    d.avg_delivery_minutes,
    COALESCE(s.safety_events, 0)        AS safety_events,
    COALESCE(s.high_severity_events, 0) AS high_severity_events
  FROM deliveries_per_day d
  LEFT JOIN safety_per_day s
    ON d.da_id = s.da_id AND d.delivery_date = s.event_date;
