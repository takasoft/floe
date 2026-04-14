-- silver.safety_events_enriched
-- Joins raw on-vehicle telemetry safety events with DA dimension so the
-- safety dashboard can be sliced by region and station without re-joining.

CREATE DYNAMIC TABLE silver.safety_events_enriched
  LAG = '5 minutes'
  REFRESH_MODE = 'INCREMENTAL'
  AS
  SELECT
    s.event_id,
    s.da_id,
    da.name       AS da_name,
    da.region,
    da.station_id,
    s.event_type,
    s.severity,
    s.occurred_at,
    CAST(s.occurred_at AS DATE) AS event_date
  FROM bronze.raw_safety_events s
  JOIN bronze.delivery_associates da ON s.da_id = da.da_id;
