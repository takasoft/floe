-- silver.deliveries_enriched
-- Joins raw delivery facts with the DA and route dimensions so downstream
-- aggregations don't have to re-join. Refreshes incrementally — when new
-- bronze.raw_deliveries snapshots arrive, this DIT picks them up.

CREATE DYNAMIC TABLE silver.deliveries_enriched
  LAG = '5 minutes'
  REFRESH_MODE = 'INCREMENTAL'
  AS
  SELECT
    d.delivery_id,
    d.da_id,
    da.name        AS da_name,
    da.region,
    da.station_id,
    d.route_id,
    r.miles,
    r.est_minutes,
    d.on_time,
    d.delivery_seconds,
    d.delivered_at,
    CAST(d.delivered_at AS DATE) AS delivery_date
  FROM bronze.raw_deliveries d
  JOIN bronze.delivery_associates da ON d.da_id = da.da_id
  JOIN bronze.routes r              ON d.route_id = r.route_id;
