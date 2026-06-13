-- Floe writes plain Apache Iceberg tables, so any engine can read and transform
-- them. This demo runs entirely in Flink SQL against the SAME catalog and
-- warehouse that Floe (DuckDB) uses: it reads a Floe-maintained silver table and
-- writes a new gold rollup back into the catalog, which Floe can then read.
--
-- The CREATE CATALOG / USE CATALOG statements are generated at runtime by
-- run-demo.sh (they need the object-store credentials), so this file only
-- contains the engine-agnostic body that runs once a catalog is selected.

-- Make INSERT statements block until the batch job finishes, so the one-shot
-- container exits only after the gold table is actually populated.
SET 'table.dml-sync' = 'true';

-- Render the trailing SELECT as a table in non-interactive (one-shot) mode.
SET 'sql-client.execution.result-mode' = 'TABLEAU';

SHOW DATABASES;

CREATE DATABASE IF NOT EXISTS gold;

-- A Flink-authored gold table living in the same Iceberg catalog as Floe's
-- DuckDB-authored tables. Per-DA delivery throughput and on-time rate.
CREATE TABLE IF NOT EXISTS gold.flink_da_throughput (
    da_id        STRING,
    region       STRING,
    deliveries   BIGINT,
    on_time_rate DOUBLE
);

INSERT OVERWRITE gold.flink_da_throughput
SELECT
    da_id,
    region,
    COUNT(*)                                        AS deliveries,
    AVG(CASE WHEN on_time THEN 1.0 ELSE 0.0 END)    AS on_time_rate
FROM silver.deliveries_enriched
GROUP BY da_id, region;

SELECT * FROM gold.flink_da_throughput ORDER BY deliveries DESC LIMIT 20;
