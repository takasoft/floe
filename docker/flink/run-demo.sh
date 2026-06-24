#!/usr/bin/env bash
#
# Entry point for the one-shot `flink-sql` demo service. It waits for the Flink
# JobManager, builds a Flink SQL script that registers Floe's Iceberg catalog
# (injecting the object-store credentials from the environment), and runs the
# engine-agnostic demo in demo.sql against it.
set -euo pipefail

JM_HOST="${FLINK_JOBMANAGER_HOST:-flink-jobmanager}"
JM_PORT="${FLINK_JOBMANAGER_PORT:-8081}"
CATALOG_NAME="${FLOE_CATALOG_NAME:-quickstart}"
S3_REGION="${FLOE_S3_REGION:-us-east-1}"

echo "Waiting for Flink JobManager at ${JM_HOST}:${JM_PORT} ..."
for _ in $(seq 1 60); do
    if curl -sf "http://${JM_HOST}:${JM_PORT}/overview" >/dev/null 2>&1; then
        echo "JobManager is up."
        break
    fi
    sleep 2
done

SQL_FILE="$(mktemp)"

# The client must submit jobs to the remote session cluster, not run locally.
cat > "${SQL_FILE}" <<SQL
SET 'execution.target' = 'remote';
SET 'rest.address' = '${JM_HOST}';
SET 'rest.port' = '${JM_PORT}';
SET 'execution.runtime-mode' = 'batch';

-- Register Floe's catalog: same Postgres JDBC backend + same S3/MinIO warehouse
-- that PyIceberg writes to. The catalog name must match Floe's project name so
-- Flink's JdbcCatalog resolves the same rows.
CREATE CATALOG ${CATALOG_NAME} WITH (
    'type' = 'iceberg',
    'catalog-impl' = 'org.apache.iceberg.jdbc.JdbcCatalog',
    'uri' = '${FLINK_JDBC_URI}',
    'jdbc.user' = '${POSTGRES_USER}',
    'jdbc.password' = '${POSTGRES_PASSWORD}',
    'warehouse' = '${FLOE_WAREHOUSE}',
    'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO',
    's3.endpoint' = '${FLOE_S3_ENDPOINT}',
    's3.path-style-access' = 'true',
    's3.access-key-id' = '${FLOE_S3_ACCESS_KEY_ID}',
    's3.secret-access-key' = '${FLOE_S3_SECRET_ACCESS_KEY}',
    'client.region' = '${S3_REGION}'
);

USE CATALOG ${CATALOG_NAME};
SQL

# Append the engine-agnostic demo body.
cat /opt/floe/demo.sql >> "${SQL_FILE}"

echo "Running Flink SQL demo against Iceberg catalog '${CATALOG_NAME}' ..."
echo "---------------------------------------------------------------"
exec /opt/flink/bin/sql-client.sh -f "${SQL_FILE}"
