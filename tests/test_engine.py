"""Tests for engine selection and the Flink executor's SQL generation.

These are all offline: they exercise the factory dispatch and the SQL the Flink
executor builds, without needing a running Flink cluster.
"""

from __future__ import annotations

import pytest

from floe.config import CatalogConfig, ComputeConfig, FloeConfig
from floe.engine import Executor, make_executor
from floe.executor import DuckDBExecutor
from floe.flink_executor import FlinkExecutor, StreamingNotSupportedError
from floe.lineage import LINEAGE_COLUMNS
from floe.models import DynamicTable


def _flink_config() -> FloeConfig:
    return FloeConfig(
        project="quickstart",
        catalog=CatalogConfig(
            type="sql",
            uri="postgresql+psycopg2://floe:secret@postgres:5432/floe",
            warehouse="s3://floe-warehouse/wh",
            properties={
                "s3.endpoint": "http://minio:9000",
                "s3.access-key-id": "a",
                "s3.secret-access-key": "b",
            },
        ),
        compute=ComputeConfig(engine="flink"),
    )


def test_make_executor_returns_duckdb(floe_config, catalog_mgr):
    ex = make_executor(floe_config, catalog_mgr, {})
    assert isinstance(ex, DuckDBExecutor)
    assert isinstance(ex, Executor)


def test_make_executor_returns_flink(catalog_mgr):
    ex = make_executor(_flink_config(), catalog_mgr, {})
    assert isinstance(ex, FlinkExecutor)
    assert isinstance(ex, Executor)


def test_flink_executor_rejects_partitioned_dit(catalog_mgr):
    ex = FlinkExecutor(_flink_config(), catalog_mgr, {})
    dit = DynamicTable(name="gold.daily", query="SELECT 1 AS x", partition_by=["event_date"])
    with pytest.raises(NotImplementedError, match="partitioned"):
        ex.refresh(dit)


def test_flink_build_select_appends_lineage_columns(catalog_mgr):
    ex = FlinkExecutor(_flink_config(), catalog_mgr, {})
    dit = DynamicTable(name="gold.r", query="SELECT a, b FROM silver.x", refresh_mode="FULL")
    sql = ex._build_select(dit, upstream_snapshot_id=123, job_run_id="run_abc")
    for col in LINEAGE_COLUMNS:
        assert col in sql
    assert "CAST(123 AS BIGINT)" in sql
    assert "run_abc" in sql
    assert "FULL" in sql
    assert "FROM (" in sql  # the user query is wrapped


def test_flink_build_select_null_snapshot(catalog_mgr):
    ex = FlinkExecutor(_flink_config(), catalog_mgr, {})
    dit = DynamicTable(name="gold.r", query="SELECT 1 AS x")
    sql = ex._build_select(dit, upstream_snapshot_id=None, job_run_id="run_x")
    assert "CAST(NULL AS BIGINT)" in sql


def test_flink_create_catalog_statement(catalog_mgr):
    ex = FlinkExecutor(_flink_config(), catalog_mgr, {})
    stmt = ex._create_catalog_statement("quickstart")
    assert "CREATE CATALOG `quickstart`" in stmt
    assert "'catalog-impl' = 'org.apache.iceberg.jdbc.JdbcCatalog'" in stmt
    assert "'uri' = 'jdbc:postgresql://postgres:5432/floe'" in stmt
    assert "'warehouse' = 's3://floe-warehouse/wh'" in stmt
    assert "'s3.endpoint' = 'http://minio:9000'" in stmt


def test_streaming_hint_injected_on_upstream(catalog_mgr):
    ex = FlinkExecutor(_flink_config(), catalog_mgr, {})
    dit = DynamicTable(
        name="gold.passthrough",
        query="SELECT delivery_id, on_time FROM bronze.raw_deliveries",
        upstream_tables=["bronze.raw_deliveries"],
    )
    select_sql = ex._build_select(dit, upstream_snapshot_id=None, job_run_id="run_x")
    streamed = ex._inject_streaming_hint(select_sql, "bronze.raw_deliveries")
    assert "bronze.raw_deliveries /*+ OPTIONS(" in streamed
    assert "'streaming'='true'" in streamed
    assert "'starting-strategy'='INCREMENTAL_FROM_LATEST_SNAPSHOT'" in streamed
    # exactly one hint (only the first/only upstream reference is rewritten)
    assert streamed.count("OPTIONS(") == 1


def test_assert_streamable_rejects_aggregation(catalog_mgr):
    ex = FlinkExecutor(_flink_config(), catalog_mgr, {})
    dit = DynamicTable(
        name="gold.by_region",
        query="SELECT region, COUNT(*) AS n FROM silver.x GROUP BY region",
        upstream_tables=["silver.x"],
    )
    with pytest.raises(StreamingNotSupportedError, match="append-only"):
        ex._assert_streamable(dit)


def test_assert_streamable_rejects_multi_source(catalog_mgr):
    ex = FlinkExecutor(_flink_config(), catalog_mgr, {})
    dit = DynamicTable(
        name="silver.joined",
        query="SELECT a.id FROM bronze.a a JOIN bronze.b b ON a.id = b.id",
        upstream_tables=["bronze.a", "bronze.b"],
    )
    with pytest.raises(StreamingNotSupportedError, match="single upstream"):
        ex._assert_streamable(dit)


def test_assert_streamable_accepts_append_only(catalog_mgr):
    ex = FlinkExecutor(_flink_config(), catalog_mgr, {})
    dit = DynamicTable(
        name="gold.passthrough",
        query="SELECT delivery_id, on_time FROM bronze.raw_deliveries",
        upstream_tables=["bronze.raw_deliveries"],
    )
    ex._assert_streamable(dit)  # should not raise


def test_extract_job_id_picks_hex_handle():
    assert FlinkExecutor._extract_job_id([["a" * 32]]) == "a" * 32
    assert FlinkExecutor._extract_job_id([["not-a-job-id"]]) == "not-a-job-id"
    assert FlinkExecutor._extract_job_id([]) is None
