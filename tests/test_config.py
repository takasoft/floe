"""Tests for config loading, env overrides, and warehouse resolution."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml
from pydantic import ValidationError

from floe.config import FloeConfig

_BASE_CONFIG = {
    "project": "demo",
    "version": "1.0",
    "catalog": {
        "type": "sql",
        "uri": "sqlite:///./.floe/catalog.db",
        "warehouse": "./warehouse",
    },
    "transformations_dir": "transformations",
}


def _write_config(tmp_path: Path, data: dict) -> Path:
    path = tmp_path / "floe.yaml"
    path.write_text(yaml.safe_dump(data), encoding="utf-8")
    return path


def test_local_warehouse_is_resolved_and_created(tmp_path):
    cfg = FloeConfig.load(_write_config(tmp_path, _BASE_CONFIG))
    wh = cfg.resolved_warehouse()
    assert Path(wh).is_absolute()
    assert Path(wh).is_dir()  # created on disk
    assert "://" not in wh


def test_uri_warehouse_is_passed_through_untouched(tmp_path):
    data = {**_BASE_CONFIG, "catalog": {**_BASE_CONFIG["catalog"], "warehouse": "s3://bucket/wh"}}
    cfg = FloeConfig.load(_write_config(tmp_path, data))
    # A URI warehouse must not be turned into a local path or created on disk.
    assert cfg.resolved_warehouse() == "s3://bucket/wh"


def test_inline_properties_are_loaded(tmp_path):
    data = {
        **_BASE_CONFIG,
        "catalog": {
            **_BASE_CONFIG["catalog"],
            "properties": {"s3.endpoint": "http://minio:9000"},
        },
    }
    cfg = FloeConfig.load(_write_config(tmp_path, data))
    assert cfg.catalog.properties["s3.endpoint"] == "http://minio:9000"


def test_env_overrides_catalog_and_s3_properties(tmp_path, monkeypatch):
    monkeypatch.setenv("FLOE_CATALOG_URI", "postgresql+psycopg2://u:p@db:5432/floe")
    monkeypatch.setenv("FLOE_WAREHOUSE", "s3://floe-warehouse/wh")
    monkeypatch.setenv("FLOE_S3_ENDPOINT", "http://minio:9000")
    monkeypatch.setenv("FLOE_S3_ACCESS_KEY_ID", "minioadmin")
    monkeypatch.setenv("FLOE_S3_SECRET_ACCESS_KEY", "miniosecret")
    monkeypatch.setenv("FLOE_S3_REGION", "us-east-1")

    cfg = FloeConfig.load(_write_config(tmp_path, _BASE_CONFIG))

    assert cfg.catalog.uri == "postgresql+psycopg2://u:p@db:5432/floe"
    assert cfg.resolved_warehouse() == "s3://floe-warehouse/wh"
    assert cfg.catalog.properties == {
        "s3.endpoint": "http://minio:9000",
        "s3.access-key-id": "minioadmin",
        "s3.secret-access-key": "miniosecret",
        "s3.region": "us-east-1",
    }


def test_generic_property_passthrough_env(tmp_path, monkeypatch):
    monkeypatch.setenv("FLOE_CATALOG_PROP__S3__CONNECT__TIMEOUT", "60")
    cfg = FloeConfig.load(_write_config(tmp_path, _BASE_CONFIG))
    assert cfg.catalog.properties["s3.connect.timeout"] == "60"


def test_postgres_uri_not_rewritten(tmp_path):
    data = {
        **_BASE_CONFIG,
        "catalog": {**_BASE_CONFIG["catalog"], "uri": "postgresql+psycopg2://u:p@db:5432/floe"},
    }
    cfg = FloeConfig.load(_write_config(tmp_path, data))
    # resolved_catalog_uri only rewrites relative sqlite URIs; Postgres is untouched.
    assert cfg.resolved_catalog_uri() == "postgresql+psycopg2://u:p@db:5432/floe"


def test_default_compute_engine_is_duckdb(tmp_path):
    cfg = FloeConfig.load(_write_config(tmp_path, _BASE_CONFIG))
    assert cfg.compute.engine == "duckdb"


def test_compute_engine_is_normalized_case_insensitively(tmp_path):
    data = {**_BASE_CONFIG, "compute": {"engine": "DuckDB"}}
    cfg = FloeConfig.load(_write_config(tmp_path, data))
    assert cfg.compute.engine == "duckdb"


def test_unsupported_compute_engine_is_rejected(tmp_path):
    # An engine Floe has no executor for must fail loudly rather than silently
    # falling back to DuckDB, so users are not misled about where refreshes run.
    data = {**_BASE_CONFIG, "compute": {"engine": "spark"}}
    with pytest.raises(ValidationError, match="unsupported compute engine"):
        FloeConfig.load(_write_config(tmp_path, data))


def test_flink_engine_is_accepted(tmp_path):
    data = {**_BASE_CONFIG, "compute": {"engine": "flink"}}
    cfg = FloeConfig.load(_write_config(tmp_path, data))
    assert cfg.compute.engine == "flink"
    # The engine always gets a usable Flink config block.
    assert cfg.compute.flink is not None
    assert cfg.compute.flink.sql_gateway_url.startswith("http")


def test_default_trigger_is_poll(tmp_path):
    cfg = FloeConfig.load(_write_config(tmp_path, _BASE_CONFIG))
    assert cfg.compute.trigger == "poll"


def test_push_trigger_requires_flink_engine(tmp_path):
    # push is event-driven streaming; it is implemented on Flink only.
    data = {**_BASE_CONFIG, "compute": {"engine": "duckdb", "trigger": "push"}}
    with pytest.raises(ValidationError, match="requires the 'flink' compute engine"):
        FloeConfig.load(_write_config(tmp_path, data))


def test_push_trigger_with_flink_is_accepted(tmp_path):
    data = {**_BASE_CONFIG, "compute": {"engine": "flink", "trigger": "push"}}
    cfg = FloeConfig.load(_write_config(tmp_path, data))
    assert cfg.compute.engine == "flink"
    assert cfg.compute.trigger == "push"


def test_unsupported_trigger_is_rejected(tmp_path):
    data = {**_BASE_CONFIG, "compute": {"engine": "duckdb", "trigger": "webhook"}}
    with pytest.raises(ValidationError, match="unsupported refresh trigger"):
        FloeConfig.load(_write_config(tmp_path, data))


def test_compute_env_overrides_select_engine_and_trigger(tmp_path, monkeypatch):
    monkeypatch.setenv("FLOE_COMPUTE_ENGINE", "flink")
    monkeypatch.setenv("FLOE_COMPUTE_TRIGGER", "push")
    monkeypatch.setenv("FLOE_FLINK_SQL_GATEWAY_URL", "http://flink-sql-gateway:8083")
    cfg = FloeConfig.load(_write_config(tmp_path, _BASE_CONFIG))
    assert cfg.compute.engine == "flink"
    assert cfg.compute.trigger == "push"
    assert cfg.compute.flink.sql_gateway_url == "http://flink-sql-gateway:8083"


def test_flink_jdbc_uri_derived_from_postgres_catalog(tmp_path):
    data = {
        **_BASE_CONFIG,
        "project": "quickstart",
        "compute": {"engine": "flink"},
        "catalog": {
            "type": "sql",
            "uri": "postgresql+psycopg2://floe:secret@postgres:5432/floe",
            "warehouse": "s3://floe-warehouse/wh",
        },
    }
    cfg = FloeConfig.load(_write_config(tmp_path, data))
    jdbc_uri, user, password = cfg.flink_jdbc()
    assert jdbc_uri == "jdbc:postgresql://postgres:5432/floe"
    assert user == "floe"
    assert password == "secret"
    assert cfg.flink_catalog_name() == "quickstart"
    props = cfg.flink_catalog_properties()
    assert props["type"] == "iceberg"
    assert props["catalog-impl"] == "org.apache.iceberg.jdbc.JdbcCatalog"
    assert props["warehouse"] == "s3://floe-warehouse/wh"


def test_flink_engine_rejects_non_postgres_catalog_for_jdbc(tmp_path):
    # A local sqlite catalog cannot back Flink's JdbcCatalog; surface a clear error.
    data = {**_BASE_CONFIG, "compute": {"engine": "flink"}}
    cfg = FloeConfig.load(_write_config(tmp_path, data))
    with pytest.raises(ValueError, match="Postgres JDBC catalog"):
        cfg.flink_jdbc()
