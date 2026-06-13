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
    # Setting `engine: flink` must fail loudly rather than silently fall back to
    # DuckDB, so users are not misled into thinking refreshes run on Flink.
    data = {**_BASE_CONFIG, "compute": {"engine": "flink"}}
    with pytest.raises(ValidationError, match="unsupported compute engine"):
        FloeConfig.load(_write_config(tmp_path, data))
