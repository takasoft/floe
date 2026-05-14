"""Shared pytest fixtures."""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pytest

from floe.catalog import CatalogManager
from floe.config import CatalogConfig, ComputeConfig, DefaultsConfig, FloeConfig


@pytest.fixture
def project_dir(tmp_path: Path) -> Path:
    (tmp_path / "transformations").mkdir()
    (tmp_path / "warehouse").mkdir()
    (tmp_path / ".floe").mkdir()
    return tmp_path


@pytest.fixture
def floe_config(project_dir: Path) -> FloeConfig:
    cfg = FloeConfig(
        project="test_project",
        version="1.0",
        catalog=CatalogConfig(
            type="sql",
            uri=f"sqlite:///{(project_dir / '.floe' / 'catalog.db').as_posix()}",
            warehouse=str(project_dir / "warehouse"),
        ),
        compute=ComputeConfig(),
        defaults=DefaultsConfig(),
        transformations_dir=str(project_dir / "transformations"),
    )
    cfg.project_root = project_dir
    return cfg


@pytest.fixture
def catalog_mgr(floe_config: FloeConfig) -> CatalogManager:
    return CatalogManager.from_config(
        name=floe_config.project,
        catalog_type=floe_config.catalog.type,
        uri=floe_config.resolved_catalog_uri(),
        warehouse=floe_config.resolved_warehouse(),
    )


@pytest.fixture
def sample_orders() -> pa.Table:
    return pa.table(
        {
            "order_id": pa.array([1, 2, 3], type=pa.int64()),
            "customer_id": pa.array([100, 101, 100], type=pa.int64()),
            "amount": pa.array([10.5, 25.0, 7.25], type=pa.float64()),
        }
    )


@pytest.fixture
def sample_customers() -> pa.Table:
    return pa.table(
        {
            "id": pa.array([100, 101], type=pa.int64()),
            "region": pa.array(["us-east", "us-west"], type=pa.string()),
        }
    )
