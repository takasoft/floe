"""Pipeline orchestration — top-level Floe entry point."""

from __future__ import annotations

from pathlib import Path

from floe.catalog import CatalogManager
from floe.config import FloeConfig
from floe.executor import RefreshExecutor
from floe.models import DynamicTable, RefreshResult
from floe.parser import load_dits
from floe.planner import DAGPlanner


class Pipeline:
    """A Floe pipeline — config + DITs + planner + executor."""

    def __init__(
        self,
        config: FloeConfig,
        dits: list[DynamicTable],
        catalog_mgr: CatalogManager,
    ):
        self.config = config
        self.dits = {d.name: d for d in dits}
        self.planner = DAGPlanner(dits)
        self.catalog_mgr = catalog_mgr
        self.executor = RefreshExecutor(catalog_mgr, self.dits)

    @classmethod
    def from_config(cls, config_path: str | Path) -> Pipeline:
        config = FloeConfig.load(config_path)
        dits = load_dits(config.resolved_transformations_dir())
        catalog_mgr = CatalogManager.from_config(
            name=config.project,
            catalog_type=config.catalog.type,
            uri=config.resolved_catalog_uri(),
            warehouse=config.resolved_warehouse(),
        )
        return cls(config, dits, catalog_mgr)

    def refresh_all(self) -> list[RefreshResult]:
        """Refresh every DIT in topological order."""
        results: list[RefreshResult] = []
        for name in self.planner.topological_order():
            dit = self.dits[name]
            results.append(self.executor.refresh(dit))
        return results

    def refresh_one(self, name: str) -> RefreshResult:
        if name not in self.dits:
            raise KeyError(f"No DIT named {name!r}")
        return self.executor.refresh(self.dits[name])

    def status(self) -> list[dict]:
        rows = []
        for name in self.planner.topological_order():
            dit = self.dits[name]
            exists = self.catalog_mgr.table_exists(name)
            rows.append({
                "name": name,
                "exists": exists,
                "refresh_mode": dit.refresh_mode.value,
                "lag": dit.lag,
                "current_snapshot": self.catalog_mgr.current_snapshot_id(name) if exists else None,
                "checkpoint": self.catalog_mgr.get_checkpoint(name) if exists else None,
                "upstream": dit.upstream_tables,
            })
        return rows
