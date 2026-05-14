"""Floe project configuration loader."""

from __future__ import annotations

from pathlib import Path

import yaml
from pydantic import BaseModel, ConfigDict, Field


class CatalogConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    type: str = Field(default="sql")
    uri: str = Field(default="sqlite:///./.floe/catalog.db")
    warehouse: str = Field(default="./warehouse")


class ComputeConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    engine: str = Field(default="duckdb")


class DefaultsConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    lag: str = Field(default="5 minutes")
    refresh_mode: str = Field(default="INCREMENTAL")
    lineage: bool = Field(default=True)


class FloeConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    project: str
    version: str = Field(default="1.0")
    catalog: CatalogConfig = Field(default_factory=CatalogConfig)
    compute: ComputeConfig = Field(default_factory=ComputeConfig)
    defaults: DefaultsConfig = Field(default_factory=DefaultsConfig)
    transformations_dir: str = Field(default="transformations")

    project_root: Path = Field(default_factory=Path.cwd, exclude=True)

    @classmethod
    def load(cls, path: str | Path) -> FloeConfig:
        path = Path(path).resolve()
        with open(path) as f:
            data = yaml.safe_load(f) or {}
        cfg = cls.model_validate(data)
        cfg.project_root = path.parent
        return cfg

    def resolved_warehouse(self) -> Path:
        wh = Path(self.catalog.warehouse)
        if not wh.is_absolute():
            wh = self.project_root / wh
        wh.mkdir(parents=True, exist_ok=True)
        return wh

    def resolved_catalog_uri(self) -> str:
        uri = self.catalog.uri
        if uri.startswith("sqlite:///") and not uri.startswith("sqlite:////"):
            relative = uri[len("sqlite:///") :]
            abs_path = (self.project_root / relative).resolve()
            abs_path.parent.mkdir(parents=True, exist_ok=True)
            return f"sqlite:///{abs_path.as_posix()}"
        return uri

    def resolved_transformations_dir(self) -> Path:
        d = Path(self.transformations_dir)
        if not d.is_absolute():
            d = self.project_root / d
        return d
