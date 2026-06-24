"""Floe project configuration loader."""

from __future__ import annotations

import os
from pathlib import Path

import yaml
from pydantic import BaseModel, ConfigDict, Field, field_validator

# Compute engines Floe can run a refresh on today. Flink is available as an
# experimental, opt-in service that operates on the same Iceberg tables (see the
# `flink` Docker Compose profile), but native Flink-based refresh is not yet
# wired into the executor, so it is intentionally not accepted here.
SUPPORTED_COMPUTE_ENGINES = frozenset({"duckdb"})


class CatalogConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    type: str = Field(default="sql")
    uri: str = Field(default="sqlite:///./.floe/catalog.db")
    warehouse: str = Field(default="./warehouse")
    properties: dict[str, str] = Field(
        default_factory=dict,
        description="Extra PyIceberg catalog/FileIO properties (e.g. s3.endpoint, "
        "s3.access-key-id). Merged verbatim into load_catalog().",
    )


class ComputeConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    engine: str = Field(default="duckdb")

    @field_validator("engine")
    @classmethod
    def _validate_engine(cls, v: str) -> str:
        engine = v.strip().lower()
        if engine not in SUPPORTED_COMPUTE_ENGINES:
            supported = ", ".join(sorted(SUPPORTED_COMPUTE_ENGINES))
            raise ValueError(
                f"unsupported compute engine {v!r}; Floe's refresh engine currently "
                f"supports: {supported}. Flink can run on the same Iceberg tables via the "
                "experimental Compose profile (`docker compose --profile flink up`); "
                "native Flink-based refresh is on the roadmap."
            )
        return engine


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
        cfg._apply_env_overrides()
        return cfg

    def _apply_env_overrides(self) -> None:
        """Override config from ``FLOE_*`` environment variables (12-factor).

        Lets a single image run unchanged across local/dev/Compose by injecting
        the catalog URI, warehouse location, and object-store credentials at
        runtime instead of baking them into the YAML. Supported keys:

        - ``FLOE_PROJECT``, ``FLOE_CATALOG_TYPE``, ``FLOE_CATALOG_URI``,
          ``FLOE_WAREHOUSE``, ``FLOE_TRANSFORMATIONS_DIR``
        - ``FLOE_S3_ENDPOINT`` / ``FLOE_S3_ACCESS_KEY_ID`` /
          ``FLOE_S3_SECRET_ACCESS_KEY`` / ``FLOE_S3_REGION``
        - ``FLOE_CATALOG_PROP__<key>`` — generic passthrough; ``__`` in the key
          becomes ``.`` (e.g. ``FLOE_CATALOG_PROP__S3__CONNECT__TIMEOUT``).
        """
        env = os.environ
        if v := env.get("FLOE_PROJECT"):
            self.project = v
        if v := env.get("FLOE_CATALOG_TYPE"):
            self.catalog.type = v
        if v := env.get("FLOE_CATALOG_URI"):
            self.catalog.uri = v
        if v := env.get("FLOE_WAREHOUSE"):
            self.catalog.warehouse = v
        if v := env.get("FLOE_TRANSFORMATIONS_DIR"):
            self.transformations_dir = v

        s3_map = {
            "FLOE_S3_ENDPOINT": "s3.endpoint",
            "FLOE_S3_ACCESS_KEY_ID": "s3.access-key-id",
            "FLOE_S3_SECRET_ACCESS_KEY": "s3.secret-access-key",
            "FLOE_S3_REGION": "s3.region",
        }
        for env_key, prop_key in s3_map.items():
            if v := env.get(env_key):
                self.catalog.properties[prop_key] = v

        prefix = "FLOE_CATALOG_PROP__"
        for key, value in env.items():
            if key.startswith(prefix):
                prop = key[len(prefix) :].lower().replace("__", ".")
                self.catalog.properties[prop] = value

    def resolved_warehouse(self) -> str:
        """Return the warehouse location as a string the catalog can consume.

        A configured URI (``s3://``, ``file://``, ``gs://`` …) is passed through
        untouched. A bare path is resolved relative to the project root and
        created on disk (local filesystem warehouse).
        """
        wh = self.catalog.warehouse
        if "://" in wh:
            return wh
        path = Path(wh)
        if not path.is_absolute():
            path = self.project_root / path
        path.mkdir(parents=True, exist_ok=True)
        return str(path)

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
