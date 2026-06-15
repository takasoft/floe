"""Floe project configuration loader."""

from __future__ import annotations

import os
from pathlib import Path

import yaml
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

# Compute engines Floe can run a refresh on.
#
# - ``duckdb`` : the default. In-process batch recompute-and-replace (see
#   :class:`floe.executor.DuckDBExecutor`).
# - ``flink``  : submits the DIT's SQL to an Apache Flink cluster via its SQL
#   Gateway, computing the table on Flink and writing it back to the SAME Iceberg
#   catalog (see :class:`floe.flink_executor.FlinkExecutor`). Opt-in; needs a
#   reachable Flink SQL Gateway (the `flink` Docker Compose profile starts one).
SUPPORTED_COMPUTE_ENGINES = frozenset({"duckdb", "flink"})

# How refreshes are triggered (orthogonal to the compute engine):
#
# - ``poll`` : pull-based. The watcher polls the Iceberg catalog for new upstream
#   snapshots and runs a refresh when one appears. Works with either engine.
# - ``push`` : event-driven. A long-running Flink streaming job reacts to upstream
#   commits and continuously updates the output (no polling). Requires the Flink
#   engine. This is the roadmap's streaming compute; see the README.
SUPPORTED_TRIGGERS = frozenset({"poll", "push"})


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


class FlinkConfig(BaseModel):
    """Settings for the ``flink`` compute engine.

    Connection details that Flink shares with the rest of Floe (warehouse,
    object-store credentials, the JDBC catalog) are derived from the top-level
    ``catalog`` config, so this block only carries Flink-specific knobs plus
    optional overrides.
    """

    model_config = ConfigDict(extra="forbid")

    sql_gateway_url: str = Field(
        default="http://localhost:8083",
        description="Base URL of the Flink SQL Gateway REST endpoint.",
    )
    jobmanager_host: str = Field(
        default="flink-jobmanager",
        description="Host of the Flink cluster REST endpoint the gateway submits jobs to.",
    )
    jobmanager_port: int = Field(default=8081, ge=1)
    catalog_name: str | None = Field(
        default=None,
        description="Flink catalog name to register. Must match the JdbcCatalog "
        "rows PyIceberg wrote; defaults to the Floe project name.",
    )
    jdbc_uri: str | None = Field(
        default=None,
        description="Plain Java JDBC URI for the catalog backend (e.g. "
        "'jdbc:postgresql://postgres:5432/floe'). Derived from catalog.uri when omitted.",
    )
    parallelism: int = Field(default=1, ge=1)
    statement_timeout_seconds: int = Field(default=300, ge=1)
    # --- streaming / push (roadmap) ---
    streaming: bool = Field(
        default=False,
        description="Run the transformation as a continuous streaming job rather "
        "than a one-shot batch job. Used by the 'push' trigger.",
    )
    monitor_interval: str = Field(
        default="10s",
        description="How often a streaming Iceberg source checks for new snapshots.",
    )


class ComputeConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    engine: str = Field(default="duckdb")
    trigger: str = Field(default="poll")
    flink: FlinkConfig | None = Field(default=None)

    @field_validator("engine")
    @classmethod
    def _validate_engine(cls, v: str) -> str:
        engine = v.strip().lower()
        if engine not in SUPPORTED_COMPUTE_ENGINES:
            supported = ", ".join(sorted(SUPPORTED_COMPUTE_ENGINES))
            raise ValueError(
                f"unsupported compute engine {v!r}; supported engines: {supported}. "
                "The 'flink' engine needs a reachable Flink SQL Gateway "
                "(`docker compose --profile flink up`)."
            )
        return engine

    @field_validator("trigger")
    @classmethod
    def _validate_trigger(cls, v: str) -> str:
        trigger = v.strip().lower()
        if trigger not in SUPPORTED_TRIGGERS:
            supported = ", ".join(sorted(SUPPORTED_TRIGGERS))
            raise ValueError(f"unsupported refresh trigger {v!r}; supported: {supported}")
        return trigger

    @model_validator(mode="after")
    def _validate_combination(self) -> ComputeConfig:
        # The push (event-driven streaming) trigger is implemented on Flink only;
        # DuckDB is a batch engine and pairs with poll.
        if self.trigger == "push" and self.engine != "flink":
            raise ValueError(
                "trigger 'push' (event-driven streaming) requires the 'flink' compute "
                "engine; the 'duckdb' engine is batch and uses trigger 'poll'."
            )
        # Always give the Flink engine a config object so downstream code can rely
        # on it being present.
        if self.engine == "flink" and self.flink is None:
            self.flink = FlinkConfig()
        return self


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
        - ``FLOE_COMPUTE_ENGINE`` / ``FLOE_COMPUTE_TRIGGER`` — select the engine
          (``duckdb`` | ``flink``) and trigger (``poll`` | ``push``).
        - ``FLOE_FLINK_SQL_GATEWAY_URL`` / ``FLOE_FLINK_JDBC_URI`` /
          ``FLOE_FLINK_CATALOG_NAME`` — Flink engine overrides.
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

        self._apply_compute_env_overrides(env)

    def _apply_compute_env_overrides(self, env) -> None:
        """Apply ``FLOE_COMPUTE_*`` / ``FLOE_FLINK_*`` overrides, re-validating.

        Engine/trigger flow through ``ComputeConfig`` validation again so the
        cross-field rules (e.g. push requires Flink) still hold when set via env.
        """
        engine = env.get("FLOE_COMPUTE_ENGINE")
        trigger = env.get("FLOE_COMPUTE_TRIGGER")
        if engine or trigger:
            self.compute = ComputeConfig.model_validate(
                {
                    "engine": engine or self.compute.engine,
                    "trigger": trigger or self.compute.trigger,
                    "flink": self.compute.flink.model_dump() if self.compute.flink else None,
                }
            )

        flink_overrides = {
            "sql_gateway_url": env.get("FLOE_FLINK_SQL_GATEWAY_URL"),
            "jdbc_uri": env.get("FLOE_FLINK_JDBC_URI"),
            "catalog_name": env.get("FLOE_FLINK_CATALOG_NAME"),
        }
        if any(v is not None for v in flink_overrides.values()):
            base = self.compute.flink or FlinkConfig()
            data = base.model_dump()
            for k, v in flink_overrides.items():
                if v is not None:
                    data[k] = v
            self.compute.flink = FlinkConfig.model_validate(data)

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

    # --- Flink engine helpers ------------------------------------------------

    def flink_catalog_name(self) -> str:
        """Flink catalog name (defaults to the project name).

        Must match the JdbcCatalog rows PyIceberg wrote so Flink resolves the
        very same tables.
        """
        flink = self.compute.flink
        if flink and flink.catalog_name:
            return flink.catalog_name
        return self.project

    def flink_jdbc(self) -> tuple[str, str | None, str | None]:
        """Return ``(jdbc_uri, user, password)`` for Flink's JdbcCatalog.

        Honours an explicit ``compute.flink.jdbc_uri`` override; otherwise derives
        the plain Java JDBC URI from the SQLAlchemy-style ``catalog.uri`` (only the
        Postgres dialect Floe ships is supported for the Flink engine).
        """
        from urllib.parse import urlparse

        flink = self.compute.flink
        parsed = urlparse(self.catalog.uri)
        user = parsed.username
        password = parsed.password
        if flink and flink.jdbc_uri:
            return flink.jdbc_uri, user, password

        scheme = parsed.scheme.split("+", 1)[0]
        if scheme not in {"postgresql", "postgres"}:
            raise ValueError(
                f"the flink engine needs a Postgres JDBC catalog; catalog.uri uses "
                f"{parsed.scheme!r}. Set compute.flink.jdbc_uri explicitly, or use a "
                "Postgres catalog (the Docker Compose stack does)."
            )
        host = parsed.hostname or "localhost"
        port = parsed.port or 5432
        db = parsed.path.lstrip("/")
        return f"jdbc:postgresql://{host}:{port}/{db}", user, password

    def flink_catalog_properties(self) -> dict[str, str]:
        """Assemble the ``CREATE CATALOG ... WITH (...)`` properties for Flink.

        Reuses the same warehouse and object-store credentials the rest of Floe
        uses (from ``catalog.warehouse`` / ``catalog.properties``), so Flink reads
        and writes the identical Iceberg catalog.
        """
        jdbc_uri, user, password = self.flink_jdbc()
        props = self.catalog.properties
        out: dict[str, str] = {
            "type": "iceberg",
            "catalog-impl": "org.apache.iceberg.jdbc.JdbcCatalog",
            "uri": jdbc_uri,
            "warehouse": self.resolved_warehouse(),
            "io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
            "s3.path-style-access": "true",
        }
        if user is not None:
            out["jdbc.user"] = user
        if password is not None:
            out["jdbc.password"] = password
        if endpoint := props.get("s3.endpoint"):
            out["s3.endpoint"] = endpoint
        if access := props.get("s3.access-key-id"):
            out["s3.access-key-id"] = access
        if secret := props.get("s3.secret-access-key"):
            out["s3.secret-access-key"] = secret
        out["client.region"] = props.get("s3.region", "us-east-1")
        return out
