"""Core domain models for Floe."""

from __future__ import annotations

from datetime import datetime, timedelta
from enum import Enum

from pydantic import BaseModel, ConfigDict, Field, field_validator


class RefreshMode(str, Enum):
    INCREMENTAL = "INCREMENTAL"
    FULL = "FULL"


class DynamicTable(BaseModel):
    """A Dynamic Iceberg Table — the central declarative abstraction."""

    model_config = ConfigDict(frozen=False, extra="forbid")

    name: str = Field(..., description="Fully-qualified table name (e.g., 'silver.orders')")
    query: str = Field(..., description="SELECT query producing the table contents")
    lag: str = Field(default="5 minutes", description="Target freshness (e.g., '5 minutes')")
    refresh_mode: RefreshMode = Field(default=RefreshMode.INCREMENTAL)
    upstream_tables: list[str] = Field(default_factory=list)
    source_path: str | None = Field(default=None, description="Path to the .sql file defining this DIT")

    # Partition-aware freshness (optional)
    partition_by: list[str] = Field(
        default_factory=list,
        description="Identity partition columns (single col supported in MVP)",
    )
    partition_freshness: str | None = Field(
        default=None,
        description="Max age of in-scope partitions (e.g., '2 days'). None = inherit table-level lag.",
    )
    partition_window: str | None = Field(
        default=None,
        description="How far back partitions are kept fresh (e.g., '14 days'). None = whole table.",
    )

    @field_validator("partition_by")
    @classmethod
    def _single_partition_col(cls, v: list[str]) -> list[str]:
        if len(v) > 1:
            raise ValueError(
                "MVP supports a single identity partition column; "
                f"got {len(v)}: {v!r}"
            )
        return v

    @property
    def is_partitioned(self) -> bool:
        return len(self.partition_by) > 0

    def partition_freshness_seconds(self) -> int | None:
        return _parse_lag(self.partition_freshness) if self.partition_freshness else None

    def partition_window_seconds(self) -> int | None:
        return _parse_lag(self.partition_window) if self.partition_window else None

    @property
    def namespace(self) -> str:
        if "." not in self.name:
            return "default"
        return self.name.rsplit(".", 1)[0]

    @property
    def table_name(self) -> str:
        if "." not in self.name:
            return self.name
        return self.name.rsplit(".", 1)[1]

    def lag_seconds(self) -> int:
        return _parse_lag(self.lag)


class RefreshResult(BaseModel):
    """Result of a single DIT refresh operation."""

    model_config = ConfigDict(frozen=False)

    table: str
    mode: RefreshMode
    rows_written: int
    input_snapshot_id: int | None
    output_snapshot_id: int | None
    job_run_id: str
    started_at: datetime
    finished_at: datetime
    skipped: bool = False
    skip_reason: str | None = None

    @property
    def duration_ms(self) -> int:
        return int((self.finished_at - self.started_at).total_seconds() * 1000)


def _parse_lag(lag: str) -> int:
    """Parse a lag string like '5 minutes' or '1 hour' into seconds."""
    parts = lag.strip().lower().split()
    if len(parts) != 2:
        raise ValueError(f"Invalid lag format: {lag!r} (expected '<n> <unit>')")

    try:
        value = int(parts[0])
    except ValueError as e:
        raise ValueError(f"Invalid lag value: {parts[0]!r}") from e

    unit = parts[1].rstrip("s")
    units = {
        "second": 1,
        "minute": 60,
        "hour": 3600,
        "day": 86400,
    }
    if unit not in units:
        raise ValueError(f"Unknown lag unit: {parts[1]!r}")

    return value * units[unit]


def lag_to_timedelta(lag: str) -> timedelta:
    return timedelta(seconds=_parse_lag(lag))
