"""Automatic lineage column injection."""

from __future__ import annotations

import uuid
from datetime import datetime, timezone

import pyarrow as pa

INPUT_SNAPSHOT_COL = "_floe_input_snapshot_id"
JOB_RUN_ID_COL = "_floe_job_run_id"
PROCESSED_AT_COL = "_floe_processed_at"
REFRESH_MODE_COL = "_floe_refresh_mode"

LINEAGE_COLUMNS = (INPUT_SNAPSHOT_COL, JOB_RUN_ID_COL, PROCESSED_AT_COL, REFRESH_MODE_COL)


def new_job_run_id() -> str:
    return f"run_{uuid.uuid4().hex[:16]}"


def inject_lineage(
    data: pa.Table,
    *,
    input_snapshot_id: int | None,
    job_run_id: str,
    refresh_mode: str,
    processed_at: datetime | None = None,
) -> pa.Table:
    """Append the four lineage columns to a PyArrow table."""
    n_rows = data.num_rows
    ts = processed_at or datetime.now(timezone.utc)

    # Strip existing lineage columns so re-runs don't accumulate
    existing = [c for c in data.column_names if c in LINEAGE_COLUMNS]
    if existing:
        data = data.drop_columns(existing)

    snapshot_array = pa.array(
        [input_snapshot_id] * n_rows,
        type=pa.int64(),
    )
    job_run_array = pa.array([job_run_id] * n_rows, type=pa.string())
    ts_array = pa.array(
        [ts] * n_rows,
        type=pa.timestamp("us", tz="UTC"),
    )
    mode_array = pa.array([refresh_mode] * n_rows, type=pa.string())

    return (
        data
        .append_column(INPUT_SNAPSHOT_COL, snapshot_array)
        .append_column(JOB_RUN_ID_COL, job_run_array)
        .append_column(PROCESSED_AT_COL, ts_array)
        .append_column(REFRESH_MODE_COL, mode_array)
    )
