"""Tests for lineage column injection."""

import pyarrow as pa

from floe.lineage import (
    INPUT_SNAPSHOT_COL,
    JOB_RUN_ID_COL,
    LINEAGE_COLUMNS,
    PROCESSED_AT_COL,
    REFRESH_MODE_COL,
    inject_lineage,
    new_job_run_id,
)


def test_lineage_columns_added():
    data = pa.table({"order_id": [1, 2], "amount": [10.0, 20.0]})
    result = inject_lineage(
        data,
        input_snapshot_id=12345,
        job_run_id="run_test",
        refresh_mode="INCREMENTAL",
    )
    assert all(col in result.column_names for col in LINEAGE_COLUMNS)
    assert result[INPUT_SNAPSHOT_COL].to_pylist() == [12345, 12345]
    assert result[JOB_RUN_ID_COL].to_pylist() == ["run_test", "run_test"]
    assert result[REFRESH_MODE_COL].to_pylist() == ["INCREMENTAL", "INCREMENTAL"]
    assert result[PROCESSED_AT_COL].null_count == 0


def test_lineage_replaces_existing_columns():
    """Re-injection should not duplicate columns."""
    data = pa.table({"x": [1, 2]})
    once = inject_lineage(data, input_snapshot_id=1, job_run_id="a", refresh_mode="FULL")
    twice = inject_lineage(once, input_snapshot_id=2, job_run_id="b", refresh_mode="INCREMENTAL")

    assert twice.column_names.count(INPUT_SNAPSHOT_COL) == 1
    assert twice[INPUT_SNAPSHOT_COL].to_pylist() == [2, 2]
    assert twice[JOB_RUN_ID_COL].to_pylist() == ["b", "b"]


def test_new_job_run_id_unique():
    a = new_job_run_id()
    b = new_job_run_id()
    assert a.startswith("run_") and b.startswith("run_")
    assert a != b


def test_lineage_with_null_snapshot():
    data = pa.table({"x": [1, 2]})
    result = inject_lineage(
        data, input_snapshot_id=None, job_run_id="r", refresh_mode="FULL"
    )
    assert result[INPUT_SNAPSHOT_COL].null_count == 2
