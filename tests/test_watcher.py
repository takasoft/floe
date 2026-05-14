"""Tests for the polling-based watcher (v0.1 event detection)."""

from __future__ import annotations

from unittest.mock import MagicMock

from floe.models import DynamicTable
from floe.planner import DAGPlanner
from floe.watcher import WatchConfig, Watcher


def _dit(name: str, upstream: list[str]) -> DynamicTable:
    return DynamicTable(
        name=name,
        query=f"SELECT * FROM {upstream[0]}" if upstream else "SELECT 1",
        upstream_tables=upstream,
    )


def _mock_pipeline(dits: list[DynamicTable], snapshots: dict[str, int]) -> MagicMock:
    """A mock Pipeline whose catalog reports the given snapshot IDs by name.

    The `snapshots` dict is shared with the test, so mutating it between
    iterations simulates new commits landing on upstream tables.
    """
    pipeline = MagicMock()
    pipeline.planner = DAGPlanner(dits)
    pipeline.dits = {d.name: d for d in dits}

    pipeline.catalog_mgr.current_snapshot_id.side_effect = lambda name: snapshots.get(name)
    pipeline.catalog_mgr.table_exists.side_effect = lambda name: name in snapshots
    return pipeline


def test_initialize_records_current_snapshots():
    dits = [_dit("silver.orders", ["bronze.raw_orders", "bronze.customers"])]
    snapshots = {"bronze.raw_orders": 100, "bronze.customers": 200}
    watcher = Watcher(_mock_pipeline(dits, snapshots))
    watcher._initialize()
    assert watcher.last_seen == {"bronze.raw_orders": 100, "bronze.customers": 200}


def test_detect_changes_returns_only_changed_sources():
    dits = [_dit("silver.orders", ["bronze.raw_orders", "bronze.customers"])]
    snapshots = {"bronze.raw_orders": 100, "bronze.customers": 200}
    watcher = Watcher(_mock_pipeline(dits, snapshots))
    watcher._initialize()

    assert watcher._detect_changes() == set()

    snapshots["bronze.raw_orders"] = 101
    assert watcher._detect_changes() == {"bronze.raw_orders"}
    # last_seen is updated, so a second poll with no further changes returns nothing
    assert watcher._detect_changes() == set()


def test_affected_dits_in_topological_order():
    dits = [
        _dit("silver.orders", ["bronze.raw_orders"]),
        _dit("gold.metrics", ["silver.orders"]),
    ]
    snapshots = {"bronze.raw_orders": 100}
    watcher = Watcher(_mock_pipeline(dits, snapshots))
    affected = watcher._affected_dits({"bronze.raw_orders"})
    assert affected == ["silver.orders", "gold.metrics"]


def test_affected_dits_handles_multiple_external_sources():
    dits = [
        _dit("silver.orders", ["bronze.raw_orders"]),
        _dit("silver.customers", ["bronze.customers"]),
        _dit("gold.metrics", ["silver.orders", "silver.customers"]),
    ]
    snapshots = {"bronze.raw_orders": 100, "bronze.customers": 200}
    watcher = Watcher(_mock_pipeline(dits, snapshots))
    affected = watcher._affected_dits({"bronze.raw_orders"})
    # Only silver.orders and gold.metrics are downstream of bronze.raw_orders
    assert affected == ["silver.orders", "gold.metrics"]


def test_run_triggers_refresh_when_upstream_changes():
    dits = [
        _dit("silver.orders", ["bronze.raw_orders"]),
        _dit("gold.metrics", ["silver.orders"]),
    ]
    snapshots = {"bronze.raw_orders": 100}
    pipeline = _mock_pipeline(dits, snapshots)

    refreshed: list[str] = []
    pipeline.executor.refresh.side_effect = lambda dit: refreshed.append(dit.name)

    watcher = Watcher(
        pipeline,
        WatchConfig(poll_interval_seconds=0, quiet_period_seconds=0, max_iterations=2),
    )

    # Mutate the snapshot on the first detect() call to simulate an upstream commit.
    original_detect = watcher._detect_changes
    call_count = {"n": 0}

    def detect_with_mutation():
        call_count["n"] += 1
        if call_count["n"] == 1:
            snapshots["bronze.raw_orders"] = 101
        return original_detect()

    watcher._detect_changes = detect_with_mutation  # type: ignore[method-assign]

    watcher.run()

    # silver.orders refreshes before gold.metrics (topological order)
    assert refreshed == ["silver.orders", "gold.metrics"]


def test_run_does_not_refresh_when_no_changes():
    dits = [_dit("silver.orders", ["bronze.raw_orders"])]
    snapshots = {"bronze.raw_orders": 100}
    pipeline = _mock_pipeline(dits, snapshots)

    refreshed: list[str] = []
    pipeline.executor.refresh.side_effect = lambda dit: refreshed.append(dit.name)

    watcher = Watcher(
        pipeline,
        WatchConfig(poll_interval_seconds=0, quiet_period_seconds=0, max_iterations=3),
    )
    watcher.run()

    assert refreshed == []


def test_current_snapshot_id_returns_none_for_missing_table():
    dits = [_dit("silver.orders", ["bronze.raw_orders"])]
    snapshots: dict[str, int] = {}  # bronze.raw_orders not yet created
    watcher = Watcher(_mock_pipeline(dits, snapshots))
    assert watcher._current_snapshot_id("bronze.raw_orders") is None
