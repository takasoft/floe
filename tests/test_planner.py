"""Tests for the DAG planner."""

import pytest

from floe.models import DynamicTable
from floe.planner import DAGPlanner


def _dit(name: str, upstream: list[str]) -> DynamicTable:
    return DynamicTable(
        name=name,
        query=f"SELECT * FROM {upstream[0]}" if upstream else "SELECT 1",
        upstream_tables=upstream,
    )


def test_topological_order():
    dits = [
        _dit("gold.metrics", ["silver.orders"]),
        _dit("silver.orders", ["bronze.raw_orders", "bronze.customers"]),
    ]
    p = DAGPlanner(dits)
    order = p.topological_order()
    assert order.index("silver.orders") < order.index("gold.metrics")


def test_cycle_detection():
    dits = [
        _dit("a", ["b"]),
        _dit("b", ["a"]),
    ]
    with pytest.raises(ValueError, match="Cycle detected"):
        DAGPlanner(dits)


def test_external_sources():
    dits = [
        _dit("silver.orders", ["bronze.raw_orders", "bronze.customers"]),
        _dit("gold.metrics", ["silver.orders"]),
    ]
    p = DAGPlanner(dits)
    assert set(p.external_sources()) == {"bronze.raw_orders", "bronze.customers"}


def test_downstream_of():
    dits = [
        _dit("silver.orders", ["bronze.raw_orders"]),
        _dit("gold.metrics", ["silver.orders"]),
    ]
    p = DAGPlanner(dits)
    assert set(p.downstream_of("silver.orders")) == {"gold.metrics"}


def test_render_ascii():
    dits = [
        _dit("silver.orders", ["bronze.raw_orders"]),
        _dit("gold.metrics", ["silver.orders"]),
    ]
    p = DAGPlanner(dits)
    output = p.render_ascii()
    assert "silver.orders" in output
    assert "gold.metrics" in output
    assert "bronze.raw_orders" in output
