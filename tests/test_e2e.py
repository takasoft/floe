"""End-to-end test: real Iceberg catalog, real DuckDB, real refresh."""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa

from floe.catalog import CatalogManager
from floe.executor import strip_lineage_columns
from floe.lineage import LINEAGE_COLUMNS
from floe.parser import parse_dit_sql
from floe.pipeline import Pipeline


def _seed_source_tables(
    catalog_mgr: CatalogManager,
    orders: pa.Table,
    customers: pa.Table,
):
    catalog_mgr.ensure_namespace("bronze")

    orders_table = catalog_mgr.catalog.create_table(
        "bronze.raw_orders",
        schema=orders.schema,
    )
    orders_table.append(orders)

    cust_table = catalog_mgr.catalog.create_table(
        "bronze.customers",
        schema=customers.schema,
    )
    cust_table.append(customers)


def _write_dit_file(transformations_dir: Path, name: str, sql: str):
    transformations_dir.mkdir(parents=True, exist_ok=True)
    (transformations_dir / f"{name}.sql").write_text(sql, encoding="utf-8")


def test_end_to_end_simple_dit(project_dir, floe_config, catalog_mgr, sample_orders, sample_customers):
    """A single DIT joining two source tables produces correct output."""
    _seed_source_tables(catalog_mgr, sample_orders, sample_customers)

    _write_dit_file(
        project_dir / "transformations",
        "silver_orders",
        """
        CREATE DYNAMIC TABLE silver.orders
          REFRESH_MODE = 'INCREMENTAL'
          AS
          SELECT o.order_id, o.amount, c.region
          FROM bronze.raw_orders o
          JOIN bronze.customers c ON o.customer_id = c.id
          ORDER BY o.order_id;
        """,
    )

    # Reload pipeline with the same catalog/config so the in-test catalog state is used.
    from floe.parser import load_dits
    dits = load_dits(project_dir / "transformations")
    pipeline = Pipeline(floe_config, dits, catalog_mgr)

    results = pipeline.refresh_all()
    assert len(results) == 1
    r = results[0]
    assert r.table == "silver.orders"
    assert r.rows_written == 3
    assert not r.skipped
    assert r.input_snapshot_id is not None
    assert r.output_snapshot_id is not None

    # Read back and verify content
    table = catalog_mgr.load_table("silver.orders").scan().to_arrow()
    assert table.num_rows == 3

    # Lineage columns are present
    for col in LINEAGE_COLUMNS:
        assert col in table.column_names

    # Logical content
    logical = strip_lineage_columns(table)
    regions = sorted(logical["region"].to_pylist())
    assert regions == ["us-east", "us-east", "us-west"]


def test_idempotent_refresh(project_dir, floe_config, catalog_mgr, sample_orders, sample_customers):
    """Refreshing twice with no upstream changes should skip on the second run."""
    _seed_source_tables(catalog_mgr, sample_orders, sample_customers)

    _write_dit_file(
        project_dir / "transformations",
        "silver_orders",
        """
        CREATE DYNAMIC TABLE silver.orders
          AS
          SELECT order_id, amount FROM bronze.raw_orders;
        """,
    )

    from floe.parser import load_dits
    dits = load_dits(project_dir / "transformations")
    pipeline = Pipeline(floe_config, dits, catalog_mgr)

    first = pipeline.refresh_all()[0]
    assert not first.skipped

    second = pipeline.refresh_all()[0]
    assert second.skipped
    assert second.skip_reason == "upstream snapshot unchanged"


def test_multi_hop_dag(project_dir, floe_config, catalog_mgr, sample_orders, sample_customers):
    """A two-hop pipeline (bronze → silver → gold) refreshes in correct order."""
    _seed_source_tables(catalog_mgr, sample_orders, sample_customers)

    _write_dit_file(
        project_dir / "transformations",
        "silver_orders",
        """
        CREATE DYNAMIC TABLE silver.orders AS
          SELECT o.order_id, o.amount, c.region
          FROM bronze.raw_orders o
          JOIN bronze.customers c ON o.customer_id = c.id;
        """,
    )
    _write_dit_file(
        project_dir / "transformations",
        "gold_region_totals",
        """
        CREATE DYNAMIC TABLE gold.region_totals
          REFRESH_MODE = 'FULL'
          AS
          SELECT region, sum(amount) as total
          FROM silver.orders
          GROUP BY region;
        """,
    )

    from floe.parser import load_dits
    dits = load_dits(project_dir / "transformations")
    pipeline = Pipeline(floe_config, dits, catalog_mgr)

    order = pipeline.planner.topological_order()
    assert order.index("silver.orders") < order.index("gold.region_totals")

    results = pipeline.refresh_all()
    assert len(results) == 2
    assert all(not r.skipped for r in results)

    gold = catalog_mgr.load_table("gold.region_totals").scan().to_arrow()
    logical = strip_lineage_columns(gold)
    rows = {r["region"]: r["total"] for r in logical.to_pylist()}
    assert rows["us-east"] == 17.75  # orders 1 (10.5) + 3 (7.25)
    assert rows["us-west"] == 25.0   # order 2


def test_lineage_snapshot_id_recorded(
    project_dir, floe_config, catalog_mgr, sample_orders, sample_customers
):
    _seed_source_tables(catalog_mgr, sample_orders, sample_customers)

    _write_dit_file(
        project_dir / "transformations",
        "silver_orders",
        "CREATE DYNAMIC TABLE silver.orders AS SELECT order_id FROM bronze.raw_orders;",
    )

    from floe.parser import load_dits
    dits = load_dits(project_dir / "transformations")
    pipeline = Pipeline(floe_config, dits, catalog_mgr)

    bronze_snap = catalog_mgr.current_snapshot_id("bronze.raw_orders")
    pipeline.refresh_all()

    silver = catalog_mgr.load_table("silver.orders").scan().to_arrow()
    snap_ids = set(silver["_floe_input_snapshot_id"].to_pylist())
    assert snap_ids == {bronze_snap}
