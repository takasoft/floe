"""End-to-end test: real Iceberg catalog, real DuckDB, real refresh."""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa

from floe.catalog import CatalogManager
from floe.executor import strip_lineage_columns
from floe.lineage import LINEAGE_COLUMNS
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


def test_partitioned_refresh_window(project_dir, floe_config, catalog_mgr):
    """Partitioned DIT with WINDOW only writes in-window partitions."""
    from datetime import datetime, timedelta, timezone

    today = datetime.now(timezone.utc).date()

    # Bronze: events spanning 30 days. Window is 14 days, so only 14 partitions stay fresh.
    rows = []
    for i in range(30):
        d = today - timedelta(days=i)
        rows.append({"event_date": d, "value": i})
    bronze = pa.table({
        "event_date": pa.array([r["event_date"] for r in rows], type=pa.date32()),
        "value":      pa.array([r["value"] for r in rows], type=pa.int64()),
    })

    catalog_mgr.ensure_namespace("bronze")
    bt = catalog_mgr.catalog.create_table("bronze.events", schema=bronze.schema)
    bt.append(bronze)

    _write_dit_file(
        project_dir / "transformations",
        "gold_daily",
        """
        CREATE DYNAMIC TABLE gold.daily
          PARTITION BY (event_date)
          PARTITION_WINDOW = '14 days'
          REFRESH_MODE = 'INCREMENTAL'
          AS
          SELECT event_date, sum(value) AS total
          FROM bronze.events
          GROUP BY event_date;
        """,
    )

    from floe.parser import load_dits
    dits = load_dits(project_dir / "transformations")
    pipeline = Pipeline(floe_config, dits, catalog_mgr)

    [r] = pipeline.refresh_all()
    assert not r.skipped
    assert r.rows_written == 15  # 14 days back + today inclusive

    out = catalog_mgr.load_table("gold.daily").scan().to_arrow()
    out_dates = sorted({d for d in out["event_date"].to_pylist()})
    cutoff = today - timedelta(days=14)
    assert min(out_dates) == cutoff
    assert max(out_dates) == today


def test_partitioned_refresh_skip_when_fresh(project_dir, floe_config, catalog_mgr):
    """A partitioned DIT skips when upstream is unchanged AND window is fresh."""
    from datetime import datetime, timedelta, timezone

    today = datetime.now(timezone.utc).date()
    bronze = pa.table({
        "event_date": pa.array([today, today - timedelta(days=1)], type=pa.date32()),
        "value":      pa.array([1, 2], type=pa.int64()),
    })

    catalog_mgr.ensure_namespace("bronze")
    catalog_mgr.catalog.create_table("bronze.events", schema=bronze.schema).append(bronze)

    _write_dit_file(
        project_dir / "transformations",
        "gold_daily",
        """
        CREATE DYNAMIC TABLE gold.daily
          PARTITION BY (event_date)
          PARTITION_WINDOW = '7 days'
          PARTITION_FRESHNESS = '1 hour'
          AS
          SELECT event_date, sum(value) AS total FROM bronze.events GROUP BY event_date;
        """,
    )

    from floe.parser import load_dits
    dits = load_dits(project_dir / "transformations")
    pipeline = Pipeline(floe_config, dits, catalog_mgr)

    first = pipeline.refresh_all()[0]
    assert not first.skipped

    # No upstream change AND we just refreshed (well within 1 hour) → skip.
    second = pipeline.refresh_all()[0]
    assert second.skipped
    assert "unchanged" in second.skip_reason


def test_partitioned_refresh_replaces_only_in_window(project_dir, floe_config, catalog_mgr):
    """An older out-of-window partition written by hand is left untouched by refresh."""
    from datetime import datetime, timedelta, timezone

    today = datetime.now(timezone.utc).date()

    # Bronze has only recent data (last 3 days).
    bronze = pa.table({
        "event_date": pa.array(
            [today, today - timedelta(days=1), today - timedelta(days=2)], type=pa.date32()
        ),
        "value": pa.array([10, 20, 30], type=pa.int64()),
    })
    catalog_mgr.ensure_namespace("bronze")
    catalog_mgr.catalog.create_table("bronze.events", schema=bronze.schema).append(bronze)

    _write_dit_file(
        project_dir / "transformations",
        "gold_daily",
        """
        CREATE DYNAMIC TABLE gold.daily
          PARTITION BY (event_date)
          PARTITION_WINDOW = '5 days'
          AS SELECT event_date, CAST(sum(value) AS BIGINT) AS total
             FROM bronze.events GROUP BY event_date;
        """,
    )

    from floe.parser import load_dits
    dits = load_dits(project_dir / "transformations")
    pipeline = Pipeline(floe_config, dits, catalog_mgr)
    pipeline.refresh_all()

    # Manually append a row OUTSIDE the 5-day window — simulating a backfill or older data.
    old_date = today - timedelta(days=20)
    out_of_window = pa.table({
        "event_date":              pa.array([old_date], type=pa.date32()),
        "total":                   pa.array([999], type=pa.int64()),
        "_floe_input_snapshot_id": pa.array([0], type=pa.int64()),
        "_floe_job_run_id":        pa.array(["manual"], type=pa.string()),
        "_floe_processed_at":      pa.array(
            [datetime.now(timezone.utc)], type=pa.timestamp("us", tz="UTC")
        ),
        "_floe_refresh_mode":      pa.array(["MANUAL"], type=pa.string()),
    })
    catalog_mgr.load_table("gold.daily").append(out_of_window)

    # Bump bronze so a refresh fires
    catalog_mgr.append("bronze.events", pa.table({
        "event_date": pa.array([today], type=pa.date32()),
        "value":      pa.array([5], type=pa.int64()),
    }))

    pipeline.refresh_all()

    out = catalog_mgr.load_table("gold.daily").scan().to_arrow()
    dates = set(out["event_date"].to_pylist())
    assert old_date in dates, "old partition was wiped by refresh — overwrite filter is too broad"


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
