"""Tests for the DIT SQL parser."""

import pytest

from floe.models import RefreshMode
from floe.parser import extract_upstream_tables, parse_dit_sql


def test_parse_minimal_dit():
    sql = """
    CREATE DYNAMIC TABLE silver.orders
      AS
      SELECT * FROM bronze.raw_orders;
    """
    dit = parse_dit_sql(sql)
    assert dit.name == "silver.orders"
    assert dit.refresh_mode == RefreshMode.INCREMENTAL  # default
    assert dit.lag == "5 minutes"  # default
    assert dit.upstream_tables == ["bronze.raw_orders"]


def test_parse_with_options():
    sql = """
    CREATE DYNAMIC TABLE gold.metrics
      LAG = '1 hour'
      REFRESH_MODE = 'FULL'
      AS
      SELECT region, sum(amount) as total
      FROM silver.orders
      GROUP BY region;
    """
    dit = parse_dit_sql(sql)
    assert dit.name == "gold.metrics"
    assert dit.lag == "1 hour"
    assert dit.refresh_mode == RefreshMode.FULL
    assert dit.upstream_tables == ["silver.orders"]


def test_extract_join_dependencies():
    query = """
        SELECT o.id, c.region
        FROM bronze.raw_orders o
        JOIN bronze.customers c ON o.customer_id = c.id
    """
    tables = extract_upstream_tables(query)
    assert set(tables) == {"bronze.raw_orders", "bronze.customers"}


def test_invalid_refresh_mode():
    sql = """
    CREATE DYNAMIC TABLE x.y
      REFRESH_MODE = 'WAT'
      AS SELECT * FROM a.b;
    """
    with pytest.raises(ValueError, match="Unknown refresh mode"):
        parse_dit_sql(sql)


def test_strips_comments():
    sql = """
    -- comment line
    CREATE DYNAMIC TABLE silver.orders AS SELECT * FROM bronze.raw_orders;
    """
    dit = parse_dit_sql(sql)
    assert dit.name == "silver.orders"


def test_namespace_and_table_name():
    dit = parse_dit_sql("CREATE DYNAMIC TABLE silver.orders AS SELECT * FROM bronze.x;")
    assert dit.namespace == "silver"
    assert dit.table_name == "orders"


def test_cte_references_excluded():
    """CTE aliases must not be treated as upstream tables."""
    query = """
        WITH agg AS (
            SELECT da_id, count(*) AS n FROM silver.deliveries GROUP BY da_id
        )
        SELECT a.da_id, a.n, d.region
        FROM agg a JOIN bronze.delivery_associates d ON a.da_id = d.da_id
    """
    tables = extract_upstream_tables(query)
    assert set(tables) == {"silver.deliveries", "bronze.delivery_associates"}
    assert "agg" not in tables
