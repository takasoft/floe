"""Append one new delivery to bronze.raw_deliveries — used to test `floe watch`.

Run from a second terminal while `floe watch` is running. The watcher should
detect the new snapshot within --poll seconds and refresh the silver and
gold DITs that depend on bronze.raw_deliveries (transitively):
silver.deliveries_enriched, gold.da_daily_performance,
gold.station_daily_summary, and gold.delivery_quality_daily.

Usage:
    python append_delivery.py                       # auto-generated delivery_id
    python append_delivery.py D099 DA002 R002       # explicit ids
"""

from __future__ import annotations

import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa

from floe.catalog import CatalogManager
from floe.config import FloeConfig

CONFIG_PATH = Path(__file__).parent / "floe.yaml"


def main() -> None:
    args = sys.argv[1:]
    delivery_id = args[0] if len(args) >= 1 else f"D{int(time.time()) % 1_000:03d}"
    da_id = args[1] if len(args) >= 2 else "DA001"
    route_id = args[2] if len(args) >= 3 else "R001"

    config = FloeConfig.load(CONFIG_PATH)
    mgr = CatalogManager.from_config(
        name=config.project,
        catalog_type=config.catalog.type,
        uri=config.resolved_catalog_uri(),
        warehouse=config.resolved_warehouse(),
    )

    table = mgr.catalog.load_table("bronze.raw_deliveries")
    row = pa.table(
        {
            "delivery_id": pa.array([delivery_id], type=pa.string()),
            "da_id": pa.array([da_id], type=pa.string()),
            "route_id": pa.array([route_id], type=pa.string()),
            "delivered_at": pa.array(
                [datetime.now(timezone.utc)], type=pa.timestamp("us", tz="UTC")
            ),
            "on_time": pa.array([True], type=pa.bool_()),
            "delivery_seconds": pa.array([3600], type=pa.int64()),
        }
    )
    table.append(row)
    table.refresh()
    snap = table.current_snapshot()
    print(f"Appended {delivery_id} (da={da_id}, route={route_id}) → bronze.raw_deliveries")
    print(f"New snapshot_id: {snap.snapshot_id if snap else 'unknown'}")


if __name__ == "__main__":
    main()
