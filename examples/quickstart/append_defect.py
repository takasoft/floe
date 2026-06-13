"""Append one new defect to bronze.delivery_defects — used to test `floe watch`.

Run from a second terminal while `floe watch` is running. The watcher
should detect the new snapshot within --poll seconds and refresh all
DITs that depend on bronze.delivery_defects (transitively).

Usage:
    python append_defect.py                       # auto-generated defect_id, attached to D023
    python append_defect.py F999 D020             # explicit defect_id and delivery_id
"""

from __future__ import annotations

import sys
import time
from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa

from floe.catalog import CatalogManager
from floe.config import FloeConfig

CONFIG_PATH = Path(__file__).parent / "floe.yaml"


def main() -> None:
    args = sys.argv[1:]
    defect_id = args[0] if len(args) >= 1 else f"F{int(time.time()) % 1_000_000:06d}"
    delivery_id = args[1] if len(args) >= 2 else "D023"

    config = FloeConfig.load(CONFIG_PATH)
    mgr = CatalogManager.from_config(
        name=config.project,
        catalog_type=config.catalog.type,
        uri=config.resolved_catalog_uri(),
        warehouse=config.resolved_warehouse(),
    )

    table = mgr.catalog.load_table("bronze.delivery_defects")
    row = pa.table({
        "defect_id":   pa.array([defect_id], type=pa.string()),
        "delivery_id": pa.array([delivery_id], type=pa.string()),
        "defect_type": pa.array(["customer_complaint"], type=pa.string()),
        "severity":    pa.array(["MEDIUM"], type=pa.string()),
        "reported_at": pa.array(
            [datetime.now(UTC)], type=pa.timestamp("us", tz="UTC")
        ),
    })
    table.append(row)
    table.refresh()
    snap = table.current_snapshot()
    print(f"Appended {defect_id} (delivery_id={delivery_id}) → bronze.delivery_defects")
    print(f"New snapshot_id: {snap.snapshot_id if snap else 'unknown'}")


if __name__ == "__main__":
    main()
