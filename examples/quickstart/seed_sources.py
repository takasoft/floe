"""Seed bronze tables with realistic delivery analytics data.

Domain: a (fictional) last-mile delivery service. We track:
  - delivery associates (DAs)
  - routes
  - per-delivery facts
  - per-driver safety events from on-vehicle telemetry

The DITs in transformations/ progressively refine these into per-DA and
per-station daily KPIs plus a regional safety dashboard.
"""

from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa

from floe.catalog import CatalogManager
from floe.config import FloeConfig

CONFIG_PATH = Path(__file__).parent / "floe.yaml"


def _ts(s: str) -> datetime:
    return datetime.fromisoformat(s).replace(tzinfo=timezone.utc)


def main():
    config = FloeConfig.load(CONFIG_PATH)
    mgr = CatalogManager.from_config(
        name=config.project,
        catalog_type=config.catalog.type,
        uri=config.resolved_catalog_uri(),
        warehouse=config.resolved_warehouse(),
    )

    mgr.ensure_namespace("bronze")

    delivery_associates = pa.table({
        "da_id":      pa.array(["DA001", "DA002", "DA003", "DA004"], type=pa.string()),
        "name":       pa.array(["Alice Chen", "Bob Garcia", "Carol Singh", "David Kim"], type=pa.string()),
        "region":     pa.array(["us-west", "us-west", "us-west", "us-west"], type=pa.string()),
        "station_id": pa.array(["SEA1", "SEA1", "PDX1", "PDX1"], type=pa.string()),
        "hire_date":  pa.array(["2024-03-15", "2024-06-01", "2024-01-10", "2025-02-14"], type=pa.string()),
    })

    routes = pa.table({
        "route_id":    pa.array(["R001", "R002", "R003"], type=pa.string()),
        "station_id":  pa.array(["SEA1", "SEA1", "PDX1"], type=pa.string()),
        "miles":       pa.array([12.5, 8.2, 15.0], type=pa.float64()),
        "est_minutes": pa.array([90, 60, 110], type=pa.int32()),
    })

    raw_deliveries = pa.table({
        "delivery_id":      pa.array([
            "D001","D002","D003","D004","D005","D006",
            "D007","D008","D009","D010","D011",
        ], type=pa.string()),
        "da_id":            pa.array([
            "DA001","DA001","DA002","DA002","DA003","DA004",
            "DA001","DA002","DA003","DA003","DA004",
        ], type=pa.string()),
        "route_id":         pa.array([
            "R001","R001","R002","R002","R003","R003",
            "R001","R002","R003","R003","R003",
        ], type=pa.string()),
        "delivered_at":     pa.array([
            _ts("2026-04-12T09:30:00"), _ts("2026-04-12T11:50:00"),
            _ts("2026-04-12T10:15:00"), _ts("2026-04-12T13:05:00"),
            _ts("2026-04-12T08:45:00"), _ts("2026-04-12T14:20:00"),
            _ts("2026-04-13T09:10:00"), _ts("2026-04-13T11:00:00"),
            _ts("2026-04-13T10:30:00"), _ts("2026-04-13T13:45:00"),
            _ts("2026-04-13T14:15:00"),
        ], type=pa.timestamp("us", tz="UTC")),
        "on_time":          pa.array([
            True, True, False, True, True, False,
            True, True, True, False, True,
        ], type=pa.bool_()),
        "delivery_seconds": pa.array([
            5400, 4800, 4200, 3600, 6300, 7200,
            5100, 3500, 6000, 6600, 6900,
        ], type=pa.int64()),
    })

    raw_safety_events = pa.table({
        "event_id":    pa.array(["S001","S002","S003","S004"], type=pa.string()),
        "da_id":       pa.array(["DA002","DA002","DA004","DA004"], type=pa.string()),
        "event_type":  pa.array(["hard_braking","speeding","distracted_driving","hard_braking"], type=pa.string()),
        "severity":    pa.array(["MEDIUM","HIGH","HIGH","LOW"], type=pa.string()),
        "occurred_at": pa.array([
            _ts("2026-04-12T10:14:00"),
            _ts("2026-04-12T13:02:00"),
            _ts("2026-04-12T14:18:00"),
            _ts("2026-04-13T14:12:00"),
        ], type=pa.timestamp("us", tz="UTC")),
    })

    seeds = [
        ("bronze.delivery_associates", delivery_associates),
        ("bronze.routes",              routes),
        ("bronze.raw_deliveries",      raw_deliveries),
        ("bronze.raw_safety_events",   raw_safety_events),
    ]

    for name, data in seeds:
        if not mgr.table_exists(name):
            mgr.catalog.create_table(name, schema=data.schema).append(data)
            print(f"Seeded {name} ({data.num_rows} rows)")
        else:
            print(f"{name} already exists — skipping")


if __name__ == "__main__":
    main()
