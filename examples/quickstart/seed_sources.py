"""Seed bronze tables with realistic delivery analytics data.

Domain: a (fictional) last-mile delivery service. We track:
  - delivery associates (DAs)
  - routes
  - per-delivery facts
  - per-driver safety events from on-vehicle telemetry
  - delivery defects (damage, complaints, contractual SLA breaches)

Defect data trickles in late — sometimes days after the delivery itself.
The DITs progressively refine these into per-DA, per-station, and per-region KPIs.
The partitioned `gold.delivery_quality_daily` DIT demonstrates Floe's
sliding-window freshness contract: only the last 14 day-partitions are kept fresh.

Dates are today-relative so the example always sits across the freshness window.
"""

from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path

import pyarrow as pa

from floe.catalog import CatalogManager
from floe.config import FloeConfig

CONFIG_PATH = Path(__file__).parent / "floe.yaml"


def _ts(d: date, hour: int, minute: int = 0) -> datetime:
    return datetime.combine(d, time(hour, minute), tzinfo=UTC)


def main():
    config = FloeConfig.load(CONFIG_PATH)
    mgr = CatalogManager.from_config(
        name=config.project,
        catalog_type=config.catalog.type,
        uri=config.resolved_catalog_uri(),
        warehouse=config.resolved_warehouse(),
    )
    mgr.ensure_namespace("bronze")

    today = datetime.now(UTC).date()
    days_ago = lambda n: today - timedelta(days=n)  # noqa: E731

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

    # 24 deliveries spread from 18 days ago to today.
    # Some fall outside the 14-day freshness window, some inside.
    delivery_specs = [
        # (delivery_id, da, route, days_ago, hour, on_time, seconds)
        ("D001", "DA001", "R001", 18,  9, True,  5400),
        ("D002", "DA002", "R002", 18, 11, True,  3700),
        ("D003", "DA003", "R003", 17, 10, False, 7200),
        ("D004", "DA001", "R001", 16,  9, True,  5500),
        ("D005", "DA002", "R002", 15, 12, True,  3600),  # last day OUTSIDE 14-day window
        ("D006", "DA001", "R001", 13,  9, True,  5300),
        ("D007", "DA002", "R002", 13, 10, False, 4400),
        ("D008", "DA003", "R003", 12,  8, True,  6100),
        ("D009", "DA004", "R003", 12, 14, False, 7400),
        ("D010", "DA001", "R001", 10,  9, True,  5200),
        ("D011", "DA002", "R002",  9, 11, True,  3500),
        ("D012", "DA003", "R003",  9, 10, True,  6000),
        ("D013", "DA004", "R003",  8, 14, True,  6800),
        ("D014", "DA001", "R001",  7,  9, True,  5100),
        ("D015", "DA002", "R002",  6, 11, True,  3550),
        ("D016", "DA003", "R003",  5, 10, False, 6700),
        ("D017", "DA004", "R003",  4, 14, True,  7000),
        ("D018", "DA001", "R001",  3,  9, True,  5050),
        ("D019", "DA002", "R002",  3, 12, True,  3800),
        ("D020", "DA003", "R003",  2, 10, True,  6050),
        ("D021", "DA004", "R003",  2, 14, False, 7300),
        ("D022", "DA001", "R001",  1,  9, True,  5000),
        ("D023", "DA002", "R002",  0, 11, True,  3450),
        ("D024", "DA003", "R003",  0, 10, True,  6100),
    ]

    raw_deliveries = pa.table({
        "delivery_id":      pa.array([d[0] for d in delivery_specs], type=pa.string()),
        "da_id":            pa.array([d[1] for d in delivery_specs], type=pa.string()),
        "route_id":         pa.array([d[2] for d in delivery_specs], type=pa.string()),
        "delivered_at":     pa.array(
            [_ts(days_ago(d[3]), d[4]) for d in delivery_specs],
            type=pa.timestamp("us", tz="UTC"),
        ),
        "on_time":          pa.array([d[5] for d in delivery_specs], type=pa.bool_()),
        "delivery_seconds": pa.array([d[6] for d in delivery_specs], type=pa.int64()),
    })

    # Safety events scattered around recent deliveries.
    safety_specs = [
        # (event_id, da, days_ago, hour, type, severity)
        ("S001", "DA002", 13, 10, "hard_braking",       "MEDIUM"),
        ("S002", "DA002", 13, 11, "speeding",           "HIGH"),
        ("S003", "DA004", 12, 14, "distracted_driving", "HIGH"),
        ("S004", "DA004",  8, 14, "hard_braking",       "LOW"),
        ("S005", "DA002",  6, 11, "speeding",           "MEDIUM"),
        ("S006", "DA004",  2, 14, "hard_braking",       "MEDIUM"),
    ]

    raw_safety_events = pa.table({
        "event_id":    pa.array([s[0] for s in safety_specs], type=pa.string()),
        "da_id":       pa.array([s[1] for s in safety_specs], type=pa.string()),
        "event_type":  pa.array([s[4] for s in safety_specs], type=pa.string()),
        "severity":    pa.array([s[5] for s in safety_specs], type=pa.string()),
        "occurred_at": pa.array(
            [_ts(days_ago(s[2]), s[3]) for s in safety_specs],
            type=pa.timestamp("us", tz="UTC"),
        ),
    })

    # Defect reports — deliberately filed AFTER the delivery itself.
    # Some defects reference deliveries that fall outside the 14-day freshness window.
    defect_specs = [
        # (defect_id, delivery_id, defect_type, severity, reported_days_ago)
        ("F001", "D003", "damaged_package",   "HIGH",   16),  # on D003 (17d ago)
        ("F002", "D007", "missed_drop_off",   "MEDIUM", 11),  # on D007 (13d ago)
        ("F003", "D009", "damaged_package",   "HIGH",   10),  # on D009 (12d ago)
        ("F004", "D013", "wrong_address",     "LOW",     6),  # on D013 (8d ago)
        ("F005", "D016", "damaged_package",   "MEDIUM",  3),  # on D016 (5d ago)
        ("F006", "D018", "missed_drop_off",   "LOW",     1),  # on D018 (3d ago)
        # Late-arriving: a defect filed today against a 7-day-old delivery
        ("F007", "D014", "customer_complaint","MEDIUM",  0),
    ]

    raw_defects = pa.table({
        "defect_id":   pa.array([d[0] for d in defect_specs], type=pa.string()),
        "delivery_id": pa.array([d[1] for d in defect_specs], type=pa.string()),
        "defect_type": pa.array([d[2] for d in defect_specs], type=pa.string()),
        "severity":    pa.array([d[3] for d in defect_specs], type=pa.string()),
        "reported_at": pa.array(
            [_ts(days_ago(d[4]), 12) for d in defect_specs],
            type=pa.timestamp("us", tz="UTC"),
        ),
    })

    seeds = [
        ("bronze.delivery_associates", delivery_associates),
        ("bronze.routes",              routes),
        ("bronze.raw_deliveries",      raw_deliveries),
        ("bronze.raw_safety_events",   raw_safety_events),
        ("bronze.delivery_defects",    raw_defects),
    ]

    for name, data in seeds:
        if not mgr.table_exists(name):
            mgr.catalog.create_table(name, schema=data.schema).append(data)
            print(f"Seeded {name} ({data.num_rows} rows)")
        else:
            print(f"{name} already exists — skipping")


if __name__ == "__main__":
    main()
