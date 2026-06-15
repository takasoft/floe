# Floe

[![CI](https://github.com/takasoft/floe/actions/workflows/test.yml/badge.svg)](https://github.com/takasoft/floe/actions/workflows/test.yml)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)
[![Python](https://img.shields.io/badge/python-3.11%2B-blue.svg)](https://www.python.org/downloads/)
[![Code style: ruff](https://img.shields.io/badge/code%20style-ruff-261230.svg)](https://github.com/astral-sh/ruff)

**The cloud-agnostic, event-driven declarative lakehouse engine built on Apache Iceberg.**

> *"Don't build the engine. Delete the engine."*

> **Status: v0.1 MVP.** The declarative refresh engine and polling-based event-driven loop both work — define a derived table in SQL with partition windowing and refresh modes, run `floe watch`, and Floe keeps it fresh as its upstreams get new Iceberg snapshots. The compute engine is pluggable: DuckDB (in-process) is the default, and an opt-in Apache **Flink** engine (`compute.engine: flink`) runs the same refresh on a Flink cluster, writing back to the very same Iceberg tables (unpartitioned, batch, poll-triggered today). Data quality routing, preconditions, push-based event hooks (replacing polling), event-driven **Flink streaming** (the `push` trigger), partitioned Flink refresh, and multi-cloud deployment profiles are [roadmap](#10-roadmap) items (v0.2+).

![Floe watcher dashboard auto-refreshing downstream DITs as upstream Iceberg tables receive new commits](https://github.com/takasoft/floe/releases/download/demo-latest/demo.gif)

## What Floe does, in one minute

You have a raw data table that keeps getting new rows — delivery events, orders, telemetry. You want a *derived* table that's a cleaned, joined, or aggregated view of it, ready for downstream consumers (analytics dashboards, ML feature pipelines, BI tools). Today, you build that by writing a custom pipeline: a Spark or Redshift script that reads the raw table, transforms it, writes the result, and runs on a schedule. Every team writes that orchestration boilerplate from scratch, for every derived table they want.

Floe lets you skip the boilerplate. You declare the derived table once in SQL:

```sql
CREATE DYNAMIC TABLE silver.orders
  PARTITION BY order_date
  PARTITION_WINDOW = INTERVAL '7 days'
  REFRESH_MODE = INCREMENTAL
  AS
  SELECT o.order_id, o.amount, c.region
  FROM bronze.raw_orders o
  JOIN bronze.customers c ON o.customer_id = c.id;
```

Floe builds the dependency graph between upstream and downstream tables, runs the SELECT incrementally for only the partitions that need refreshing when upstream data lands, writes the result back into Iceberg, and injects lineage columns so you can trace every row back to the upstream snapshot that produced it. No orchestration code to write per table.

```mermaid
flowchart LR
    A[("bronze.raw_orders<br/>(Iceberg)")] -->|new commit| F["Floe<br/>refresh engine"]
    B[("bronze.customers<br/>(Iceberg)")] -->|new commit| F
    D["CREATE DYNAMIC TABLE<br/>silver.orders AS SELECT ..."] -.declares.-> F
    F -->|incremental refresh<br/>+ lineage columns| O[("silver.orders<br/>(Iceberg)")]
    O --> Q[Analytics / ML / BI<br/>via Trino, DuckDB, Spark]
```

The vision is to bring Snowflake-style Dynamic Tables to Apache Iceberg, without cloud lock-in. v0.1 ships the core refresh engine; the rest — data quality routing, event-driven triggering, multi-cloud profiles — is what's left to build.

**What's implemented today (v0.1):**

- **Declarative** — define *what* you want, not *how* to refresh it
- **Iceberg-native** — pure open table format; works with any Iceberg-compatible reader (Trino, DuckDB, Spark)
- **Partition-windowed incremental refresh** — partitioned tables rewrite only the in-window partitions when upstreams change; unpartitioned tables recompute-and-replace idempotently (true snapshot-diff incremental is on the roadmap)
- **Full refresh** for complete recomputes
- **Event-driven refresh via polling** — `floe watch` keeps derived tables fresh as upstream Iceberg tables receive new snapshots
- **Pluggable compute engine** — refresh in-process with DuckDB (default), or run the same SQL on an Apache **Flink** cluster with `compute.engine: flink` (unpartitioned tables, batch, poll-triggered); Flink writes back to the same open Iceberg tables. Engine (who computes) and trigger (how a refresh starts) are independent axes.
- **Automatic lineage injection** — every row in a derived table carries `_floe_input_snapshot_id` and `_floe_job_run_id` columns
- **Stateless workers** — all state lives in Iceberg snapshots; no external state stores

**Roadmap (v0.2 and beyond):**

- **Native push-based event hooks** — Iceberg commit listeners + pluggable Event Bus (NATS, EventBridge, Event Hubs, Pub/Sub) replace the v0.1 polling loop
- **Flink streaming compute (the `push` trigger)** — the Flink engine already ships for batch, poll-triggered refresh; the roadmap adds long-running streaming jobs that react to upstream commits (event-driven `push`) for low-latency `TRIGGERED` mode, plus partitioned/windowed refresh on Flink
- **Built-in data quality** — preconditions, quarantine routing, DQ rule enforcement on every write
- **Cloud-agnostic deployment** — packaged for AWS, Azure, GCP. (v0.1 runs against a local SQLite Iceberg catalog.)
- **AI-native authoring** — describe a pipeline in plain English; Floe generates the SQL DDL and wires it into the DAG
- **Local-first iteration** — build and test pipelines instantly against a small local dataset; no CDK/CloudFormation round-trip per change

---

## Try the demo

The quickstart example ships a self-contained delivery-analytics catalog and a demo runner that fires two upstream appends while the dashboard is live:

```bash
# one-time setup
cd examples/quickstart
python seed_sources.py   # seeds bronze.deliveries + bronze.delivery_defects
floe apply               # materialises all three DITs for the first time

# watch the live dashboard
python demo_runner.py    # starts floe watch with the Rich UI; appends data twice in the background
```

The dashboard shows the pipeline DAG, per-table status, snapshot IDs, and a rolling event log — all updating in real time as Floe detects new Iceberg snapshots and refreshes downstream tables.

To record a GIF of the demo (Linux/macOS, requires [VHS](https://github.com/charmbracelet/vhs)):

```bash
# from the repo root (venv must be active)
vhs examples/quickstart/demo.tape
```

---

## Run in Docker (MinIO + Postgres)

The repo ships a Docker Compose stack that runs Floe as a "proper" deployment:
an S3-compatible object store (MinIO) as the Iceberg warehouse, a Postgres-backed
PyIceberg SQL catalog, and the Floe worker as a long-running container. This is the
same code path as a cloud S3 + RDS deployment, just pointed at local containers.

```bash
cp .env.example .env        # adjust credentials if you like
docker compose up --build
```

What comes up:

| Service | Role |
|---------|------|
| `minio` | S3-compatible object store (warehouse); console at http://localhost:9001 |
| `minio-init` | one-shot: creates the warehouse bucket |
| `postgres` | backing store for the PyIceberg SQL catalog |
| `floe-seed` | one-shot: loads the quickstart bronze tables |
| `floe-apply` | one-shot: materialises every dynamic table |
| `floe-watch` | long-running worker: refreshes DITs as upstreams change |

The worker reacts to `SIGTERM`, so `docker compose down` shuts it down gracefully
mid-poll rather than killing it.

### Configuration is 12-factor

The image is configured entirely through `FLOE_*` environment variables (set in
`docker-compose.yml`), so one image runs unchanged locally, in CI, or in the cloud:

| Variable | Purpose |
|----------|---------|
| `FLOE_CATALOG_URI` | catalog connection string (e.g. `postgresql+psycopg2://…` or `sqlite:///…`) |
| `FLOE_WAREHOUSE` | warehouse location (`s3://bucket/prefix` or a local path) |
| `FLOE_S3_ENDPOINT` / `FLOE_S3_ACCESS_KEY_ID` / `FLOE_S3_SECRET_ACCESS_KEY` / `FLOE_S3_REGION` | object-store access |
| `FLOE_CATALOG_PROP__<key>` | generic PyIceberg property passthrough (`__` → `.`) |

> **Why Compose and not Kubernetes?** v0.1's worker is a single, uncoordinated
> polling loop whose change-detection state lives in memory, so running multiple
> replicas would double-refresh. Horizontal scale-out needs the push-based event
> bus and worker coordination on the [roadmap](#10-roadmap) (v0.2). Until then
> Compose is the right fit; Kubernetes is the natural home once those land.

### Flink as a compute engine

Floe's compute engine is pluggable. The default is **DuckDB** (in-process,
batch). Opt into **Apache Flink** and Floe submits each table's SQL to a Flink
cluster over its **SQL Gateway**; Flink computes the result and writes it back
into the **same** Iceberg catalog and warehouse Floe manages. Because Floe's
outputs are plain, engine-agnostic Iceberg tables, either engine can read or
refresh any table.

Engine (*who* computes) and trigger (*how* a refresh starts) are independent axes:

| | `trigger: poll` (pull) | `trigger: push` (streaming) |
|---|---|---|
| `engine: duckdb` | ✅ default (v0.1) | — (DuckDB is batch-only) |
| `engine: flink` | ✅ batch refresh on Flink | 🚧 roadmap (event-driven streaming) |

Select the engine in `floe.yaml` (or via `FLOE_COMPUTE_ENGINE` /
`FLOE_COMPUTE_TRIGGER` / `FLOE_FLINK_SQL_GATEWAY_URL`):

```yaml
compute:
  engine: flink     # duckdb (default) | flink
  trigger: poll     # poll (default) | push (roadmap; requires engine: flink)
  flink:
    sql_gateway_url: http://localhost:8083
```

Bring the Flink cluster and SQL Gateway up with the opt-in `flink` profile:

```bash
# core stack still runs; the flink-* services are added on top
docker compose --profile flink up --build
# Flink dashboard: http://localhost:8081 ; SQL Gateway REST: http://localhost:8083
```

| Service | Role |
|---------|------|
| `flink-jobmanager` | Flink coordinator; dashboard at http://localhost:8081 |
| `flink-taskmanager` | Flink worker (executes the job) |
| `flink-sql-gateway` | long-running SQL Gateway (REST on :8083); Floe's `flink` engine submits statements here |
| `flink-sql` | one-shot interop demo: registers Floe's catalog and runs `docker/flink/demo.sql` |

**Scope today:** the Flink engine handles **unpartitioned** tables with batch
(`poll`) refresh, mirroring the DuckDB engine's recompute-and-replace semantics
and the same `_floe_*` lineage columns (it `INSERT OVERWRITE`s an existing table,
or creates it on first run). Partitioned/windowed tables still use the DuckDB
engine — a Flink refresh of one raises a clear error — and the streaming `push`
trigger is on the roadmap. Pinned versions live in `docker/flink/Dockerfile`
(Flink 2.1, Iceberg 1.11.0); Flink's `JdbcCatalog` and PyIceberg's `SqlCatalog`
share the one Postgres catalog, so both engines resolve the identical tables.

### Dashboards and observability

Five web views let you watch and query the system as it runs:

| What you want to see | Where | How to open |
|----------------------|-------|-------------|
| **Table updates in real time** (the pipeline DAG, per-table status, snapshot IDs, lineage trace, and a rolling event log) | Floe's own terminal dashboard | run `floe watch`, or `python examples/quickstart/demo_runner.py` for a scripted demo |
| **The Iceberg warehouse** (buckets, data files, manifests, and metadata as new snapshots commit) | MinIO console | http://localhost:9001 (log in with the `.env` credentials) |
| **Flink jobs** (operators, records in and out, backpressure, checkpoints, task slots) | Flink Web UI | http://localhost:8081 (requires the `flink` profile) |
| **The containers themselves** (live, searchable logs and CPU / memory stats, grouped by Compose service) | Dozzle, an opt-in container UI | `docker compose --profile ui up -d dozzle`, then http://localhost:8888 |
| **Query the tables with SQL** (ad-hoc queries, row previews, snapshot history, time travel, charts) | Metabase, an opt-in web SQL workbench backed by Trino | `docker compose --profile sql up -d`, then http://localhost:3000 (see [Inspecting a table](#inspecting-a-table)) |

Start with `floe watch`: it renders the DAG and refreshes each downstream table
live as Floe detects new upstream Iceberg snapshots (the GIF at the top of this
README is exactly this dashboard). The container UI is handy when you run Docker
headless (for example Docker Engine in WSL2) and so don't have Docker Desktop's
built-in Compose view; if you do run Docker Desktop, its Containers tab already
shows this Compose project. Dozzle is read-only and stores nothing, so it is safe
to leave running alongside the stack.

### Inspecting a table

The MinIO console shows the warehouse as raw objects: opaque Parquet data files
and Iceberg metadata / manifest blobs. That is handy for confirming that files
and snapshots physically land (MinIO is just a local stand-in for AWS S3, Azure
ADLS, or GCS), but you cannot read a table or its history from it.

To inspect a table itself, use standard Iceberg tooling rather than anything
Floe-specific. **PyIceberg ships a `pyiceberg` CLI** (already a Floe dependency)
that talks to the very same catalog. Point it at the running stack from inside
the worker container, using the values from your `.env`; the catalog name is the
`project` from `floe.yaml` (here, `quickstart`):

```bash
docker compose exec floe-watch bash      # open a shell in the worker container

# inside the container, pointing pyiceberg at the same catalog Floe writes to:
export PYICEBERG_CATALOG__QUICKSTART__TYPE=sql
export PYICEBERG_CATALOG__QUICKSTART__URI="postgresql+psycopg2://floe:floe@postgres:5432/floe"
export PYICEBERG_CATALOG__QUICKSTART__WAREHOUSE="s3://floe-warehouse/wh"
export PYICEBERG_CATALOG__QUICKSTART__S3__ENDPOINT="http://minio:9000"
export PYICEBERG_CATALOG__QUICKSTART__S3__ACCESS_KEY_ID=minioadmin
export PYICEBERG_CATALOG__QUICKSTART__S3__SECRET_ACCESS_KEY=minioadmin

pyiceberg --catalog quickstart list     silver                         # tables in a namespace
pyiceberg --catalog quickstart describe silver.deliveries_enriched     # schema + current snapshot + full snapshot history
pyiceberg --catalog quickstart schema   silver.deliveries_enriched     # column types
pyiceberg --catalog quickstart files    silver.deliveries_enriched     # the Parquet files behind the table
```

`describe` prints the schema, the current snapshot, and the full list of
snapshots (every refresh commits one), so it doubles as the table's version
history and audit trail.

For ad-hoc SQL in a browser, the optional `sql` profile starts a complete
open-source SQL workbench over the very same tables, again with no Floe-specific
code: **Trino** (its Iceberg connector reads the shared Postgres catalog and
MinIO warehouse) fronted by **Metabase** (a web SQL editor and charting UI that
ships a Trino driver):

```bash
docker compose --profile sql up -d
# Metabase (web SQL editor + charts): http://localhost:3000
# Trino   (its own query/cluster UI): http://localhost:8080
```

On Metabase's first run, create an account, then add a database of type
**Starburst** with host `trino`, port `8080`, catalog `iceberg`, username
`metabase`, and no password. You can then query the tables from the browser:

```sql
-- on-time rate by region
SELECT region, count(*) AS deliveries,
       round(100.0 * sum(if(on_time, 1, 0)) / count(*), 1) AS pct_on_time
FROM iceberg.silver.deliveries_enriched
GROUP BY region ORDER BY deliveries DESC;

-- version history / audit trail (one commit per refresh)
SELECT committed_at, operation, summary['total-records'] AS rows
FROM iceberg.silver."deliveries_enriched$snapshots"
ORDER BY committed_at DESC;

-- time travel: read the table as of an older snapshot
SELECT count(*) FROM iceberg.silver.deliveries_enriched
FOR VERSION AS OF <snapshot_id>;
```

Trino's Iceberg connector also exposes `$history` and `$files` metadata tables.
Prefer the terminal? **DuckDB** is already a Floe dependency and has its own
browser UI (`duckdb -ui`) plus an
[`iceberg` extension](https://duckdb.org/docs/stable/extensions/iceberg). Every
option here reads the same Iceberg metadata, so none of them needs anything
specific to Floe.

---

## Table of Contents

1. [The Problem](#1-the-problem)
2. [Inspiration: Snowflake's Architecture](#2-inspiration-snowflakes-architecture)
3. [Core Concepts](#3-core-concepts)
4. [Architecture Overview](#4-architecture-overview)
5. [Component Deep Dives](#5-component-deep-dives)
6. [Developer Interface](#6-developer-interface)
7. [Deployment Modes](#7-deployment-modes)
8. [Operational Patterns](#8-operational-patterns)
9. [Comparison with Existing Projects](#9-comparison-with-existing-projects)
10. [Roadmap](#10-roadmap)
11. [Open Questions](#11-open-questions)

---

## 1. The Problem

### 1.1 A pipeline that should be simple

You build a delivery analytics pipeline. Two source tables — `bronze.deliveries` (every package delivered) and `bronze.delivery_defects` (damage reports, customer complaints, contractual SLA breaches) — both partitioned by `delivery_date`. You build a `gold.delivery_quality_daily` table that joins them to compute the daily defect rate per region.

Simple, until you understand how the data actually behaves:

- **Delivery events** show up within minutes of the package being delivered.
- **Defect reports** trickle in for days afterward. A delivery on April 22 might have a customer complaint filed on April 27, a contractual review filed on May 1, and a chargeback filed on May 5.

Your `gold.delivery_quality_daily` partition for April 22 is *wrong* on April 23, *less wrong* on April 24, and *roughly correct* by May 5. Each old partition needs to be **revisited** as late-arriving defects show up.

### 1.2 The freshness requirement that won't fit in cron

You write down the SLA your stakeholders actually want:

> *Every partition for the last 14 days must have been recomputed within 2 days of that partition's date.*

So today's partition needs a refresh today or yesterday. Last week's partition needs a refresh from this week. The partition from 14 days ago is allowed to be a little older. Older than 14 days, you don't care — that data is now stable.

This is a **sliding-window freshness contract**, and no single cron schedule can express it. You can run a job daily that rewrites the last 14 partitions, but then every question about "is this fresh enough?" lives in your head, not in the system.

### 1.3 The dilemmas nobody likes

You write the daily job. Then the operational questions start:

- **Upstream isn't ready yet.** `bronze.delivery_defects` hasn't refreshed today. Do you wait? Wait how long? At what point do you give up and run with stale upstream data?
- **Or do you trigger upstream?** If you can re-run the upstream pipeline yourself, do you do it on every run? You're now coupling pipelines that were supposed to be independent — and you've just rebuilt a fragile orchestration layer.
- **Late arrivals outside the window.** A defect lands for January 12 — a partition from four months ago, well outside your 14-day window. Do you refresh it? Your job ignores anything older.
- **Backfills.** A bug is fixed. You need to recompute 90 partitions correctly. Your daily-cron design has no answer for this; you write a one-off script, run it manually, hope nobody else's pipeline is reading mid-write.
- **Partial dependencies.** Today's `gold` partition only needs today's `bronze` data. But your job re-reads all 14 days of upstream because there's no easy way to express "only this partition depends on that partition."

You patch each one. You add sensors. You add YAML dependency configs. You add manual override flags. Six months later your "platform" is a 50K-line Python service that nobody but you can extend without breaking. **This is the trap. Every data team falls in.** The fix is always more orchestration code, and that code becomes the most fragile thing in your stack.

### 1.4 What if the data drove its own refresh?

Floe is what happens when you stop scheduling and let the **data** trigger the work.

A new partition lands in `bronze.delivery_defects`? The corresponding `gold.delivery_quality_daily` partition is automatically marked stale and refreshed. A late-arriving defect for January 12? The January 12 gold partition refreshes — same code path, no special handling. A backfill? You replay the upstream snapshots; everything downstream catches up automatically. You write the SQL once, declare the freshness target, and Floe figures out the rest.

There is no scheduling. There is no "is the upstream fresh enough" decision in your code. The DAG knows.

### 1.5 What's missing in open source

Streaming-native lakehouses are a maturing space, but no project hits all five must-haves:

| Project | Event-Driven | Iceberg-Native | Dynamic Table DAG | Cloud-Agnostic | Local Mode |
|---------|:---:|:---:|:---:|:---:|:---:|
| Apache Paimon | ✅ | ❌ (own format) | ❌ | ✅ | Partial |
| RisingWave | ✅ | Partial | ✅ | ✅ | ✅ |
| Flink + Iceberg (DIY) | ✅ | ✅ | ❌ | ✅ | ❌ |
| Delta Live Tables | ✅ | ❌ | ✅ | ❌ (Databricks only) | ❌ |
| Snowflake Dynamic Tables | ✅ | ❌ | ✅ | ❌ (Snowflake only) | ❌ |
| **Floe** | ✅ | ✅ | ✅ | ✅ | ✅ |

No existing open-source project combines all five. Floe fills this gap.

### 1.6 What Floe is and isn't

Floe **is** a coordination layer for declarative pipelines on Apache Iceberg. You declare your transformations and freshness targets; Floe handles dependency resolution, incremental refresh, lineage, and data quality routing.

Floe **is not**:

- **A query engine.** Floe does not serve interactive SQL queries. Use Trino, DuckDB, or Spark SQL to read Floe-managed Iceberg tables.
- **A data catalog UI.** Use Apache Polaris, Project Nessie, or your cloud provider's catalog.
- **A replacement for Kafka/Flink.** Floe is a coordination and declaration layer that orchestrates Flink jobs — it does not replace the underlying compute.

---

## 2. Inspiration: Snowflake's Architecture

Floe is a direct implementation of the patterns described in Snowflake's research papers: *"Streaming Democratized"* and *"What's the Difference?"*

### 2.1 Immutable Versioned Storage

Snowflake never modifies a file in place. Every write produces a new table version defined by the set of micro-partitions active at that timestamp. Apache Iceberg replicates this exactly:

- Every commit creates a new **Snapshot ID**
- Snapshots are immutable; old snapshots enable time travel
- Incremental scans read only the files added between two snapshot IDs: `read.option("start-snapshot-id", X)`

This makes Iceberg the correct storage foundation for Floe — not Delta Lake (which has a different metadata model) and not a proprietary format.

### 2.2 Query Differentiation

Snowflake treats streaming as a **mathematical problem**, not a data movement problem. Instead of re-running the full query `Q` on every refresh, the Snowflake compiler rewrites it into its derivative `ΔQ` — a query that operates only on the rows that changed since the last checkpoint.

Algebraic rules apply:
- `Δ(A JOIN B)` → scan only new rows in A joined against full B (or vice versa)
- `Δ(GROUP BY)` → merge new aggregates into existing aggregates
- `Δ(FILTER)` → apply filter to new rows only

Compute cost scales with **change volume**, not **total data volume**.

Floe implements this via Flink's incremental processing model, generating merge plans that operate on Iceberg snapshot diffs rather than full table scans.

### 2.3 Dynamic Tables

Snowflake's Dynamic Tables abstract away scheduling entirely:

1. User declares the desired output (SQL query) and desired freshness (`TARGET_LAG`)
2. The system builds a DAG of dependencies
3. The scheduler monitors upstream freshness and triggers refreshes only when needed

Floe's **Dynamic Iceberg Tables (DITs)** are the open-source equivalent, extended with explicit data quality preconditions and quarantine routing.

---

## 3. Core Concepts

### 3.1 Dynamic Iceberg Table (DIT)

The central abstraction. A DIT is an Iceberg table defined by a query over other Iceberg tables or streaming sources. It has:

- A **query** (the desired output)
- A **lag target** (desired freshness)
- A **refresh mode** (how to compute the update)
- Optional **preconditions** (data readiness checks before processing)
- Optional **DQ routing** (what to do with rows that fail quality checks)

```sql
CREATE DYNAMIC TABLE silver.orders
  LAG = '5 minutes'
  REFRESH_MODE = INCREMENTAL
  PRECONDITIONS (
    min_rows(bronze.raw_orders, count=1)
  )
  ON_DQ_FAIL ROUTE TO quarantine.orders_failed
  AS
  SELECT
    o.order_id,
    o.amount,
    c.region,
    c.segment
  FROM bronze.raw_orders o
  JOIN bronze.customers c ON o.customer_id = c.id;
```

### 3.2 Refresh Modes

| Mode | Description | When to Use |
|------|-------------|-------------|
| `INCREMENTAL` | Reads only the Iceberg snapshot diff; merges into output table | Default; works for most joins, filters, simple aggregations |
| `FULL` | Full recompute from scratch | Complex aggregations where incremental rewrite is not possible |
| `TRIGGERED` | Fires immediately on any upstream commit event | Sub-minute latency requirements |
| `SCHEDULED` | Time-based (cron expression), ignores commit events | External data sources without Iceberg commits |

Floe automatically degrades to `FULL` if the DAG Planner determines `INCREMENTAL` is not safe for the given query.

> **v0.1 implementation:** `TRIGGERED` and `SCHEDULED` are roadmap modes. Today `INCREMENTAL` does not yet read Iceberg snapshot diffs — for *partitioned* tables it rewrites only the in-window partitions (a partition-level incremental), and for *unpartitioned* tables it recomputes the full result and replaces the table. Recompute-and-replace is idempotent for every query shape (joins, aggregations), so refreshes never duplicate or double-count rows. The snapshot-diff delta merge described above lands with the Flink compute layer in v0.2.

### 3.3 Partition-Aware Freshness

A single `LAG` value is fine for tables that are always rewritten as a whole, but most production pipelines have a more nuanced freshness contract:

> *"Every partition for the last N days must have been recomputed within X of right now."*

This pattern shows up everywhere — financial close cycles, regulatory reporting windows, late-arriving event corrections, slowly-stabilizing aggregates. Floe expresses it directly:

```sql
CREATE DYNAMIC TABLE gold.delivery_quality_daily
  PARTITION BY (delivery_date)
  PARTITION_WINDOW = '14 days'        -- how far back to keep fresh
  PARTITION_FRESHNESS = '2 days'      -- max staleness within the window
  REFRESH_MODE = 'INCREMENTAL'
  AS
  SELECT
    d.delivery_date, d.region, d.station_id,
    COUNT(*) AS total_deliveries,
    COUNT(f.defect_id) AS defect_count,
    ROUND(100.0 * COUNT(f.defect_id) / COUNT(*), 2) AS defect_rate_pct
  FROM silver.deliveries_enriched d
  LEFT JOIN bronze.delivery_defects f USING (delivery_id)
  GROUP BY d.delivery_date, d.region, d.station_id;
```

Behavior:

- **Window** — at refresh time, only rows where `delivery_date >= today - 14 days` are computed and written. Partitions outside the window are never touched by Floe and remain at whatever value they had at last refresh.
- **Freshness** — a refresh fires when EITHER upstream changes OR `now() - last_refresh > 2 days`. The freshness clock guarantees the SLA is met even if upstream is quiet.
- **Atomic partition replacement** — in-window partitions are overwritten in a single Iceberg commit using a `partition_col >= cutoff` filter. Older partitions are not part of the transaction.
- **Late arrivals are free** — when a defect is filed today against a 7-day-old delivery, the next refresh of the 14-day window picks it up. No backfill script, no special-case handling.

The output table is created with an Iceberg identity partition spec on the declared columns, so partition pruning works for any downstream reader (Trino, Spark, DuckDB).

### 3.4 The Precondition (Sensor Pattern)

A precondition is a data readiness assertion that must pass before a refresh executes. If a precondition fails, the refresh exits cleanly (no error, no partial write) and retries on the next event.

Built-in precondition functions:

| Function | Description |
|----------|-------------|
| `min_rows(table, count=N)` | Table must have at least N rows |
| `completeness(table, partition_col, distinct_vals=N)` | Partition column must have N distinct values (e.g., 7 days in a week) |
| `snapshot_age(table, max_age='1 hour')` | Upstream snapshot must be fresher than threshold |
| `dependency_ready(table_a, table_b)` | Both upstream tables must share the same processing epoch |
| Custom Python function | Any callable returning `bool` |

Example — wait for a full week before computing weekly scorecard:

```sql
CREATE DYNAMIC TABLE gold.weekly_scorecard
  LAG = '1 day'
  REFRESH_MODE = INCREMENTAL
  PRECONDITIONS (
    completeness(silver.events, partition_col='day', distinct_vals=7, filter="week = current_week()")
  )
  AS
  SELECT week, da_id, sum(score) as total_score
  FROM silver.events
  GROUP BY week, da_id;
```

### 3.5 Data Quality Routing (Quarantine Pattern)

Rather than crashing on bad data or silently dropping rows, Floe routes DQ-failed rows to a companion quarantine table. The production table only receives rows that pass all DQ checks.

```sql
CREATE DYNAMIC TABLE silver.payments
  LAG = '10 minutes'
  ON_DQ_FAIL ROUTE TO quarantine.payments_failed
  DQ_RULES (
    not_null(payment_id),
    not_null(amount),
    range_check(amount, min=0.01, max=100000),
    no_duplicates(payment_id)
  )
  AS SELECT * FROM bronze.raw_payments;
```

The quarantine table is a regular Iceberg table with an additional `_floe_dq_failure_reason` column. An alert fires if the quarantine write rate exceeds a configured threshold.

### 3.6 Automatic Snapshot Lineage

Every DIT write automatically injects lineage metadata columns. These are non-optional — they are how Floe guarantees auditability for payment-impacting and compliance-sensitive data.

| Column | Description |
|--------|-------------|
| `_floe_input_snapshot_id` | Snapshot ID of the upstream table read during this refresh |
| `_floe_job_run_id` | Unique ID for this specific refresh execution |
| `_floe_processed_at` | UTC timestamp of when this row was written |
| `_floe_refresh_mode` | `INCREMENTAL` or `FULL` — which mode was used |

This enables exact audit queries: *"Show me the exact upstream state that produced payment record X."*

```sql
SELECT _floe_input_snapshot_id
FROM silver.payments
WHERE payment_id = 'pay_abc123';
-- Returns: 8029341234
-- Now time-travel to that snapshot:
SELECT * FROM bronze.raw_payments
FOR VERSION AS OF 8029341234
WHERE payment_id = 'pay_abc123';
```

### 3.7 The DAG

Floe parses all DIT definitions in a project and builds a directed acyclic graph of table dependencies. This DAG drives:

- **Cascade scheduling** — when Table A commits, Floe knows which downstream DITs (B, C, D...) need to be evaluated
- **Parallel refresh** — independent branches of the DAG refresh concurrently
- **Cycle detection** — circular dependencies are rejected at `floe apply` time
- **Schema change propagation** — upstream schema changes are detected before a refresh runs, not during

---

## 4. Architecture Overview

> **v0.1 implementation note:** the architecture below is the target design. v0.1 ships a single-process implementation: polling replaces the push-based Event Bus, and DuckDB replaces Flink as the compute layer. The DAG Planner, Refresh Executor, and Catalog Manager all run in the same Python process. v0.2 swaps the polling loop for catalog commit listeners and a pluggable Event Bus; Flink integration follows.

```mermaid
flowchart TD
    subgraph ControlPlane["Floe Control Plane"]
        DAG["DAG Planner\n─────────────\n• Parse DITs\n• Build graph\n• Plan queries\n• Cycle detect"]
        SCHED["Refresh Scheduler\n─────────────\n• Event listener\n• LAG evaluation\n• Job dispatch\n• Preconditions"]
        CAT["Catalog Manager\n─────────────\n• Table registry\n• Schema mgmt\n• Snapshot index\n• Lineage store"]
    end

    subgraph EventBus["Event Bus (pluggable)"]
        EB["Local: NATS\nAWS: EventBridge\nAzure: Event Hubs\nGCP: Pub/Sub\nOSS: Kafka / Redpanda"]
    end

    subgraph Compute["Compute Layer"]
        W1["Flink Worker\nincremental merge"]
        W2["Flink Worker\nfull refresh"]
        W3["Flink Worker\nCDC ingest"]
    end

    subgraph Storage["Apache Iceberg"]
        META["Metadata\n─────────────\nREST Catalog\nNessie / Polaris"]
        OBJ["Object Storage (pluggable)\n─────────────\nLocal / MinIO · AWS S3\nAzure ADLS Gen2 · GCP GCS"]
    end

    DAG --> EB
    SCHED --> EB
    CAT --> EB
    EB --> W1
    EB --> W2
    EB --> W3
    W1 --> META
    W2 --> META
    W3 --> META
    META --> OBJ
```

### Refresh Event Flow

```
1.  Source write → Iceberg table (CDC via Flink, direct write, or streaming ingest)
2.  Iceberg commit → emits CommitEvent { table, snapshot_id, timestamp }
3.  Event Bus delivers CommitEvent to Refresh Scheduler
4.  Refresh Scheduler checks: which downstream DITs depend on this table?
5.  For each dependent DIT:
      a. Evaluate LAG: is a refresh due?
      b. Run PRECONDITIONS: does the data meet readiness criteria?
      c. If both pass → submit Flink job with incremental merge plan
6.  Flink job reads Iceberg snapshot diff, applies transformation, writes new snapshot
7.  New Iceberg commit on downstream table → emit CommitEvent → repeat from step 3
```

---

## 5. Component Deep Dives

### 5.1 DAG Planner

The DAG Planner is responsible for parsing DIT definitions and building the dependency graph.

**Inputs:** DIT SQL/Python definitions, existing Iceberg catalog state
**Outputs:** Execution plan per DIT (query rewrite, refresh mode, incremental merge strategy)

Key responsibilities:

- **SQL parsing** — extract table references from `AS SELECT ...` to build edges
- **Incremental eligibility** — determine if a query can be run incrementally or requires full recompute. Rules:
  - Simple projections and filters: `INCREMENTAL`
  - Joins where one side is append-only: `INCREMENTAL`
  - Aggregations with `GROUP BY`: `INCREMENTAL` (using partial aggregation merge)
  - Window functions with unbounded windows: `FULL`
  - Self-joins: `FULL`
- **Query rewrite** — generate the `ΔQ` (delta query) for incremental mode, using Iceberg's snapshot diff API
- **Schema validation** — detect upstream schema changes before runtime

### 5.2 Refresh Scheduler

The Refresh Scheduler is the heart of the event-driven loop. It is a lightweight, stateless process — its only persistent state is the last-processed snapshot ID per DIT, stored as Iceberg table properties.

**Responsibilities:**
- Subscribe to the Event Bus for `CommitEvent` messages
- Maintain the per-DIT refresh timer (LAG evaluation)
- Evaluate preconditions before dispatching a Flink job
- Prevent duplicate dispatches (idempotency guard using snapshot ID)
- Handle backpressure: if a DIT's Flink job is still running, queue (do not drop) the next trigger

**Idempotency:** Before dispatching a job, the Scheduler checks: *"Has this specific upstream snapshot already been processed for this DIT?"* If yes, skip. The checkpoint is written to the output Iceberg table's properties — no external state store required.

### 5.3 Catalog Manager

Wraps the Iceberg catalog to provide Floe-specific operations:

- Table registration and discovery
- Schema evolution coordination (schema changes require DAG re-validation)
- Snapshot indexing for incremental scan planning
- Lineage metadata storage (DIT → upstream snapshot mapping)

**Supported catalogs:**
- Iceberg REST Catalog (default)
- Project Nessie (Git-like branching for Iceberg tables)
- Apache Polaris (cloud-managed)
- AWS Glue Data Catalog
- Hive Metastore

### 5.4 Event Bus (Pluggable)

> **v0.1 implementation:** an in-process polling loop (`floe watch`) replaces the Event Bus. It polls every external upstream table's `current_snapshot_id` at a configurable interval and dispatches refreshes directly. The push-based design below is v0.2+.

The Event Bus is the signal backbone. It carries `CommitEvent` messages from Iceberg writes to the Refresh Scheduler, and CDC events from source systems to ingestion workers.

| Deployment | Event Bus | Notes |
|------------|-----------|-------|
| Local | Embedded NATS JetStream | Zero external dependencies |
| AWS | Amazon EventBridge | Native Iceberg S3 event integration |
| Azure | Azure Event Hubs | Kafka-compatible API |
| GCP | Google Cloud Pub/Sub | — |
| Self-managed | Apache Kafka / Redpanda | Works everywhere |

The Event Bus interface is a thin abstraction (`publish(event)`, `subscribe(topic, handler)`). Swapping implementations requires only a config change — no code changes to the Scheduler or workers.

### 5.5 Compute Layer (Apache Flink)

> **v0.1 implementation:** DuckDB is the default compute engine, used for both `INCREMENTAL` and `FULL` refresh modes. An opt-in **Flink engine** (`compute.engine: flink`) already ships for unpartitioned, batch, poll-triggered refresh, submitting SQL to a Flink SQL Gateway and writing back to the same Iceberg catalog (see [Flink as a compute engine](#flink-as-a-compute-engine)). What remains for v0.2 is the **streaming-first** Flink path below: long-running jobs for low-latency `TRIGGERED` mode, the event-driven `push` trigger, and partitioned/windowed refresh on Flink. The trade-offs and deployment model below describe that v0.2+ target.

Apache Flink is the primary compute engine. It was chosen over Spark/Glue for two reasons:

1. **Native streaming** — Flink is a streaming-first engine. Spark is batch-first with streaming bolted on. For low-latency `TRIGGERED` refresh modes, Flink's architecture is the correct fit.
2. **Iceberg connector maturity** — Flink's Iceberg connector supports full read/write including streaming writes with exactly-once semantics.

Flink jobs in Floe are **short-lived and stateless**:
- Launched per refresh (not long-running streaming jobs, unless in `TRIGGERED` mode)
- State lives in Iceberg, not in Flink's state backend
- Workers are disposable — any failure simply retries from the last snapshot

For `TRIGGERED` mode (sub-minute latency), Floe runs a persistent Flink streaming job that continuously reads the Iceberg change log.

**Flink deployment options:**
- Standalone (local mode, Docker Compose)
- Kubernetes (via Flink Kubernetes Operator)
- AWS: Amazon EMR / Kinesis Data Analytics
- Azure: HDInsight / Azure Stream Analytics (Flink)
- GCP: Dataproc (Flink)

### 5.6 Storage Layer (Apache Iceberg)

Floe writes exclusively to Apache Iceberg tables. No proprietary format, no vendor lock-in.

**Why Iceberg (not Delta Lake, not Hudi, not Paimon):**

| Feature | Iceberg | Delta Lake | Hudi | Paimon |
|---------|:-------:|:----------:|:----:|:------:|
| Open spec (no single vendor) | ✅ | Partial (Databricks) | ✅ | ✅ |
| Snapshot-based versioning | ✅ | ✅ | Partial | ✅ |
| Incremental scan API | ✅ (`start-snapshot-id`) | Limited | ✅ | ✅ |
| REST Catalog standard | ✅ | ❌ | ❌ | ❌ |
| Multi-engine read support | ✅ | ✅ | Partial | Partial |
| Schema evolution (safe) | ✅ | Partial | Partial | ✅ |

Iceberg's `start-snapshot-id` / `end-snapshot-id` scan option is what makes Floe's incremental refresh mathematically exact. It is the direct equivalent of Snowflake's micro-partition diff.

---

## 6. Developer Interface

### 6.1 Project Structure

```
my-pipeline/
├── floe.yaml              # project config (catalog, compute, storage)
├── sources/
│   └── raw_orders.sql     # source table definitions
├── transformations/
│   ├── silver_orders.sql  # DIT definitions
│   └── gold_metrics.sql
├── dq/
│   └── rules.py           # custom DQ rule functions
└── tests/
    └── test_silver_orders.py
```

### 6.2 Configuration: `floe.yaml`

```yaml
# The complete schema the v0.1 loader accepts (see `floe init`). Unknown
# keys are rejected, so this is the full surface, not a subset.
project: my-analytics-pipeline
version: "1.0"

catalog:
  type: sql                              # PyIceberg SqlCatalog (SQLite or Postgres JDBC)
  uri: sqlite:///./.floe/catalog.db      # local; or postgresql+psycopg2://user:pw@host:5432/db
  warehouse: ./warehouse                 # local path; or s3://my-bucket/wh for object storage
  properties:                            # extra PyIceberg/FileIO props, merged verbatim
    s3.endpoint: http://localhost:9000   # e.g. MinIO; creds can also come from FLOE_S3_* env

compute:
  engine: duckdb                         # duckdb (default, in-process) | flink (submit to a Flink cluster)
  trigger: poll                          # poll (default) | push (roadmap; requires engine: flink)

defaults:
  lag: "5 minutes"
  refresh_mode: INCREMENTAL              # INCREMENTAL | FULL
  lineage: true                          # inject _floe_* lineage columns

transformations_dir: transformations
```

Credentials and environment-specific locations can be injected at runtime through `FLOE_*` environment variables (12-factor), which is how the bundled Docker Compose stack points the same image at Postgres and MinIO without editing this file. Dedicated top-level `storage` and `event_bus` blocks, and a multi-engine `compute` block (`mode` / `parallelism`), are roadmap (v0.2+).

### 6.3 Declarative SQL DDL

Full DIT definition syntax:

```sql
CREATE DYNAMIC TABLE <catalog>.<database>.<table>
  [ PARTITION BY (<col>[, <col>]*) ]
  [ LAG = '<duration>' ]
  [ PARTITION_WINDOW = '<duration>' ]
  [ PARTITION_FRESHNESS = '<duration>' ]
  [ REFRESH_MODE = INCREMENTAL | FULL | TRIGGERED | SCHEDULED '<cron>' ]
  [ PRECONDITIONS (
      <precondition_function>(...) [, ...]
  ) ]
  [ DQ_RULES (
      <dq_rule_function>(...) [, ...]
  ) ]
  [ ON_DQ_FAIL ROUTE TO <quarantine_table> | FAIL | DROP ]
  [ TAGS ( key = 'value' [, ...] ) ]
  AS
  <select_statement>;
```

> **v0.1 implementation:** the parser recognizes `PARTITION BY`, `LAG`, `REFRESH_MODE` (`INCREMENTAL` | `FULL`), `PARTITION_WINDOW`, and `PARTITION_FRESHNESS`. The `PRECONDITIONS`, `DQ_RULES`, `ON_DQ_FAIL`, and `TAGS` clauses, plus the `TRIGGERED` and `SCHEDULED` refresh modes, are roadmap (v0.2+) and are not parsed yet. For working syntax, see `examples/quickstart/transformations/`.

Partition-aware options:

- `PARTITION BY (col)` — output table is created with Iceberg identity partitioning on these columns
- `PARTITION_WINDOW` — partitions older than `today - <duration>` are excluded from refresh; previous values are preserved
- `PARTITION_FRESHNESS` — maximum staleness for in-window partitions; a refresh fires if upstream changes OR if this deadline lapses

### 6.4 Python SDK

> **v0.1 implementation:** the example below is the target SDK. Today `floe` exports `Pipeline`, `DynamicTable`, `RefreshMode`, and `RefreshResult`; the supported entry point is `Pipeline.from_config("floe.yaml")` with `.refresh_all()`, `.refresh_one(name)`, and `.status()`. The `preconditions` and `dq` helpers, `pipeline.register()` / `pipeline.apply()`, and the DQ keyword arguments shown here are roadmap (v0.2+).

```python
from floe import Pipeline, DynamicTable, preconditions, dq

pipeline = Pipeline.from_config("floe.yaml")

silver_orders = DynamicTable(
    name="silver.orders",
    query="""
        SELECT o.order_id, o.amount, c.region
        FROM bronze.raw_orders o
        JOIN bronze.customers c ON o.customer_id = c.id
    """,
    lag="5 minutes",
    refresh_mode="incremental",
    preconditions=[
        preconditions.min_rows("bronze.raw_orders", count=1),
    ],
    dq_rules=[
        dq.not_null("order_id"),
        dq.range_check("amount", min=0.01),
    ],
    on_dq_fail="quarantine",
)

pipeline.register(silver_orders)
pipeline.apply()
```

Custom precondition:

```python
from floe.preconditions import precondition

@precondition
def weekly_completeness(spark, table: str, week: str) -> bool:
    count = spark.read.format("iceberg").load(table) \
        .filter(f"week = '{week}'") \
        .select("day").distinct().count()
    return count >= 7
```

### 6.5 CLI

```bash
# Shipped in v0.1. Every command accepts -c/--config (default: floe.yaml).
floe init my-pipeline           # scaffold a new project (floe.yaml + transformations/, warehouse/)
floe plan                       # validate DIT definitions and print the DAG
floe apply                      # refresh every DIT once, in dependency order
floe refresh silver.orders      # refresh a single DIT now
floe status                     # per-table status: exists, mode, lag, snapshot, checkpoint, upstream
floe dag                        # print the dependency DAG (ASCII)
floe watch                      # poll upstreams; refresh dependents on change (live dashboard)
floe watch --no-ui --poll 10    # headless: plain logs, custom poll interval (seconds)
floe version                    # print the Floe version
```

Shadow-mode deploy and promote, backfill (`reset-checkpoint`), and `logs` are
planned; see the [roadmap](#10-roadmap) (v0.4) and the operational patterns in
section 8.

---

## 7. Deployment Modes

Floe runs the same engine everywhere; only the catalog, object store, and
event source change between environments. **7.1 (Local) is what ships
today.** The Kubernetes and cloud profiles below are target designs tracked
on the [roadmap](#10-roadmap) (v0.3).

### 7.1 Local (Development)

Two ways to run Floe locally, both with no cloud dependencies.

**Plain process (no containers).** `floe init` scaffolds a project whose
`floe.yaml` uses a SQLite Iceberg catalog and a local-filesystem warehouse, so
the engine runs directly in your shell:

```bash
pip install -e .                  # or: pip install floe
floe init my-pipeline && cd my-pipeline
# add .sql DIT definitions under transformations/
floe plan                         # validate and show the DAG
floe apply                        # materialise every DIT once
floe watch                        # keep them fresh as upstreams change
```

**Full local stack (Docker Compose).** For a deployment that mirrors the cloud
(an S3-compatible object store, a Postgres-backed catalog, and a long-running
worker), use the bundled Compose stack from the **Run in Docker** section above:

```bash
cp .env.example .env
docker compose up --build                   # core: MinIO + Postgres + worker
docker compose --profile sql   up -d        # optional: browser SQL workbench (Trino + Metabase)
docker compose --profile ui    up -d dozzle # optional: container dashboard
docker compose --profile flink up           # optional: Flink on the same Iceberg tables
```

### 7.2 Kubernetes

Helm chart for production deployment. Bring your own object storage and event bus.

```bash
helm repo add floe https://charts.floe.dev
helm install floe floe/floe -f floe-values.yaml
```

### 7.3 Cloud Quick-Start Configs

**AWS:**
```yaml
storage:   { type: s3,    bucket: my-bucket }
catalog:   { type: glue,  region: us-east-1 }
event_bus: { type: eventbridge }
compute:   { engine: flink, mode: emr }
```

**Azure:**
```yaml
storage:   { type: adls,      account: myaccount, container: warehouse }
catalog:   { type: rest,      uri: https://my-polaris.azurewebsites.net }
event_bus: { type: eventhubs, namespace: my-ns }
compute:   { engine: flink,   mode: hdinsight }
```

**GCP:**
```yaml
storage:   { type: gcs,    bucket: my-bucket }
catalog:   { type: rest,   uri: https://my-catalog.run.app }
event_bus: { type: pubsub, project: my-project }
compute:   { engine: flink, mode: dataproc }
```

---

## 8. Operational Patterns

> **Target operational model (v0.4).** The patterns in this section describe
> where Floe is headed. The commands and integrations they use, shadow-mode
> `floe deploy` / `floe promote`, backfill `floe reset-checkpoint`, `floe diff`,
> and the pluggable metrics backends, are on the [roadmap](#10-roadmap), not in
> v0.1. Today, `floe status` and the `floe watch` dashboard already cover live
> refresh health and lag.

### 8.1 Shadow Mode Migration

Shadow mode enables risk-free migration from a legacy pipeline to a Floe DIT. The new pipeline runs in parallel; production traffic is only switched after validation.

```bash
# Deploy in shadow mode — writes to silver.orders_shadow, runs daily reconciliation
floe deploy --shadow silver.orders

# After N days of zero variance:
floe promote silver.orders   # atomically swaps read pointer; legacy pipeline stops
```

Reconciliation reports: row count diff, value diff (configurable columns), latency diff.

### 8.2 Backfill

```bash
floe reset-checkpoint silver.orders --from-beginning
floe reset-checkpoint silver.orders --from 2026-01-01
floe reset-checkpoint silver.orders --from-snapshot 8029341234
```

Backfill uses the exact same code path as incremental refresh — there is no separate backfill mode or job.

### 8.3 Schema Evolution

Iceberg handles schema evolution natively (add column, rename column, widen type, reorder). Floe adds a guardrail:

- `floe diff` detects upstream schema changes before they propagate
- Breaking changes (drop column, type narrowing) block downstream DITs and alert
- Non-breaking changes (add column, widen type) auto-propagate with a catalog version bump

### 8.4 Monitoring

Floe emits structured metrics to a pluggable observability backend:

| Metric | Description |
|--------|-------------|
| `floe.refresh.latency_ms` | Time from CommitEvent to downstream snapshot commit |
| `floe.refresh.lag_seconds` | Actual lag vs. configured LAG target |
| `floe.dq.quarantine_rate` | Fraction of rows routed to quarantine |
| `floe.precondition.skips` | Number of times a precondition blocked a refresh |
| `floe.dag.depth` | Number of hops in the longest DAG path |

Supported backends: Prometheus, AWS CloudWatch, Azure Monitor, GCP Cloud Monitoring, OpenTelemetry.

---

## 9. Comparison with Existing Projects

### 9.1 vs. Snowflake Dynamic Tables

| Dimension | Snowflake Dynamic Tables | Floe |
|-----------|--------------------------|------|
| Table format | Proprietary micro-partitions | Apache Iceberg (open) |
| Cloud | Snowflake only | Any / local |
| Preconditions | `TARGET_LAG` only | Full precondition DSL |
| DQ routing | Not built-in | First-class quarantine pattern |
| Lineage | Internal (not exported) | Explicit `_floe_*` columns in Iceberg |
| Cost model | Snowflake credits | Open source; pay for your own compute |

### 9.2 vs. Apache Paimon

Paimon is the closest streaming table format with event-driven capabilities, but it uses its own storage format (LSM-tree based). Tables stored in Paimon cannot be read directly by Trino, DuckDB, or standard Iceberg readers. Floe is pure Iceberg — any Iceberg-compatible reader works.

### 9.3 vs. Delta Live Tables (Databricks)

DLT provides a very similar declarative pipeline experience but is Databricks-exclusive. Moving off Databricks means rewriting all pipelines. Floe is vendor-neutral by design.

### 9.4 vs. RisingWave

RisingWave is a streaming database that can sink to Iceberg. It handles Iceberg as an output, not as the primary storage layer. Floe treats Iceberg as the system of record — both the input and the output — which enables full time-travel and snapshot lineage across the entire pipeline.

### 9.5 vs. DIY Flink + Iceberg

Building the same system yourself requires writing Flink jobs per transformation, building a custom orchestration layer, implementing idempotency and checkpoint management, writing DQ routing logic per job, and building a DAG planner for multi-hop cascades. Floe replaces all of that boilerplate so engineers write only the transformation logic.

---

## 10. Roadmap

### v0.1 — Foundation
- [x] Core DIT engine: DAG Planner, Refresh Executor, Catalog Manager
- [x] DuckDB reference compute backend (default, in-process)
- [x] Pluggable compute engine; opt-in Apache Flink engine for unpartitioned batch refresh (poll-triggered), writing the same Iceberg tables via the Flink SQL Gateway
- [x] Local deployment against SQLite Iceberg catalog
- [x] `INCREMENTAL` and `FULL` refresh modes
- [x] Partition-aware refresh (`PARTITION BY` + `PARTITION_WINDOW` + `PARTITION_FRESHNESS`)
- [x] Automatic `_floe_*` lineage column injection
- [x] CLI: `init`, `plan`, `apply`, `refresh`, `status`, `dag`, `watch`, `version`
- [x] Polling-based event detection (`floe watch`) — push-based hooks move to v0.2
- [x] Docker Compose local stack (MinIO warehouse + Postgres catalog + worker; opt-in Flink, SQL-workbench, and container-UI profiles)

### v0.2 — Production Compute, Event Bus & Data Quality
- [ ] Flink **streaming** engine (event-driven `push` trigger) + partitioned/windowed Flink refresh — the batch Flink engine (poll) already ships in v0.1
- [ ] Push-based event detection: Iceberg commit listeners + pluggable Event Bus (NATS / EventBridge / Event Hubs / Pub/Sub)
- [ ] `PRECONDITIONS` DSL (built-in functions + custom Python)
- [ ] `DQ_RULES` DSL
- [ ] Quarantine routing (`ON_DQ_FAIL ROUTE TO`)
- [ ] DQ alerting (threshold-based quarantine rate)

### v0.3 — Cloud Deployment
- [ ] Kubernetes Helm chart
- [ ] AWS config profile (EventBridge + S3 + Glue catalog)
- [ ] Azure config profile (Event Hubs + ADLS + Polaris)
- [ ] GCP config profile (Pub/Sub + GCS)

### v0.4 — Operations
- [ ] Shadow mode (`floe deploy --shadow`, `floe promote`)
- [ ] Backfill commands (`floe reset-checkpoint`)
- [ ] Schema evolution guardrails
- [ ] Prometheus / OpenTelemetry metrics

### v0.5 — Developer Experience
- [ ] `TRIGGERED` refresh mode (persistent Flink streaming job)
- [ ] Python SDK (full parity with SQL DDL)
- [ ] VS Code extension (DIT syntax highlighting, DAG preview)
- [ ] Instant local sandbox: spin up sample-data DITs in seconds (no CDK/CloudFormation), with the same DDL that runs in cloud
- [ ] AI-native authoring: natural-language → DIT DDL via MCP server / SDK, with DAG-aware validation against the live catalog

### v1.0 — Production Hardening
- [ ] Multi-catalog federation (query across catalogs in one DAG)
- [ ] Branch-based development (via Nessie catalog branching)
- [ ] Flink job failure recovery and retry policies
- [ ] Operational runbook and chaos testing suite

---

## 11. Open Questions

| Question | Notes |
|----------|-------|
| **Core language: Python or JVM?** | Python CLI for UX; Flink jobs are JVM. Control plane could be Python (easier contribution) or Kotlin (type-safe, JVM interop). Decision needed before v0.1. |
| **Catalog default for local mode** | REST catalog (via `iceberg-rest-image`) is simplest. Nessie adds branching but heavier. Start with REST, add Nessie in v0.4. |
| **Incremental aggregation correctness** | Partial aggregation merge (e.g., `SUM`, `COUNT`) is safe. `MEDIAN`, `DISTINCT COUNT` are not incrementally composable. Need a clear error message + automatic `FULL` fallback. |
| **Multi-hop cascade latency** | Deep DAGs (A → B → C → D) accumulate per-hop refresh latency. Need a maximum-cascade-depth config and a latency budget planner. |
| **Flink cold-start for short-lived jobs** | Flink job startup (~30s for standalone, ~2-4min for cloud-managed) limits how low `LAG` can go for non-`TRIGGERED` modes. Document this ceiling clearly. |
| **Licensing** | Apache 2.0 is the obvious choice for ecosystem compatibility (Iceberg, Flink are both Apache 2.0). |

---

## References

### Research Papers

- Zhu, R. et al. (2023). [**Streaming Democratized: How Snowflake Implements Dynamic Tables**](https://dl.acm.org/doi/epdf/10.1145/3589776). *ACM SIGMOD 2023.*
- Grover, A. et al. (2025). [**What's the Difference? Incremental Processing with Change Queries in Snowflake**](https://dl.acm.org/doi/epdf/10.1145/3722212.3724455). *ACM SIGMOD 2025.*

### Core Technologies

- [Apache Iceberg](https://iceberg.apache.org/) — open table format for huge analytic datasets
- [Apache Flink](https://flink.apache.org/) — stateful stream processing engine
- [Apache Flink × Iceberg connector](https://iceberg.apache.org/docs/latest/flink/) — Flink read/write support for Iceberg tables
- [Apache Paimon](https://paimon.apache.org/) — streaming lakehouse table format
- [Project Nessie](https://projectnessie.org/) — Git-like version control for data lakes
- [Apache Polaris](https://polaris.apache.org/) — open-source Iceberg catalog
- [NATS](https://nats.io/) — lightweight cloud-native messaging (local event bus)
- [MinIO](https://min.io/) — S3-compatible object storage (local deployment)

### Related Projects

- [RisingWave](https://risingwave.com/) — streaming database with Iceberg integration
- [Delta Live Tables](https://www.databricks.com/product/delta-live-tables) — declarative pipelines on Databricks
- [Snowflake Dynamic Tables](https://docs.snowflake.com/en/user-guide/dynamic-tables-intro) — the primary inspiration for this project

---

## License

Apache 2.0

---

*Floe is named for a sheet of floating ice — ice in motion. Small, flat, and fast.*
