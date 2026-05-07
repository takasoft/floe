"""Floe Visualizable Demo — step-by-step walkthrough of a delivery analytics pipeline.

Illustrates how Floe:
  • ingests raw source data (the Bronze layer)
  • builds a dependency DAG from declarative SQL definitions
  • propagates changes through Silver → Gold in topological order
  • skips refreshes when upstream snapshots haven't changed (incremental mode)

Usage (from examples/quickstart/):
    python demo.py           # run (or continue) from the current catalog state
    python demo.py --reset   # drop all tables and start completely fresh
"""

from __future__ import annotations

import sys
import time
from pathlib import Path
from typing import Any

# Allow running directly without `pip install -e .`
_HERE = Path(__file__).parent.resolve()
_SRC = _HERE.parent.parent / "src"
sys.path.insert(0, str(_SRC))
sys.path.insert(0, str(_HERE))

import pyarrow as pa  # noqa: E402
import seed_sources  # noqa: E402  (local to quickstart/)
from rich import box  # noqa: E402
from rich.align import Align  # noqa: E402
from rich.console import Console  # noqa: E402
from rich.panel import Panel  # noqa: E402
from rich.rule import Rule  # noqa: E402
from rich.syntax import Syntax  # noqa: E402
from rich.table import Table as RichTable  # noqa: E402
from rich.tree import Tree  # noqa: E402

from floe.catalog import CatalogManager  # noqa: E402
from floe.config import FloeConfig  # noqa: E402
from floe.executor import strip_lineage_columns  # noqa: E402
from floe.models import RefreshResult  # noqa: E402
from floe.pipeline import Pipeline  # noqa: E402
from floe.planner import _layered_topological  # noqa: E402

CONFIG_PATH = _HERE / "floe.yaml"
SAMPLE_ROWS = 5

console = Console()


# ---------------------------------------------------------------------------
# Helper utilities
# ---------------------------------------------------------------------------

def _arrow_to_rich(arrow: pa.Table, title: str, max_rows: int = SAMPLE_ROWS) -> RichTable:
    """Render a PyArrow table as a Rich Table, capped at *max_rows* rows."""
    t = RichTable(title=title, box=box.SIMPLE_HEAD, border_style="dim", show_lines=False)
    for col in arrow.column_names:
        t.add_column(col, overflow="fold")
    sliced = arrow.slice(0, max_rows)
    for i in range(sliced.num_rows):
        t.add_row(*[str(sliced.column(j)[i].as_py()) for j in range(sliced.num_columns)])
    if arrow.num_rows > max_rows:
        remainder = f"  … {arrow.num_rows - max_rows} more rows"
        t.add_row(*([remainder] + [""] * (len(arrow.column_names) - 1)))
    return t


def _read_table(mgr: CatalogManager, name: str) -> pa.Table | None:
    if not mgr.table_exists(name):
        return None
    return strip_lineage_columns(mgr.load_table(name).scan().to_arrow())


# ---------------------------------------------------------------------------
# Demo sections
# ---------------------------------------------------------------------------

def show_header() -> None:
    console.print()
    console.print(Panel(
        Align.center(
            "[bold bright_white]Floe[/bold bright_white]  ·  "
            "[dim]Declarative Lakehouse Engine on Apache Iceberg[/dim]\n\n"
            "[dim]Event-driven  ·  Cloud-agnostic  ·  Stateless workers[/dim]"
        ),
        border_style="bright_blue",
        padding=(1, 6),
    ))
    console.print()
    console.print(
        "  This demo walks through a fictional [cyan]last-mile delivery analytics[/cyan] pipeline:\n\n"
        "    [cyan]Bronze[/cyan]  →  raw events from five source tables\n"
        "    [blue]Silver[/blue]  →  enriched fact tables (join with dimension data)\n"
        "    [yellow]Gold[/yellow]    →  business KPIs (per-DA, per-station, per-region)\n\n"
        "  Floe builds the dependency DAG automatically from the SQL definitions,\n"
        "  refreshes tables in topological order, and skips work when snapshots\n"
        "  haven't changed."
    )
    console.print()


def show_bronze(mgr: CatalogManager) -> None:
    console.print(Rule("[bold cyan]① Bronze Layer — Raw Source Data[/bold cyan]", style="cyan"))
    console.print()
    bronze_tables = [
        "bronze.delivery_associates",
        "bronze.routes",
        "bronze.raw_deliveries",
        "bronze.raw_safety_events",
        "bronze.delivery_defects",
    ]
    for name in bronze_tables:
        arrow = _read_table(mgr, name)
        if arrow is not None:
            console.print(_arrow_to_rich(arrow, f"[cyan]{name}[/cyan]"))
            console.print()


def show_dag(pipeline: Pipeline) -> None:
    console.print(Rule("[bold yellow]② Dependency DAG[/bold yellow]", style="yellow"))
    console.print()

    planner = pipeline.planner
    graph = planner.graph
    layers = _layered_topological(graph)

    def _style(name: str) -> str:
        ns = name.split(".")[0] if "." in name else "unknown"
        return {"bronze": "cyan", "silver": "blue", "gold": "yellow"}.get(ns, "white")

    root = Tree("[bold dim]Floe Pipeline[/bold dim]")

    ext_branch = root.add("[dim]External sources (Bronze — not managed by Floe)[/dim]")
    for s in sorted(planner.external_sources()):
        ext_branch.add(f"[dim]{s}[/dim]")

    managed_branch = root.add("[bold]Managed Dynamic Iceberg Tables (DITs)[/bold]")
    node_refs: dict[str, Any] = {}

    for level_nodes in layers:
        for name in sorted(level_nodes):
            dit = planner.dits[name]
            s = _style(name)
            preds = list(graph.predecessors(name))
            # For nodes with multiple predecessors, list all upstreams in the label
            # because Rich Tree is a tree (not a DAG) and can only show one parent.
            extra = ""
            if len(preds) > 1:
                extra = f"  [dim italic](also depends on: {', '.join(preds[1:])})[/dim italic]"
            label = f"[{s}]{name}[/{s}]  [dim]lag={dit.lag}  mode={dit.refresh_mode.value}[/dim]{extra}"
            parent = node_refs.get(preds[0]) if preds else None
            node_refs[name] = (parent if parent is not None else managed_branch).add(label)

    console.print(Align.left(root))
    console.print()


def run_transformation(
    pipeline: Pipeline,
    dit_name: str,
    step: int,
    total: int,
) -> RefreshResult:
    """Display one transformation step and run the refresh."""
    dit = pipeline.dits[dit_name]
    mgr = pipeline.catalog_mgr

    console.print()
    console.print(Rule(
        f"[white]Step {step}/{total} — [bold yellow]{dit_name}[/bold yellow][/white]",
        style="white",
    ))

    upstreams = dit.upstream_tables
    if upstreams:
        console.print(f"  Upstream : [dim]{', '.join(upstreams)}[/dim]")
    console.print(f"  Mode : [dim]{dit.refresh_mode.value}[/dim]  Lag : [dim]{dit.lag}[/dim]")

    # ── Input samples ──────────────────────────────────────────────────────
    if upstreams:
        console.print()
        console.print("  [bold]Inputs — sample rows from upstream tables:[/bold]")
        for up in upstreams:
            arrow = _read_table(mgr, up)
            if arrow is not None:
                console.print(_arrow_to_rich(arrow, f"  ↑ {up}", max_rows=3))
        console.print()

    # ── SQL ────────────────────────────────────────────────────────────────
    console.print("  [bold]Transformation query:[/bold]")
    sql_src = Path(dit.source_path).read_text() if dit.source_path else dit.query
    console.print(Panel(
        Syntax(sql_src.strip(), "sql", theme="monokai", line_numbers=False),
        border_style="green",
        padding=(0, 1),
    ))

    # ── Refresh ────────────────────────────────────────────────────────────
    console.print()
    console.print("  ⟳  Running refresh …", end="", style="dim")
    t0 = time.monotonic()
    result = pipeline.executor.refresh(dit)
    ms = (time.monotonic() - t0) * 1000

    if result.skipped:
        console.print(f" [yellow]skipped[/yellow]  ({result.skip_reason})")
    else:
        console.print(f" [green]✓  {result.rows_written} rows  ·  {ms:.0f} ms[/green]")

    # ── Output sample ──────────────────────────────────────────────────────
    if not result.skipped:
        arrow = _read_table(mgr, dit_name)
        if arrow is not None:
            console.print()
            console.print("  [bold]Output — sample rows written to Iceberg:[/bold]")
            console.print(_arrow_to_rich(arrow, f"  ↓ {dit_name}"))

    return result


def show_summary(results: list[RefreshResult], pipeline: Pipeline) -> None:
    console.print()
    console.print(Rule("[bold green]③ Pipeline Complete — Summary[/bold green]", style="green"))
    console.print()

    t = RichTable(title="Refresh Results", box=box.ROUNDED, border_style="green")
    t.add_column("Table", style="bold")
    t.add_column("Mode")
    t.add_column("Rows written", justify="right")
    t.add_column("Out snapshot", justify="right", style="dim")
    t.add_column("Duration (ms)", justify="right", style="dim")
    t.add_column("Status")
    for r in results:
        t.add_row(
            r.table,
            r.mode.value,
            "-" if r.skipped else str(r.rows_written),
            str(r.output_snapshot_id) if r.output_snapshot_id else "-",
            str(r.duration_ms),
            "[yellow]skipped[/yellow]" if r.skipped else "[green]✓  ok[/green]",
        )
    console.print(t)
    console.print()

    console.print("[bold]Gold layer — final business KPIs:[/bold]")
    console.print()
    for name in pipeline.planner.topological_order():
        if not name.startswith("gold"):
            continue
        arrow = _read_table(pipeline.catalog_mgr, name)
        if arrow is not None:
            console.print(_arrow_to_rich(arrow, f"[yellow]{name}[/yellow]"))
            console.print()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main(*, reset: bool = False) -> None:
    show_header()

    config = FloeConfig.load(CONFIG_PATH)
    mgr = CatalogManager.from_config(
        name=config.project,
        catalog_type=config.catalog.type,
        uri=config.resolved_catalog_uri(),
        warehouse=config.resolved_warehouse(),
    )

    if reset:
        console.print("[yellow]Resetting: dropping all managed tables …[/yellow]")
        for ns in ["gold", "silver", "bronze"]:
            for tbl in mgr.list_tables(ns):
                mgr.drop_table_if_exists(tbl)
                console.print(f"  [dim]dropped {tbl}[/dim]")
        console.print()

    # Seed bronze source tables
    console.print("[dim]Seeding bronze source tables …[/dim]")
    seed_sources.main()
    console.print()

    # ① Bronze
    show_bronze(mgr)

    # ② DAG
    pipeline = Pipeline.from_config(CONFIG_PATH)
    show_dag(pipeline)

    # ③ Transformation walkthrough
    console.print(Rule("[bold]Transformation Walkthrough[/bold]"))
    topo = pipeline.planner.topological_order()
    console.print(
        f"  [dim]{len(topo)} DITs will run in topological order "
        f"(upstream always refreshed before downstream)[/dim]"
    )

    results: list[RefreshResult] = []
    for i, dit_name in enumerate(topo, 1):
        r = run_transformation(pipeline, dit_name, step=i, total=len(topo))
        results.append(r)

    # ④ Summary
    show_summary(results, pipeline)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Floe visualizable demo")
    parser.add_argument(
        "--reset",
        action="store_true",
        help="Drop all Iceberg tables and re-seed from scratch",
    )
    args = parser.parse_args()
    main(reset=args.reset)
