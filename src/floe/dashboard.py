"""Rich Live dashboard for `floe watch`.

Renders the pipeline DAG with per-DIT status and a rolling event log,
updated in real time as the watcher detects upstream changes and
refreshes downstream DITs.

Color scheme follows the README flowchart: bronze=cyan, silver=blue,
gold=yellow.
"""

from __future__ import annotations

import time
from collections import deque
from dataclasses import dataclass
from datetime import datetime

from rich.console import Group
from rich.live import Live
from rich.panel import Panel
from rich.table import Table as RichTable
from rich.text import Text
from rich.tree import Tree

from floe.models import RefreshResult
from floe.pipeline import Pipeline
from floe.watcher import Watcher

MAX_LOG_LINES = 12
SNAPSHOT_DIGITS = 8  # show only the last N digits — full IDs are unwieldy
FLASH_DURATION_S = 2.5  # seconds to keep a "just changed" highlight on a row
RECENT_ROWS_PER_SOURCE = 3  # how many tail rows to show per changed bronze table


def _ns_color(name: str) -> str:
    ns = name.split(".", 1)[0] if "." in name else ""
    return {"bronze": "cyan", "silver": "blue", "gold": "yellow"}.get(ns, "white")


def _short_snap(snap: int | None) -> str:
    if snap is None:
        return "—"
    s = str(snap)
    return s if len(s) <= SNAPSHOT_DIGITS else "…" + s[-SNAPSHOT_DIGITS:]


@dataclass
class TableState:
    name: str
    snapshot_id: int | None = None
    last_refresh_time: datetime | None = None
    last_refresh_ms: int | None = None
    last_rows_written: int | None = None
    status: str = "idle"  # idle | refreshing | fresh
    flash_until: float = 0.0  # epoch seconds; until this time, render highlighted


class WatcherDashboard:
    """Rich Live dashboard for the Floe watcher."""

    def __init__(self, pipeline: Pipeline, watcher: Watcher, poll_interval: float):
        self.pipeline = pipeline
        self.watcher = watcher
        self.poll_interval = poll_interval
        self.events: deque[Text] = deque(maxlen=MAX_LOG_LINES)
        self.tables: dict[str, TableState] = {}
        # Most recently observed tail rows per changed external source. Populated
        # in on_change so the UI can show "what actually landed in bronze."
        self.recent_activity: dict[str, object] = {}  # source_name -> pa.Table
        # Lineage chain (refreshed DIT -> primary upstream -> ...) built from the
        # `_floe_input_snapshot_id` column after each successful refresh.
        self.last_lineage_chain: list[tuple[str, int | None]] = []
        for src in pipeline.planner.external_sources():
            self.tables[src] = TableState(name=src)
        for dit in pipeline.planner.topological_order():
            self.tables[dit] = TableState(name=dit)

    # ---- event hooks ---------------------------------------------------

    def log(self, markup: str) -> None:
        ts = datetime.now().strftime("%H:%M:%S")
        self.events.append(Text.from_markup(f"[dim]{ts}[/dim] ") + Text.from_markup(markup))

    def on_init(self) -> None:
        for src, snap in self.watcher.last_seen.items():
            st = self.tables.setdefault(src, TableState(name=src))
            st.snapshot_id = snap
            self.log(f"watching [cyan]{src}[/cyan]  snap=[dim]{_short_snap(snap)}[/dim]")
        missing: list[str] = []
        for dit in self.pipeline.planner.topological_order():
            try:
                if self.pipeline.catalog_mgr.table_exists(dit):
                    snap = self.pipeline.catalog_mgr.current_snapshot_id(dit)
                    self.tables[dit].snapshot_id = snap
                    self.tables[dit].status = "fresh"
                else:
                    missing.append(dit)
            except Exception:  # noqa: BLE001 -- catalog drivers throw varied types
                pass
        if missing:
            self.log(
                f"[yellow]warning:[/yellow] {len(missing)} DIT(s) not yet materialized — "
                f"run `floe apply` once to bootstrap, otherwise refreshes that "
                f"depend on them will fail."
            )

    def on_change(self, sources: set[str]) -> None:
        for src in sources:
            st = self.tables.setdefault(src, TableState(name=src))
            new_snap = self.watcher.last_seen.get(src)
            st.snapshot_id = new_snap
            st.flash_until = time.time() + FLASH_DURATION_S
            tail = self._read_tail(src, limit=RECENT_ROWS_PER_SOURCE)
            if tail is not None:
                self.recent_activity[src] = tail
        names = ", ".join(f"[cyan]{s}[/cyan]" for s in sorted(sources))
        self.log(f"[bold yellow]change detected[/bold yellow] on {names}")

    def _read_tail(self, source_name: str, *, limit: int):
        """Read the most recently appended rows from ``source_name``.

        Returns the last ``limit`` rows in Iceberg scan order — for tables that
        are append-only (the demo's bronze tables) this matches "latest by
        insertion." Returns ``None`` on any read error so dashboard rendering
        never fails because of a transient catalog read.
        """
        try:
            if not self.pipeline.catalog_mgr.table_exists(source_name):
                return None
            ib_table = self.pipeline.catalog_mgr.load_table(source_name)
            arrow = ib_table.scan().to_arrow()
            if arrow.num_rows == 0:
                return None
            start = max(0, arrow.num_rows - limit)
            return arrow.slice(start, arrow.num_rows - start)
        except Exception:  # noqa: BLE001 -- catalog drivers throw varied types
            return None

    def on_refresh_start(self, dit_name: str) -> None:
        st = self.tables.setdefault(dit_name, TableState(name=dit_name))
        st.status = "refreshing"
        col = _ns_color(dit_name)
        self.log(f"refreshing [{col}]{dit_name}[/{col}]…")

    def on_refresh_done(self, dit_name: str, result: RefreshResult) -> None:
        st = self.tables.setdefault(dit_name, TableState(name=dit_name))
        st.flash_until = time.time() + FLASH_DURATION_S
        col = _ns_color(dit_name)
        if result.skipped:
            # Skipped means "nothing to do" — leave the last_* fields pointing
            # at the most recent successful refresh so the DAG tree doesn't
            # regress to "0 rows · 0 ms."
            st.status = "fresh"
            self.log(f"[yellow]skipped[/yellow] [{col}]{dit_name}[/{col}] · {result.skip_reason}")
        else:
            st.status = "fresh"
            st.last_refresh_time = datetime.now()
            st.last_refresh_ms = result.duration_ms
            st.last_rows_written = result.rows_written
            st.snapshot_id = result.output_snapshot_id or st.snapshot_id
            self.log(
                f"[green]✓[/green] [{col}]{dit_name}[/{col}]  "
                f"{result.rows_written} rows · {result.duration_ms} ms"
            )
            chain = self._build_lineage_chain(dit_name)
            if chain:
                self.last_lineage_chain = chain

    def on_refresh_error(self, dit_name: str, exc: BaseException) -> None:
        st = self.tables.setdefault(dit_name, TableState(name=dit_name))
        st.status = "error"
        col = _ns_color(dit_name)
        # Keep the message short — full traceback goes to the logger
        msg = str(exc).splitlines()[0] if str(exc) else type(exc).__name__
        self.log(f"[red]✗ refresh failed[/red] [{col}]{dit_name}[/{col}] · {msg}")

    # ---- rendering -----------------------------------------------------

    def _format_node(self, name: str, st: TableState) -> str:
        col = _ns_color(name)
        if st.status == "refreshing":
            dot = "[bold yellow]●[/bold yellow]"
        elif st.status == "error":
            dot = "[bold red]●[/bold red]"
        elif st.snapshot_id is not None:
            dot = "[green]●[/green]"
        else:
            dot = "[dim]○[/dim]"
        is_flashing = time.time() < st.flash_until
        name_token = (
            f"[reverse {col}]{name}[/reverse {col}]" if is_flashing else f"[{col}]{name}[/{col}]"
        )
        snap_token = f"[dim]snap={_short_snap(st.snapshot_id)}[/dim]"
        recency = ""
        if st.last_refresh_time:
            ago = (datetime.now() - st.last_refresh_time).total_seconds()
            recency = (
                f"  [dim]· refreshed {ago:.0f}s ago"
                f" · {st.last_rows_written} rows · {st.last_refresh_ms} ms[/dim]"
            )
        deps = ""
        if name in self.pipeline.dits:
            upstreams = self.pipeline.dits[name].upstream_tables
            if upstreams:
                deps = f"  [dim]← from {', '.join(upstreams)}[/dim]"
        return f"{dot} {name_token}  {snap_token}{recency}{deps}"

    def _build_lineage_chain(self, dit_name: str) -> list[tuple[str, int | None]]:
        """Trace where the latest row of ``dit_name`` came from.

        Walks the primary-upstream chain, reading the ``_floe_input_snapshot_id``
        column at each hop until it reaches an external source (which has no
        lineage column). Returns ``[(table, snapshot_id), ...]`` from the
        refreshed DIT down to its bronze root. Returns ``[]`` if any read fails.
        """
        chain: list[tuple[str, int | None]] = []
        current = dit_name
        next_snap: int | None = None
        visited: set[str] = set()
        for _hop in range(5):  # cap depth so a cycle can't run away
            if current in visited:
                break
            visited.add(current)
            try:
                if not self.pipeline.catalog_mgr.table_exists(current):
                    break
                ib_table = self.pipeline.catalog_mgr.load_table(current)
                cur_snap = (
                    next_snap
                    if next_snap is not None
                    else (
                        ib_table.current_snapshot().snapshot_id
                        if ib_table.current_snapshot()
                        else None
                    )
                )
                chain.append((current, cur_snap))

                # External sources have no lineage column — stop here.
                if current not in self.pipeline.dits:
                    break
                arrow = ib_table.scan().to_arrow()
                if arrow.num_rows == 0 or "_floe_input_snapshot_id" not in arrow.column_names:
                    break
                input_snap = arrow["_floe_input_snapshot_id"][-1].as_py()
                upstreams = self.pipeline.dits[current].upstream_tables
                if not upstreams:
                    break
                current = upstreams[0]  # primary upstream
                next_snap = input_snap
            except Exception:  # noqa: BLE001 -- catalog drivers throw varied types
                break
        return chain

    def __rich__(self) -> Group:
        return self._render()

    def _render(self) -> Group:
        n_sources = len(self.pipeline.planner.external_sources())
        n_dits = len(self.pipeline.dits)
        header = Panel(
            Text.from_markup(
                f"[bold]Floe Watcher[/bold]  ·  "
                f"polling every {self.poll_interval}s  ·  "
                f"watching {n_sources} sources, {n_dits} DITs  ·  "
                f"[dim]Ctrl+C to stop[/dim]"
            ),
            border_style="bright_blue",
            padding=(0, 2),
        )
        tree = Tree("[bold]Pipeline DAG[/bold]", guide_style="dim")
        ext_branch = tree.add("[dim]External sources (watched)[/dim]")
        for src in sorted(self.pipeline.planner.external_sources()):
            st = self.tables.setdefault(src, TableState(name=src))
            ext_branch.add(self._format_node(src, st))
        managed_branch = tree.add("[bold]Managed Dynamic Iceberg Tables[/bold]")
        for dit in self.pipeline.planner.topological_order():
            st = self.tables.setdefault(dit, TableState(name=dit))
            managed_branch.add(self._format_node(dit, st))
        dag_panel = Panel(tree, title="DAG", border_style="white", padding=(0, 1))
        activity_panel = self._render_activity_panel()
        lineage_panel = self._render_lineage_panel()
        if not self.events:
            evt_content = Text.from_markup("[dim]Waiting for upstream changes…[/dim]")
        else:
            evt_content = Text("\n").join(self.events)
        evt_panel = Panel(evt_content, title="Events", border_style="blue", padding=(0, 1))
        return Group(header, dag_panel, activity_panel, lineage_panel, evt_panel)

    def _render_lineage_panel(self) -> Panel:
        if not self.last_lineage_chain:
            return Panel(
                Text.from_markup(
                    "[dim]Lineage trace will appear here after the first refresh — "
                    "every row carries its upstream snapshot ID.[/dim]"
                ),
                title="Lineage trace (latest refreshed row)",
                border_style="magenta",
                padding=(0, 1),
            )
        rows: list = []
        for i, (table, snap) in enumerate(self.last_lineage_chain):
            col = _ns_color(table)
            connector = "  [magenta]←[/magenta] " if i > 0 else "    "
            rows.append(
                Text.from_markup(
                    f"{connector}[{col}]{table}[/{col}]  [dim]@ snap {_short_snap(snap)}[/dim]"
                )
            )
        return Panel(
            Group(*rows),
            title="Lineage trace (latest refreshed row)",
            border_style="magenta",
            padding=(0, 1),
        )

    def _render_activity_panel(self) -> Panel:
        if not self.recent_activity:
            return Panel(
                Text.from_markup("[dim]Waiting for upstream changes…[/dim]"),
                title="Recent Upstream Activity",
                border_style="cyan",
                padding=(0, 1),
            )
        sections: list = []
        for src, arrow in self.recent_activity.items():
            col = _ns_color(src)
            sections.append(Text.from_markup(f"[bold {col}]{src}[/bold {col}]"))
            t = RichTable(
                show_header=True,
                header_style="dim",
                box=None,
                pad_edge=False,
                padding=(0, 1),
            )
            for c in arrow.column_names:
                t.add_column(c, style="bright_white", overflow="fold")
            for i in range(arrow.num_rows):
                cells = []
                for c in arrow.column_names:
                    v = arrow[c][i].as_py()
                    if isinstance(v, datetime):
                        cells.append(v.strftime("%m-%d %H:%M:%S"))
                    elif v is None:
                        cells.append("—")
                    else:
                        cells.append(str(v))
                t.add_row(*cells)
            sections.append(t)
        return Panel(
            Group(*sections),
            title="Recent Upstream Activity",
            border_style="cyan",
            padding=(0, 1),
        )


def run_dashboard(pipeline: Pipeline, watcher: Watcher, poll_interval: float) -> None:
    """Run the watcher with a live Rich dashboard. Blocks until Ctrl+C."""
    dashboard = WatcherDashboard(pipeline, watcher, poll_interval)
    watcher._initialize()
    dashboard.on_init()

    with Live(dashboard, refresh_per_second=4, screen=False):
        try:
            iter_count = 0
            while True:
                time.sleep(poll_interval)
                watcher.step(
                    on_change=dashboard.on_change,
                    on_refresh_start=dashboard.on_refresh_start,
                    on_refresh_done=dashboard.on_refresh_done,
                    on_refresh_error=dashboard.on_refresh_error,
                )
                iter_count += 1
                if watcher.config.max_iterations and iter_count >= watcher.config.max_iterations:
                    break
        except KeyboardInterrupt:
            dashboard.log("[red]stopped by user[/red]")
