"""Refresh executor — runs DIT queries via DuckDB and writes to Iceberg."""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta

import duckdb
import pyarrow as pa
from pyiceberg.expressions import GreaterThanOrEqual

from floe.catalog import CatalogManager
from floe.lineage import LINEAGE_COLUMNS, inject_lineage, new_job_run_id
from floe.models import DynamicTable, RefreshMode, RefreshResult


class RefreshExecutor:
    """Executes a DIT refresh: read upstream Iceberg → DuckDB SQL → write Iceberg."""

    def __init__(self, catalog_mgr: CatalogManager, all_dits: dict[str, DynamicTable]):
        self.catalog_mgr = catalog_mgr
        self.all_dits = all_dits

    def refresh(self, dit: DynamicTable) -> RefreshResult:
        started_at = datetime.now(UTC)
        job_run_id = new_job_run_id()

        if dit.is_partitioned:
            return self._refresh_partitioned(dit, started_at, job_run_id)
        return self._refresh_unpartitioned(dit, started_at, job_run_id)

    # --- unpartitioned (table-level) refresh -------------------------------

    def _refresh_unpartitioned(
        self, dit: DynamicTable, started_at: datetime, job_run_id: str
    ) -> RefreshResult:
        effective_mode = self._resolve_mode(dit)
        primary_upstream = self._primary_upstream(dit)
        upstream_snapshot_id = (
            self.catalog_mgr.current_snapshot_id(primary_upstream)
            if primary_upstream and self.catalog_mgr.table_exists(primary_upstream)
            else None
        )

        if effective_mode == RefreshMode.INCREMENTAL and primary_upstream:
            last_processed = (
                self.catalog_mgr.get_checkpoint(dit.name)
                if self.catalog_mgr.table_exists(dit.name)
                else None
            )
            if (
                upstream_snapshot_id is not None
                and last_processed is not None
                and upstream_snapshot_id == last_processed
            ):
                return RefreshResult(
                    table=dit.name,
                    mode=effective_mode,
                    rows_written=0,
                    input_snapshot_id=upstream_snapshot_id,
                    output_snapshot_id=self.catalog_mgr.current_snapshot_id(dit.name),
                    job_run_id=job_run_id,
                    started_at=started_at,
                    finished_at=datetime.now(UTC),
                    skipped=True,
                    skip_reason="upstream snapshot unchanged",
                )

        result_arrow = self._execute_query(dit, effective_mode, primary_upstream)
        result_with_lineage = inject_lineage(
            result_arrow,
            input_snapshot_id=upstream_snapshot_id,
            job_run_id=job_run_id,
            refresh_mode=effective_mode.value,
        )

        if not self.catalog_mgr.table_exists(dit.name):
            self.catalog_mgr.ensure_namespace(dit.namespace)
            self.catalog_mgr.catalog.create_table(dit.name, schema=result_with_lineage.schema)

        if effective_mode == RefreshMode.FULL:
            self.catalog_mgr.overwrite(dit.name, result_with_lineage)
        else:
            self.catalog_mgr.append(dit.name, result_with_lineage)

        if upstream_snapshot_id is not None:
            self.catalog_mgr.set_checkpoint(dit.name, upstream_snapshot_id)

        return RefreshResult(
            table=dit.name,
            mode=effective_mode,
            rows_written=result_with_lineage.num_rows,
            input_snapshot_id=upstream_snapshot_id,
            output_snapshot_id=self.catalog_mgr.current_snapshot_id(dit.name),
            job_run_id=job_run_id,
            started_at=started_at,
            finished_at=datetime.now(UTC),
        )

    # --- partitioned (window-aware) refresh --------------------------------

    def _refresh_partitioned(
        self, dit: DynamicTable, started_at: datetime, job_run_id: str
    ) -> RefreshResult:
        partition_col = dit.partition_by[0]

        primary_upstream = self._primary_upstream(dit)
        upstream_snap = (
            self.catalog_mgr.current_snapshot_id(primary_upstream)
            if primary_upstream and self.catalog_mgr.table_exists(primary_upstream)
            else None
        )

        table_exists = self.catalog_mgr.table_exists(dit.name)
        last_processed = self.catalog_mgr.get_checkpoint(dit.name) if table_exists else None
        last_window_refresh = (
            self.catalog_mgr.get_window_refreshed_at(dit.name) if table_exists else None
        )

        upstream_changed = upstream_snap is not None and last_processed != upstream_snap

        window_stale = False
        freshness_secs = dit.partition_freshness_seconds()
        if freshness_secs is not None:
            if last_window_refresh is None:
                window_stale = True
            else:
                age = (datetime.now(UTC) - last_window_refresh).total_seconds()
                window_stale = age > freshness_secs

        if table_exists and not upstream_changed and not window_stale:
            return RefreshResult(
                table=dit.name,
                mode=dit.refresh_mode,
                rows_written=0,
                input_snapshot_id=upstream_snap,
                output_snapshot_id=self.catalog_mgr.current_snapshot_id(dit.name),
                job_run_id=job_run_id,
                started_at=started_at,
                finished_at=datetime.now(UTC),
                skipped=True,
                skip_reason="upstream unchanged and window not stale",
            )

        cutoff = self._compute_window_cutoff(dit)

        # Compute the (possibly windowed) result.
        result_arrow = self._execute_query(
            dit, RefreshMode.FULL, primary_upstream, window_filter=(partition_col, cutoff)
        )
        result_with_lineage = inject_lineage(
            result_arrow,
            input_snapshot_id=upstream_snap,
            job_run_id=job_run_id,
            refresh_mode=dit.refresh_mode.value,
        )

        if not table_exists:
            self.catalog_mgr.create_partitioned_table(
                dit.name, result_with_lineage.schema, dit.partition_by
            )

        if cutoff is not None:
            self.catalog_mgr.overwrite_with_filter(
                dit.name,
                result_with_lineage,
                GreaterThanOrEqual(partition_col, cutoff.isoformat()),
            )
        else:
            self.catalog_mgr.overwrite(dit.name, result_with_lineage)

        if upstream_snap is not None:
            self.catalog_mgr.set_checkpoint(dit.name, upstream_snap)
        self.catalog_mgr.set_window_refreshed_at(dit.name, datetime.now(UTC))

        return RefreshResult(
            table=dit.name,
            mode=dit.refresh_mode,
            rows_written=result_with_lineage.num_rows,
            input_snapshot_id=upstream_snap,
            output_snapshot_id=self.catalog_mgr.current_snapshot_id(dit.name),
            job_run_id=job_run_id,
            started_at=started_at,
            finished_at=datetime.now(UTC),
        )

    def _compute_window_cutoff(self, dit: DynamicTable) -> date | None:
        secs = dit.partition_window_seconds()
        if secs is None:
            return None
        return (datetime.now(UTC) - timedelta(seconds=secs)).date()

    def _resolve_mode(self, dit: DynamicTable) -> RefreshMode:
        # If the output table doesn't exist yet, we must do a FULL build first.
        if not self.catalog_mgr.table_exists(dit.name):
            return RefreshMode.FULL
        return dit.refresh_mode

    def _primary_upstream(self, dit: DynamicTable) -> str | None:
        if not dit.upstream_tables:
            return None
        # MVP: pick the first upstream as primary for snapshot tracking.
        return dit.upstream_tables[0]

    def _execute_query(
        self,
        dit: DynamicTable,
        mode: RefreshMode,
        primary_upstream: str | None,
        *,
        window_filter: tuple[str, date | None] | None = None,
    ) -> pa.Table:
        """Execute the DIT's SQL query in DuckDB against current Iceberg snapshots.

        If ``window_filter`` is ``(column, cutoff_date)``, the cutoff is applied at
        two layers:

        1. **Upstream pushdown:** for any upstream whose schema contains
           ``column``, the Iceberg scan is given a ``column >= cutoff`` row
           filter so partition/file pruning happens before bytes are read.
        2. **Output guard:** the user's query is wrapped in
           ``SELECT * FROM (...) WHERE column >= cutoff`` to handle the case
           where ``column`` is synthesised by a projection (e.g.
           ``date_trunc('day', ts) AS column``) and is therefore not present in
           any upstream schema.
        """
        con = duckdb.connect(":memory:")
        con.execute("SET TimeZone='UTC'")

        try:
            for upstream in dit.upstream_tables:
                if not self.catalog_mgr.table_exists(upstream):
                    raise RuntimeError(
                        f"Upstream table {upstream!r} required by {dit.name!r} does not exist"
                    )

                row_filter = self._upstream_row_filter(upstream, window_filter)
                arrow_table = self._read_upstream(
                    upstream,
                    incremental=(mode == RefreshMode.INCREMENTAL and upstream == primary_upstream),
                    target_dit=dit.name,
                    row_filter=row_filter,
                )
                con.register(_view_alias(upstream), arrow_table)

            rewritten = _rewrite_table_names(dit.query, dit.upstream_tables)

            if window_filter is not None and window_filter[1] is not None:
                col, cutoff = window_filter
                rewritten = (
                    f"SELECT * FROM ({rewritten.rstrip(';').strip()}) AS _floe_inner "
                    f"WHERE {col} >= DATE '{cutoff.isoformat()}'"
                )

            return con.execute(rewritten).fetch_arrow_table()
        finally:
            con.close()

    def _upstream_row_filter(
        self,
        upstream: str,
        window_filter: tuple[str, date | None] | None,
    ):
        """Build an Iceberg row filter for the windowed slice of `upstream`.

        Returns ``None`` if the upstream's schema does not contain the partition
        column — in that case the column is synthesised by the user's query and
        the outer SQL guard takes over.
        """
        if window_filter is None or window_filter[1] is None:
            return None
        col, cutoff = window_filter
        table = self.catalog_mgr.load_table(upstream)
        if col not in table.schema().column_names:
            return None
        return GreaterThanOrEqual(col, cutoff.isoformat())

    def _read_upstream(
        self,
        name: str,
        *,
        incremental: bool,
        target_dit: str,
        row_filter=None,
    ) -> pa.Table:
        """Read an upstream Iceberg table, optionally incrementally and/or row-filtered."""
        table = self.catalog_mgr.load_table(name)
        current_snap = table.current_snapshot()
        if current_snap is None:
            return pa.table({})  # empty

        last_processed = (
            self.catalog_mgr.get_checkpoint(target_dit)
            if self.catalog_mgr.table_exists(target_dit)
            else None
        )

        scan_kwargs = {"row_filter": row_filter} if row_filter is not None else {}

        if incremental and last_processed and last_processed != current_snap.snapshot_id:
            try:
                return table.scan(**scan_kwargs).use_ref(current_snap.snapshot_id).to_arrow()
            except Exception:
                return table.scan(**scan_kwargs).to_arrow()
        return table.scan(**scan_kwargs).to_arrow()


def _view_alias(table_name: str) -> str:
    """DuckDB view alias for a fully-qualified Iceberg table."""
    return table_name.replace(".", "__")


def _rewrite_table_names(query: str, upstreams: list[str]) -> str:
    """Replace dotted upstream names in the SQL with DuckDB view aliases."""
    import re

    rewritten = query
    # Sort by length desc so longer names match first (avoid partial matches).
    for name in sorted(upstreams, key=len, reverse=True):
        alias = _view_alias(name)
        # Match the dotted name as a whole word (not surrounded by alphanumerics or _)
        pattern = r"(?<![\w.])" + re.escape(name) + r"(?![\w.])"
        rewritten = re.sub(pattern, alias, rewritten)
    return rewritten


def strip_lineage_columns(arrow: pa.Table) -> pa.Table:
    """Remove lineage cols (used by tests to compare logical content)."""
    cols = [c for c in arrow.column_names if c not in LINEAGE_COLUMNS]
    return arrow.select(cols)
