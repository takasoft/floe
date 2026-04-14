"""Refresh executor — runs DIT queries via DuckDB and writes to Iceberg."""

from __future__ import annotations

from datetime import datetime, timezone

import duckdb
import pyarrow as pa
from pyiceberg.catalog import Catalog

from floe.catalog import CatalogManager
from floe.lineage import LINEAGE_COLUMNS, inject_lineage, new_job_run_id
from floe.models import DynamicTable, RefreshMode, RefreshResult


class RefreshExecutor:
    """Executes a DIT refresh: read upstream Iceberg → DuckDB SQL → write Iceberg."""

    def __init__(self, catalog_mgr: CatalogManager, all_dits: dict[str, DynamicTable]):
        self.catalog_mgr = catalog_mgr
        self.all_dits = all_dits

    def refresh(self, dit: DynamicTable) -> RefreshResult:
        started_at = datetime.now(timezone.utc)
        job_run_id = new_job_run_id()

        # Determine effective refresh mode.
        effective_mode = self._resolve_mode(dit)

        # Pick the "primary" upstream snapshot to record on output rows.
        primary_upstream = self._primary_upstream(dit)
        upstream_snapshot_id = (
            self.catalog_mgr.current_snapshot_id(primary_upstream)
            if primary_upstream and self.catalog_mgr.table_exists(primary_upstream)
            else None
        )

        # Idempotency: if INCREMENTAL and primary upstream snapshot hasn't changed, skip.
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
                    finished_at=datetime.now(timezone.utc),
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

        # Create destination table if missing, using the result's schema.
        if not self.catalog_mgr.table_exists(dit.name):
            namespace = dit.namespace
            self.catalog_mgr.ensure_namespace(namespace)
            self.catalog_mgr.catalog.create_table(
                dit.name,
                schema=result_with_lineage.schema,
            )

        if effective_mode == RefreshMode.FULL:
            self.catalog_mgr.overwrite(dit.name, result_with_lineage)
        else:
            self.catalog_mgr.append(dit.name, result_with_lineage)

        if upstream_snapshot_id is not None:
            self.catalog_mgr.set_checkpoint(dit.name, upstream_snapshot_id)

        finished_at = datetime.now(timezone.utc)
        return RefreshResult(
            table=dit.name,
            mode=effective_mode,
            rows_written=result_with_lineage.num_rows,
            input_snapshot_id=upstream_snapshot_id,
            output_snapshot_id=self.catalog_mgr.current_snapshot_id(dit.name),
            job_run_id=job_run_id,
            started_at=started_at,
            finished_at=finished_at,
        )

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
    ) -> pa.Table:
        """Execute the DIT's SQL query in DuckDB against current Iceberg snapshots."""
        con = duckdb.connect(":memory:")
        # Force UTC so PyIceberg accepts the timestamp(tz=...) columns we read back.
        con.execute("SET TimeZone='UTC'")

        try:
            # Register every referenced table as an Arrow view in DuckDB.
            for upstream in dit.upstream_tables:
                if not self.catalog_mgr.table_exists(upstream):
                    raise RuntimeError(
                        f"Upstream table {upstream!r} required by {dit.name!r} does not exist"
                    )

                arrow_table = self._read_upstream(
                    upstream,
                    incremental=(
                        mode == RefreshMode.INCREMENTAL and upstream == primary_upstream
                    ),
                    target_dit=dit.name,
                )
                con.register(_view_alias(upstream), arrow_table)

            # Rewrite the query to use the registered views.
            rewritten = _rewrite_table_names(dit.query, dit.upstream_tables)

            return con.execute(rewritten).fetch_arrow_table()
        finally:
            con.close()

    def _read_upstream(self, name: str, *, incremental: bool, target_dit: str) -> pa.Table:
        """Read an upstream Iceberg table, optionally incrementally."""
        table = self.catalog_mgr.load_table(name)
        current_snap = table.current_snapshot()
        if current_snap is None:
            return pa.table({})  # empty

        last_processed = (
            self.catalog_mgr.get_checkpoint(target_dit)
            if self.catalog_mgr.table_exists(target_dit)
            else None
        )

        if incremental and last_processed and last_processed != current_snap.snapshot_id:
            try:
                arrow = (
                    table.scan()
                    .use_ref(current_snap.snapshot_id)
                    .to_arrow()
                )
                # Filter to rows added strictly after the last checkpoint snapshot.
                # PyIceberg incremental scan API surface area varies by version;
                # for the MVP we read the full current snapshot and emit-all-then-merge.
                # A future enhancement is to use append-scan APIs once available.
                return arrow
            except Exception:
                return table.scan().to_arrow()
        return table.scan().to_arrow()


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
