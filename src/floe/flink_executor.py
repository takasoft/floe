"""Flink compute engine — runs a DIT's SQL on Apache Flink, writing Iceberg.

Floe's default engine is DuckDB (in-process). This engine instead submits the
transformation to a Flink cluster via its **SQL Gateway** REST API, computing the
table on Flink and writing the result back into the *same* Iceberg catalog Floe
manages. The Iceberg tables are plain and engine-agnostic, so a table can be read
or refreshed by either engine.

Scope (this release): unpartitioned DITs, batch refresh (``trigger: poll``). The
watcher drives this engine unchanged because it implements the same
``refresh(dit, *, force=False) -> RefreshResult`` contract as the DuckDB engine.
Partitioned/windowed DITs and the streaming ``push`` trigger are not yet handled
here and raise a clear error.

The SQL it generates mirrors the DuckDB engine's recompute-and-replace semantics:
a full ``INSERT OVERWRITE`` (or ``CREATE TABLE AS SELECT`` for a first run), with
the four Floe lineage columns appended as SQL literals so the output schema
matches what the DuckDB engine produces.
"""

from __future__ import annotations

import json
import time
import urllib.error
import urllib.request
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from floe.lineage import (
    INPUT_SNAPSHOT_COL,
    JOB_RUN_ID_COL,
    PROCESSED_AT_COL,
    REFRESH_MODE_COL,
    new_job_run_id,
)
from floe.models import DynamicTable, RefreshResult

if TYPE_CHECKING:
    from floe.catalog import CatalogManager
    from floe.config import FloeConfig


class FlinkExecutorError(RuntimeError):
    """Raised when a Flink SQL Gateway statement fails."""


class FlinkSQLGatewayClient:
    """Minimal client for the Flink SQL Gateway REST API (stdlib only).

    One gateway *session* is stateful: a ``CREATE CATALOG`` / ``USE CATALOG`` run
    in it persists for later statements. Statements are submitted one at a time;
    DML runs as a Flink job whose completion we wait for via the operation status.
    """

    def __init__(self, base_url: str, *, timeout: float = 30.0):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout

    def _request(self, method: str, path: str, body: dict | None = None) -> dict[str, Any]:
        url = f"{self.base_url}{path}"
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(url, data=data, method=method)
        req.add_header("Content-Type", "application/json")
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                payload = resp.read()
        except urllib.error.HTTPError as exc:  # 4xx/5xx carry a useful body
            detail = exc.read().decode(errors="replace")
            raise FlinkExecutorError(
                f"Flink SQL Gateway {method} {path} failed ({exc.code}): {detail}"
            ) from exc
        except urllib.error.URLError as exc:
            raise FlinkExecutorError(
                f"could not reach Flink SQL Gateway at {self.base_url} "
                f"({exc.reason}). Is the `flink` Compose profile up?"
            ) from exc
        return json.loads(payload) if payload else {}

    def info(self) -> dict[str, Any]:
        return self._request("GET", "/v1/info")

    def open_session(self, properties: dict[str, str]) -> str:
        resp = self._request("POST", "/v1/sessions", {"properties": properties})
        return resp["sessionHandle"]

    def close_session(self, session: str) -> None:
        try:
            self._request("DELETE", f"/v1/sessions/{session}")
        except FlinkExecutorError:
            pass  # best-effort cleanup

    def execute(self, session: str, statement: str, *, wait_seconds: float) -> None:
        """Run one statement and block until it finishes (or raise on error)."""
        resp = self._request(
            "POST", f"/v1/sessions/{session}/statements", {"statement": statement}
        )
        op = resp["operationHandle"]
        deadline = time.monotonic() + wait_seconds
        while True:
            status = self._request(
                "GET", f"/v1/sessions/{session}/operations/{op}/status"
            ).get("status")
            if status == "FINISHED":
                return
            if status in {"ERROR", "CANCELED", "CLOSED", "TIMEOUT"}:
                raise FlinkExecutorError(
                    f"statement failed (status={status}): {self._error_detail(session, op)}\n"
                    f"--- statement ---\n{statement}"
                )
            if time.monotonic() > deadline:
                raise FlinkExecutorError(
                    f"statement timed out after {wait_seconds}s (status={status})\n"
                    f"--- statement ---\n{statement}"
                )
            time.sleep(1.0)

    def _error_detail(self, session: str, op: str) -> str:
        try:
            body = self._request(
                "GET", f"/v1/sessions/{session}/operations/{op}/result/0?rowFormat=JSON"
            )
            return json.dumps(body.get("errors", body))[:2000]
        except FlinkExecutorError as exc:
            return str(exc)[:2000]


def _quote_ident(name: str) -> str:
    """Backtick-quote a Flink SQL identifier."""
    return "`" + name.replace("`", "``") + "`"


class FlinkExecutor:
    """Executes a DIT refresh on Flink, writing back to the shared Iceberg catalog."""

    def __init__(
        self,
        config: FloeConfig,
        catalog_mgr: CatalogManager,
        all_dits: dict[str, DynamicTable],
    ):
        self.config = config
        self.catalog_mgr = catalog_mgr
        self.all_dits = all_dits
        self.flink = config.compute.flink
        if self.flink is None:  # pragma: no cover - config guarantees this
            from floe.config import FlinkConfig

            self.flink = FlinkConfig()
        self.client = FlinkSQLGatewayClient(self.flink.sql_gateway_url)

    # --- public API (Executor protocol) ------------------------------------

    def refresh(self, dit: DynamicTable, *, force: bool = False) -> RefreshResult:
        if dit.is_partitioned:
            raise NotImplementedError(
                f"the flink engine does not yet support partitioned DITs ({dit.name!r}); "
                "use the duckdb engine for partitioned/windowed tables, or keep this DIT "
                "unpartitioned. Partitioned Flink refresh is on the roadmap."
            )

        started_at = datetime.now(UTC)
        job_run_id = new_job_run_id()

        primary_upstream = dit.upstream_tables[0] if dit.upstream_tables else None
        upstream_snapshot_id = (
            self.catalog_mgr.current_snapshot_id(primary_upstream)
            if primary_upstream and self.catalog_mgr.table_exists(primary_upstream)
            else None
        )

        table_exists = self.catalog_mgr.table_exists(dit.name)
        current_upstreams = self._current_upstream_snapshots(dit)

        if not force and table_exists:
            last_upstreams = self.catalog_mgr.get_upstream_checkpoints(dit.name)
            if last_upstreams is not None and last_upstreams == current_upstreams:
                return RefreshResult(
                    table=dit.name,
                    mode=dit.refresh_mode,
                    rows_written=0,
                    input_snapshot_id=upstream_snapshot_id,
                    output_snapshot_id=self.catalog_mgr.current_snapshot_id(dit.name),
                    job_run_id=job_run_id,
                    started_at=started_at,
                    finished_at=datetime.now(UTC),
                    skipped=True,
                    skip_reason="upstream snapshot unchanged",
                )

        self._run_flink_refresh(dit, upstream_snapshot_id, job_run_id, table_exists=table_exists)

        self.catalog_mgr.set_upstream_checkpoints(dit.name, current_upstreams)
        if upstream_snapshot_id is not None:
            self.catalog_mgr.set_checkpoint(dit.name, upstream_snapshot_id)

        return RefreshResult(
            table=dit.name,
            mode=dit.refresh_mode,
            rows_written=self._row_count(dit.name),
            input_snapshot_id=upstream_snapshot_id,
            output_snapshot_id=self.catalog_mgr.current_snapshot_id(dit.name),
            job_run_id=job_run_id,
            started_at=started_at,
            finished_at=datetime.now(UTC),
        )

    # --- internals ---------------------------------------------------------

    def _run_flink_refresh(
        self,
        dit: DynamicTable,
        upstream_snapshot_id: int | None,
        job_run_id: str,
        *,
        table_exists: bool,
    ) -> None:
        catalog_name = self.config.flink_catalog_name()
        namespace = dit.namespace
        table = dit.table_name
        fq_target = f"{_quote_ident(catalog_name)}.{_quote_ident(namespace)}.{_quote_ident(table)}"
        select_sql = self._build_select(dit, upstream_snapshot_id, job_run_id)

        if table_exists:
            write_stmt = f"INSERT OVERWRITE {fq_target}\n{select_sql}"
        else:
            self.catalog_mgr.ensure_namespace(namespace)
            write_stmt = f"CREATE TABLE {fq_target} AS\n{select_sql}"

        statements = [
            *self._setup_statements(),
            self._create_catalog_statement(catalog_name),
            f"USE CATALOG {_quote_ident(catalog_name)}",
            f"CREATE DATABASE IF NOT EXISTS {_quote_ident(namespace)}",
            write_stmt,
        ]

        wait = float(self.flink.statement_timeout_seconds)
        session = self.client.open_session(self._session_properties())
        try:
            for stmt in statements:
                self.client.execute(session, stmt, wait_seconds=wait)
        finally:
            self.client.close_session(session)

    def _session_properties(self) -> dict[str, str]:
        return {
            "execution.target": "remote",
            "rest.address": self.flink.jobmanager_host,
            "rest.port": str(self.flink.jobmanager_port),
            "execution.runtime-mode": "batch",
            "table.dml-sync": "true",
            "parallelism.default": str(self.flink.parallelism),
        }

    def _setup_statements(self) -> list[str]:
        # Belt-and-braces: also issue the execution targeting as SET statements so
        # the session reliably submits to the remote cluster regardless of how the
        # gateway interprets create-session properties.
        return [
            "SET 'execution.target' = 'remote'",
            f"SET 'rest.address' = '{self.flink.jobmanager_host}'",
            f"SET 'rest.port' = '{self.flink.jobmanager_port}'",
            "SET 'execution.runtime-mode' = 'batch'",
            "SET 'table.dml-sync' = 'true'",
            f"SET 'parallelism.default' = '{self.flink.parallelism}'",
        ]

    def _create_catalog_statement(self, catalog_name: str) -> str:
        props = self.config.flink_catalog_properties()
        with_lines = ",\n".join(f"  '{k}' = '{v}'" for k, v in props.items())
        return f"CREATE CATALOG {_quote_ident(catalog_name)} WITH (\n{with_lines}\n)"

    def _build_select(
        self, dit: DynamicTable, upstream_snapshot_id: int | None, job_run_id: str
    ) -> str:
        """Wrap the DIT query, appending the four lineage columns as literals.

        The user's SQL runs against the registered catalog, so its dotted upstream
        names (e.g. ``silver.deliveries_enriched``) resolve as database.table.
        """
        snap_literal = (
            "CAST(NULL AS BIGINT)"
            if upstream_snapshot_id is None
            else f"CAST({upstream_snapshot_id} AS BIGINT)"
        )
        inner = dit.query.rstrip().rstrip(";").strip()
        run_id = job_run_id.replace("'", "''")
        mode = dit.refresh_mode.value.replace("'", "''")
        lineage = (
            f"{snap_literal} AS {_quote_ident(INPUT_SNAPSHOT_COL)},\n"
            f"  CAST('{run_id}' AS STRING) AS {_quote_ident(JOB_RUN_ID_COL)},\n"
            f"  CAST(CURRENT_TIMESTAMP AS TIMESTAMP_LTZ(6)) AS {_quote_ident(PROCESSED_AT_COL)},\n"
            f"  CAST('{mode}' AS STRING) AS {_quote_ident(REFRESH_MODE_COL)}"
        )
        return f"SELECT\n  _floe_src.*,\n  {lineage}\nFROM (\n{inner}\n) AS _floe_src"

    def _current_upstream_snapshots(self, dit: DynamicTable) -> dict[str, int]:
        snaps: dict[str, int] = {}
        for upstream in dit.upstream_tables:
            if not self.catalog_mgr.table_exists(upstream):
                continue
            sid = self.catalog_mgr.current_snapshot_id(upstream)
            if sid is not None:
                snaps[upstream] = sid
        return snaps

    def _row_count(self, name: str) -> int:
        """Rows in the freshly written table (snapshot summary, else a scan)."""
        try:
            table = self.catalog_mgr.load_table(name)
            snap = table.current_snapshot()
            if snap is not None and snap.summary is not None:
                total = snap.summary.get("total-records")
                if total is not None:
                    return int(total)
            return table.scan().to_arrow().num_rows
        except Exception:  # noqa: BLE001 - row count is best-effort metadata
            return 0
