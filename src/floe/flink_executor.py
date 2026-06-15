"""Flink compute engine — runs a DIT's SQL on Apache Flink, writing Iceberg.

Floe's default engine is DuckDB (in-process). This engine instead submits the
transformation to a Flink cluster via its **SQL Gateway** REST API, computing the
table on Flink and writing the result back into the *same* Iceberg catalog Floe
manages. The Iceberg tables are plain and engine-agnostic, so a table can be read
or refreshed by either engine.

Scope (this release): unpartitioned DITs. Two execution shapes are supported:

* **Batch refresh** (``trigger: poll``): the watcher drives this engine unchanged
  because it implements the same ``refresh(dit, *, force=False) -> RefreshResult``
  contract as the DuckDB engine. A full ``INSERT OVERWRITE`` (or ``CREATE TABLE AS
  SELECT`` for a first run) recomputes and replaces the table.
* **Streaming push** (``trigger: push``): :meth:`FlinkExecutor.start_stream`
  submits one long-running Flink job that reads the upstream Iceberg table as a
  streaming source and appends new rows to the output as upstream commits land.
  Scoped to single-upstream, append-only transforms; partitioned, multi-source,
  or aggregating DITs raise :class:`StreamingNotSupportedError` (use ``poll``).

The four Floe lineage columns are appended as SQL literals in both shapes so the
output schema matches what the DuckDB engine produces.
"""

from __future__ import annotations

import json
import re
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

import sqlglot
from sqlglot import exp

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


class StreamingNotSupportedError(NotImplementedError):
    """Raised when a DIT's shape is not yet supported by the streaming push engine."""


@dataclass
class StreamHandle:
    """A submitted Flink streaming job that keeps a DIT fresh continuously."""

    table: str
    job_id: str | None
    job_run_id: str


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
        self._run(session, statement, wait_seconds=wait_seconds)

    def execute_collect(
        self, session: str, statement: str, *, wait_seconds: float
    ) -> list[list[Any]]:
        """Run one statement, block until it finishes, and return its first result page.

        Used for async DML (``table.dml-sync=false``): a streaming ``INSERT``
        finishes at *submission* and returns a single row holding the Flink job
        id, which the caller records to track the running job.
        """
        op = self._run(session, statement, wait_seconds=wait_seconds)
        body = self._request(
            "GET", f"/v1/sessions/{session}/operations/{op}/result/0?rowFormat=JSON"
        )
        results = body.get("results") or {}
        return [row.get("fields", []) for row in results.get("data", [])]

    def _run(self, session: str, statement: str, *, wait_seconds: float) -> str:
        """Submit a statement, poll to completion, and return the operation handle."""
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
                return op
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

    def start_stream(self, dit: DynamicTable) -> StreamHandle:
        """Submit a long-running Flink streaming job that keeps ``dit`` fresh.

        This is the ``push`` trigger: rather than polling and re-running a batch
        refresh, Floe submits one continuous Flink job that reads the upstream
        Iceberg table as a streaming source (monitoring it every
        ``compute.flink.monitor_interval``) and appends new rows to the output as
        upstream commits land. The job runs on the cluster independently of Floe.

        Scope (this release): single-upstream, append-only (no aggregation)
        transforms. Aggregating or multi-source streaming needs an upsert sink
        with a declared key (or stateful joins) and is on the roadmap; such DITs
        raise :class:`StreamingNotSupportedError`.
        """
        self._assert_streamable(dit)

        catalog_name = self.config.flink_catalog_name()
        namespace, table = dit.namespace, dit.table_name
        fq_target = f"{_quote_ident(catalog_name)}.{_quote_ident(namespace)}.{_quote_ident(table)}"
        job_run_id = new_job_run_id()

        # Lineage with a NULL input snapshot: a streaming job consumes a continuous
        # series of upstream commits, so no single input snapshot applies.
        select_sql = self._build_select(dit, None, job_run_id)

        if not self.catalog_mgr.table_exists(dit.name):
            self.catalog_mgr.ensure_namespace(namespace)
            self._create_empty_table(catalog_name, namespace, fq_target, select_sql)

        streaming_select = self._inject_streaming_hint(select_sql, dit.upstream_tables[0])
        job_id = self._submit_streaming_insert(
            catalog_name, namespace, fq_target, streaming_select
        )
        return StreamHandle(table=dit.name, job_id=job_id, job_run_id=job_run_id)

    def _assert_streamable(self, dit: DynamicTable) -> None:
        if dit.is_partitioned:
            raise StreamingNotSupportedError(
                f"streaming push does not yet support partitioned DITs ({dit.name!r}); "
                "use trigger: poll. Partitioned streaming is on the roadmap."
            )
        if len(dit.upstream_tables) != 1:
            raise StreamingNotSupportedError(
                f"streaming push currently supports a single upstream source; {dit.name!r} "
                f"reads {len(dit.upstream_tables)}. Multi-source (join) streaming is on the "
                "roadmap; use trigger: poll."
            )
        try:
            parsed = sqlglot.parse_one(dit.query, dialect="duckdb")
        except Exception as exc:  # noqa: BLE001 - surface as a streaming-scope error
            raise StreamingNotSupportedError(
                f"could not analyse {dit.name!r} for streaming: {exc}"
            ) from exc
        if parsed.find(exp.Group) is not None or parsed.find(exp.AggFunc) is not None:
            raise StreamingNotSupportedError(
                f"streaming push currently supports append-only transforms; {dit.name!r} "
                "aggregates (GROUP BY / aggregate function), which needs an upsert sink with a "
                "declared key. Use trigger: poll for aggregations; streaming upsert is on the "
                "roadmap."
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

    def _session_properties(
        self,
        mode: str = "batch",
        *,
        dml_sync: bool = True,
        dynamic_options: bool = False,
        checkpoint_interval: str | None = None,
    ) -> dict[str, str]:
        props = {
            "execution.target": "remote",
            "rest.address": self.flink.jobmanager_host,
            "rest.port": str(self.flink.jobmanager_port),
            "execution.runtime-mode": mode,
            "table.dml-sync": "true" if dml_sync else "false",
            "parallelism.default": str(self.flink.parallelism),
        }
        if dynamic_options:
            props["table.dynamic-table-options.enabled"] = "true"
        if checkpoint_interval:
            props["execution.checkpointing.interval"] = checkpoint_interval
        return props

    def _setup_statements(
        self,
        mode: str = "batch",
        *,
        dml_sync: bool = True,
        dynamic_options: bool = False,
        checkpoint_interval: str | None = None,
    ) -> list[str]:
        # Belt-and-braces: also issue the execution targeting as SET statements so
        # the session reliably submits to the remote cluster regardless of how the
        # gateway interprets create-session properties.
        stmts = [
            "SET 'execution.target' = 'remote'",
            f"SET 'rest.address' = '{self.flink.jobmanager_host}'",
            f"SET 'rest.port' = '{self.flink.jobmanager_port}'",
            f"SET 'execution.runtime-mode' = '{mode}'",
            f"SET 'table.dml-sync' = '{'true' if dml_sync else 'false'}'",
            f"SET 'parallelism.default' = '{self.flink.parallelism}'",
        ]
        if dynamic_options:
            stmts.append("SET 'table.dynamic-table-options.enabled' = 'true'")
        if checkpoint_interval:
            # The Iceberg sink only commits on checkpoints, so a streaming job must
            # checkpoint or it would buffer rows forever and never write output.
            stmts.append(f"SET 'execution.checkpointing.interval' = '{checkpoint_interval}'")
        return stmts

    def _create_empty_table(
        self, catalog_name: str, namespace: str, fq_target: str, select_sql: str
    ) -> None:
        """Create the streaming output table (schema only) via a 0-row batch CTAS."""
        statements = [
            *self._setup_statements("batch", dml_sync=True),
            self._create_catalog_statement(catalog_name),
            f"USE CATALOG {_quote_ident(catalog_name)}",
            f"CREATE DATABASE IF NOT EXISTS {_quote_ident(namespace)}",
            f"CREATE TABLE {fq_target} AS\n{select_sql}\nWHERE 1 = 0",
        ]
        wait = float(self.flink.statement_timeout_seconds)
        session = self.client.open_session(self._session_properties("batch", dml_sync=True))
        try:
            for stmt in statements:
                self.client.execute(session, stmt, wait_seconds=wait)
        finally:
            self.client.close_session(session)

    def _submit_streaming_insert(
        self, catalog_name: str, namespace: str, fq_target: str, streaming_select: str
    ) -> str | None:
        """Submit the continuous INSERT and return the Flink job id."""
        ckpt = self.flink.monitor_interval
        statements = [
            *self._setup_statements(
                "streaming", dml_sync=False, dynamic_options=True, checkpoint_interval=ckpt
            ),
            self._create_catalog_statement(catalog_name),
            f"USE CATALOG {_quote_ident(catalog_name)}",
            f"CREATE DATABASE IF NOT EXISTS {_quote_ident(namespace)}",
        ]
        wait = float(self.flink.statement_timeout_seconds)
        session = self.client.open_session(
            self._session_properties(
                "streaming", dml_sync=False, dynamic_options=True, checkpoint_interval=ckpt
            )
        )
        try:
            for stmt in statements:
                self.client.execute(session, stmt, wait_seconds=wait)
            insert = f"INSERT INTO {fq_target}\n{streaming_select}"
            rows = self.client.execute_collect(session, insert, wait_seconds=wait)
            return self._extract_job_id(rows)
        finally:
            self.client.close_session(session)

    @staticmethod
    def _extract_job_id(rows: list[list[Any]]) -> str | None:
        """Pull the 32-char hex Flink job id from an async INSERT's result row."""
        for row in rows:
            for field in row:
                if (
                    isinstance(field, str)
                    and len(field) == 32
                    and all(c in "0123456789abcdef" for c in field)
                ):
                    return field
        if rows and rows[0]:
            return str(rows[0][0])
        return None

    def _inject_streaming_hint(self, select_sql: str, upstream: str) -> str:
        """Attach Iceberg streaming-read OPTIONS to the upstream table reference.

        ``starting-strategy=INCREMENTAL_FROM_LATEST_SNAPSHOT`` means the job reacts
        only to upstream commits made *after* it starts (clean push semantics), so
        it neither backfills nor double-counts existing rows.
        """
        opts = (
            "'streaming'='true', "
            f"'monitor-interval'='{self.flink.monitor_interval}', "
            "'starting-strategy'='INCREMENTAL_FROM_LATEST_SNAPSHOT'"
        )
        hint = f" /*+ OPTIONS({opts}) */"
        pattern = re.compile(r"\b" + re.escape(upstream) + r"\b")
        new_sql, n = pattern.subn(upstream + hint, select_sql, count=1)
        if n == 0:
            raise StreamingNotSupportedError(
                f"could not locate upstream {upstream!r} in the query to enable streaming reads"
            )
        return new_sql

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
