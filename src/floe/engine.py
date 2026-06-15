"""Engine selection — map ``compute.engine`` to a refresh ``Executor``.

Floe separates two concerns:

* **Engine** — *who* runs a transformation. Implementations satisfy the
  :class:`Executor` protocol (``refresh(dit, *, force=False) -> RefreshResult``):
  :class:`floe.executor.DuckDBExecutor` (default, in-process batch) and
  :class:`floe.flink_executor.FlinkExecutor` (submits SQL to a Flink cluster).
* **Trigger** — *how* a refresh is initiated (poll vs push). That axis lives in
  the watcher/runner, not here.

Because both executors expose the same ``refresh`` method, the polling watcher
drives either engine unchanged; only the streaming ``push`` trigger needs the
Flink engine specifically.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol, runtime_checkable

from floe.models import DynamicTable, RefreshResult

if TYPE_CHECKING:
    from floe.catalog import CatalogManager
    from floe.config import FloeConfig


@runtime_checkable
class Executor(Protocol):
    """Structural interface every compute engine implements."""

    def refresh(self, dit: DynamicTable, *, force: bool = False) -> RefreshResult: ...


def make_executor(
    config: FloeConfig,
    catalog_mgr: CatalogManager,
    dits: dict[str, DynamicTable],
) -> Executor:
    """Build the executor for the configured ``compute.engine``.

    Imports the engine implementation lazily so that, for example, a DuckDB-only
    install never imports the Flink executor (and vice versa).
    """
    engine = config.compute.engine
    if engine == "duckdb":
        from floe.executor import DuckDBExecutor

        return DuckDBExecutor(catalog_mgr, dits)
    if engine == "flink":
        from floe.flink_executor import FlinkExecutor

        return FlinkExecutor(config, catalog_mgr, dits)
    # config validation should prevent reaching here.
    raise ValueError(f"no executor registered for compute engine {engine!r}")
