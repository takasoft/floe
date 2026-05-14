"""Polling-based event detector for v0.1.

Subscribes (via polling) to the Iceberg catalog. When an upstream table's
current snapshot ID changes, identifies downstream DITs via the planner
and triggers refresh in topological order. Single-process, in-memory state.

v0.2 will replace this with a push-based event bus + catalog commit hooks.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable
from dataclasses import dataclass

from floe.models import RefreshResult
from floe.pipeline import Pipeline

log = logging.getLogger(__name__)

# Observer callback types
OnChange = Callable[[set[str]], None]
OnRefreshStart = Callable[[str], None]
OnRefreshDone = Callable[[str, RefreshResult], None]
OnRefreshError = Callable[[str, BaseException], None]


@dataclass
class WatchConfig:
    poll_interval_seconds: float = 10.0
    quiet_period_seconds: float = 2.0  # debounce after detecting a change
    max_iterations: int | None = None  # for tests; None = run forever


class Watcher:
    """Polls external upstream tables; refreshes affected DITs when snapshots change."""

    def __init__(self, pipeline: Pipeline, config: WatchConfig | None = None):
        self.pipeline = pipeline
        self.config = config or WatchConfig()
        self.last_seen: dict[str, int | None] = {}

    def _current_snapshot_id(self, table_name: str) -> int | None:
        try:
            if not self.pipeline.catalog_mgr.table_exists(table_name):
                return None
            return self.pipeline.catalog_mgr.current_snapshot_id(table_name)
        except Exception as exc:  # noqa: BLE001 -- catalog drivers throw varied types
            log.warning("could not read snapshot for %s: %s", table_name, exc)
            return None

    def _initialize(self) -> None:
        for src in self.pipeline.planner.external_sources():
            self.last_seen[src] = self._current_snapshot_id(src)
            log.info("watching %s @ snapshot %s", src, self.last_seen[src])

    def _detect_changes(self) -> set[str]:
        changed: set[str] = set()
        for src, prior in list(self.last_seen.items()):
            current = self._current_snapshot_id(src)
            if current is not None and current != prior:
                changed.add(src)
                self.last_seen[src] = current
        return changed

    def _affected_dits(self, changed_sources: set[str]) -> list[str]:
        """Return DITs (transitive) whose upstreams include any changed external
        source, in topological order. External sources are not graph nodes in
        the planner, so we find direct dependents by scanning DIT.upstream_tables
        and then expand via planner.downstream_of() for transitive descendants."""
        planner = self.pipeline.planner
        directly_affected: set[str] = set()
        for dit_name, dit in planner.dits.items():
            if any(src in dit.upstream_tables for src in changed_sources):
                directly_affected.add(dit_name)
        all_affected: set[str] = set(directly_affected)
        for dit_name in directly_affected:
            all_affected.update(planner.downstream_of(dit_name))
        return [n for n in planner.topological_order() if n in all_affected]

    def step(
        self,
        *,
        on_change: OnChange | None = None,
        on_refresh_start: OnRefreshStart | None = None,
        on_refresh_done: OnRefreshDone | None = None,
        on_refresh_error: OnRefreshError | None = None,
    ) -> set[str]:
        """Run one polling iteration (no sleep). Returns changed sources, post-debounce.

        Observer callbacks let external listeners (e.g., a Rich dashboard) react
        in real time: `on_change(set[str])` when upstream changes are detected,
        `on_refresh_start(dit_name)` before each refresh, `on_refresh_done(dit_name,
        RefreshResult)` after success, and `on_refresh_error(dit_name, exc)` if
        the refresh raises. A single DIT failure does not stop the loop —
        subsequent DITs in topological order still attempt to refresh.
        """
        changed = self._detect_changes()
        if not changed:
            return changed
        log.info("upstream changes detected on: %s", sorted(changed))
        if on_change:
            on_change(set(changed))
        if self.config.quiet_period_seconds > 0:
            time.sleep(self.config.quiet_period_seconds)
            more = self._detect_changes()
            if more:
                changed |= more
                if on_change:
                    on_change(more)
        for dit_name in self._affected_dits(changed):
            log.info("refreshing %s", dit_name)
            if on_refresh_start:
                on_refresh_start(dit_name)
            try:
                result = self.pipeline.executor.refresh(self.pipeline.dits[dit_name])
            except Exception as exc:  # noqa: BLE001 -- engine errors vary
                log.exception("refresh failed for %s", dit_name)
                if on_refresh_error:
                    on_refresh_error(dit_name, exc)
                continue
            if on_refresh_done:
                on_refresh_done(dit_name, result)
        return changed

    def run(self) -> None:
        """Run the polling loop. Blocks until KeyboardInterrupt or max_iterations."""
        self._initialize()
        iter_count = 0
        try:
            while True:
                time.sleep(self.config.poll_interval_seconds)
                self.step()
                iter_count += 1
                if self.config.max_iterations and iter_count >= self.config.max_iterations:
                    break
        except KeyboardInterrupt:
            log.info("watcher stopped by user")
