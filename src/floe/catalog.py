"""Catalog manager — wraps PyIceberg with Floe-specific operations."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa
from pyiceberg.catalog import Catalog, load_catalog
from pyiceberg.exceptions import NoSuchNamespaceError, NoSuchTableError
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.transforms import IdentityTransform

CHECKPOINT_PROPERTY = "floe.last_processed_snapshot_id"
UPSTREAM_CHECKPOINTS_PROPERTY = "floe.last_processed_snapshots"
SOURCE_TABLE_PROPERTY = "floe.source_table"
WINDOW_REFRESHED_AT_PROPERTY = "floe.window_last_refreshed_at"


class CatalogManager:
    """Wraps an Iceberg catalog with Floe-specific helpers."""

    def __init__(self, catalog: Catalog):
        self._catalog = catalog

    @classmethod
    def from_config(
        cls,
        name: str,
        catalog_type: str,
        uri: str,
        warehouse: str | Path,
    ) -> CatalogManager:
        warehouse_path = Path(warehouse).resolve()
        warehouse_path.mkdir(parents=True, exist_ok=True)
        warehouse_uri = f"file://{warehouse_path.as_posix()}"

        catalog = load_catalog(
            name,
            **{
                "type": catalog_type,
                "uri": uri,
                "warehouse": warehouse_uri,
            },
        )
        return cls(catalog)

    @property
    def catalog(self) -> Catalog:
        return self._catalog

    def ensure_namespace(self, namespace: str) -> None:
        try:
            self._catalog.list_namespaces(namespace)
        except NoSuchNamespaceError:
            pass
        try:
            self._catalog.create_namespace(namespace)
        except Exception:
            # already exists — fine
            pass

    def table_exists(self, name: str) -> bool:
        try:
            self._catalog.load_table(name)
            return True
        except NoSuchTableError:
            return False

    def load_table(self, name: str) -> Table:
        return self._catalog.load_table(name)

    def create_table_if_missing(self, name: str, schema: Schema) -> Table:
        namespace = name.rsplit(".", 1)[0] if "." in name else "default"
        self.ensure_namespace(namespace)
        try:
            return self._catalog.load_table(name)
        except NoSuchTableError:
            return self._catalog.create_table(name, schema=schema)

    def create_partitioned_table(
        self,
        name: str,
        arrow_schema: pa.Schema,
        partition_columns: list[str],
    ) -> Table:
        """Create an Iceberg table with identity partitioning on the given columns.

        Implementation: create the table unpartitioned first (so PyIceberg
        assigns fresh field IDs), then evolve the partition spec via
        ``update_spec``. Avoids manual field-ID juggling.
        """
        namespace = name.rsplit(".", 1)[0] if "." in name else "default"
        self.ensure_namespace(namespace)
        table = self._catalog.create_table(name, schema=arrow_schema)
        with table.update_spec() as update:
            for col in partition_columns:
                update.add_field(col, IdentityTransform(), col)
        return self._catalog.load_table(name)

    def append(self, name: str, data: pa.Table) -> Table:
        table = self._catalog.load_table(name)
        table.append(data)
        return self._catalog.load_table(name)

    def overwrite(self, name: str, data: pa.Table) -> Table:
        table = self._catalog.load_table(name)
        table.overwrite(data)
        return self._catalog.load_table(name)

    def current_snapshot_id(self, name: str) -> int | None:
        table = self._catalog.load_table(name)
        snap = table.current_snapshot()
        return snap.snapshot_id if snap else None

    def get_checkpoint(self, name: str) -> int | None:
        table = self._catalog.load_table(name)
        val = table.properties.get(CHECKPOINT_PROPERTY)
        if val is None:
            return None
        try:
            return int(val)
        except ValueError:
            return None

    def set_checkpoint(self, name: str, snapshot_id: int) -> None:
        table = self._catalog.load_table(name)
        with table.transaction() as txn:
            txn.set_properties({CHECKPOINT_PROPERTY: str(snapshot_id)})

    def get_upstream_checkpoints(self, name: str) -> dict[str, int] | None:
        """Return the per-upstream snapshot IDs processed by the last refresh.

        Unlike :meth:`get_checkpoint` (which tracks only the primary upstream),
        this captures *every* upstream so the executor can detect changes to any
        of them — e.g. a late-arriving defect appended to a secondary upstream.
        Returns ``None`` if the table has never been refreshed by Floe.
        """
        table = self._catalog.load_table(name)
        val = table.properties.get(UPSTREAM_CHECKPOINTS_PROPERTY)
        if not val:
            return None
        try:
            data = json.loads(val)
            return {str(k): int(v) for k, v in data.items()}
        except (ValueError, TypeError):
            return None

    def set_upstream_checkpoints(self, name: str, mapping: dict[str, int]) -> None:
        table = self._catalog.load_table(name)
        payload = json.dumps({k: str(v) for k, v in mapping.items()}, sort_keys=True)
        with table.transaction() as txn:
            txn.set_properties({UPSTREAM_CHECKPOINTS_PROPERTY: payload})

    def get_window_refreshed_at(self, name: str) -> datetime | None:
        table = self._catalog.load_table(name)
        val = table.properties.get(WINDOW_REFRESHED_AT_PROPERTY)
        if not val:
            return None
        try:
            return datetime.fromisoformat(val)
        except ValueError:
            return None

    def set_window_refreshed_at(self, name: str, ts: datetime) -> None:
        table = self._catalog.load_table(name)
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=UTC)
        with table.transaction() as txn:
            txn.set_properties({WINDOW_REFRESHED_AT_PROPERTY: ts.isoformat()})

    def overwrite_with_filter(self, name: str, data: pa.Table, filter_expr) -> Table:
        """Replace rows matching `filter_expr` with `data` (partition-aware overwrite)."""
        table = self._catalog.load_table(name)
        table.overwrite(data, overwrite_filter=filter_expr)
        return self._catalog.load_table(name)

    def list_tables(self, namespace: str) -> list[str]:
        try:
            return [".".join(t) for t in self._catalog.list_tables(namespace)]
        except NoSuchNamespaceError:
            return []

    def drop_table_if_exists(self, name: str) -> None:
        if self.table_exists(name):
            self._catalog.drop_table(name)
