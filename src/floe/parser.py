"""Parser for Floe DIT (CREATE DYNAMIC TABLE) SQL files."""

from __future__ import annotations

import re
from pathlib import Path

import sqlglot
from sqlglot import exp

from floe.models import DynamicTable, RefreshMode

_OUTER_RE = re.compile(
    r"""
    ^\s*
    CREATE \s+ DYNAMIC \s+ TABLE \s+
    (?P<name>[\w.]+)
    (?:\s+(?P<middle>.+?))?
    \s+ AS \s+
    (?P<query>.+?)
    ;?\s*$
    """,
    re.IGNORECASE | re.VERBOSE | re.DOTALL,
)

_OPTION_RE = re.compile(r"(\w+)\s*=\s*'([^']*)'", re.IGNORECASE)
_PARTITION_BY_RE = re.compile(r"PARTITION\s+BY\s*\(([^)]+)\)", re.IGNORECASE)


def parse_dit_sql(sql_text: str, source_path: str | None = None) -> DynamicTable:
    """Parse a single CREATE DYNAMIC TABLE statement."""
    cleaned = "\n".join(
        line for line in sql_text.splitlines() if not line.strip().startswith("--")
    ).strip()

    m = _OUTER_RE.match(cleaned)
    if not m:
        raise ValueError(
            "Could not parse DIT statement. Expected: "
            "CREATE DYNAMIC TABLE <name> [PARTITION BY (col)] [<KEY> = '<value>']* AS <query>"
        )

    name = m.group("name").strip()
    middle = (m.group("middle") or "").strip()
    query = m.group("query").strip()

    partition_by, leftover = _extract_partition_by(middle)
    options = {k.lower(): v for k, v in _OPTION_RE.findall(leftover)}

    lag = options.get("lag", "5 minutes")
    refresh_mode_raw = options.get("refresh_mode", "INCREMENTAL").upper()

    try:
        refresh_mode = RefreshMode(refresh_mode_raw)
    except ValueError as e:
        raise ValueError(
            f"Unknown refresh mode {refresh_mode_raw!r}. Valid: {[m.value for m in RefreshMode]}"
        ) from e

    upstream = extract_upstream_tables(query)

    return DynamicTable(
        name=name,
        query=query,
        lag=lag,
        refresh_mode=refresh_mode,
        partition_by=partition_by,
        partition_freshness=options.get("partition_freshness"),
        partition_window=options.get("partition_window"),
        upstream_tables=upstream,
        source_path=source_path,
    )


def _extract_partition_by(text: str) -> tuple[list[str], str]:
    m = _PARTITION_BY_RE.search(text)
    if not m:
        return [], text
    cols = [c.strip() for c in m.group(1).split(",") if c.strip()]
    leftover = text[: m.start()] + text[m.end() :]
    return cols, leftover


def extract_upstream_tables(query: str) -> list[str]:
    """Extract fully-qualified table references from a SELECT query.

    CTE aliases are excluded — they are not real upstream tables.
    """
    try:
        parsed = sqlglot.parse_one(query, dialect="duckdb")
    except Exception as e:
        raise ValueError(f"Failed to parse SQL query: {e}") from e

    cte_names: set[str] = set()
    for cte in parsed.find_all(exp.CTE):
        if cte.alias:
            cte_names.add(cte.alias)

    tables: list[str] = []
    seen: set[str] = set()
    for t in parsed.find_all(exp.Table):
        catalog = t.args.get("catalog")
        db = t.args.get("db")
        # An unqualified table reference whose name matches a CTE alias is a CTE ref.
        if not catalog and not db and t.name in cte_names:
            continue
        parts = []
        if catalog:
            parts.append(catalog.name)
        if db:
            parts.append(db.name)
        parts.append(t.name)
        full = ".".join(parts)
        if full not in seen:
            seen.add(full)
            tables.append(full)
    return tables


def discover_dit_files(transformations_dir: Path) -> list[Path]:
    """Find all .sql files in a directory tree."""
    if not transformations_dir.exists():
        return []
    return sorted(transformations_dir.rglob("*.sql"))


def load_dits(transformations_dir: Path) -> list[DynamicTable]:
    """Parse all DIT files in a directory."""
    dits = []
    for path in discover_dit_files(transformations_dir):
        sql = path.read_text(encoding="utf-8")
        dit = parse_dit_sql(sql, source_path=str(path))
        dits.append(dit)
    return dits
