"""DAG planner — builds a dependency graph from DIT definitions."""

from __future__ import annotations

import networkx as nx

from floe.models import DynamicTable


class DAGPlanner:
    """Builds and validates the dependency DAG of Dynamic Iceberg Tables."""

    def __init__(self, dits: list[DynamicTable]):
        self.dits = {d.name: d for d in dits}
        self.graph = self._build_graph(dits)

    @staticmethod
    def _build_graph(dits: list[DynamicTable]) -> nx.DiGraph:
        g = nx.DiGraph()
        names = {d.name for d in dits}

        for d in dits:
            g.add_node(d.name, dit=d)

        for d in dits:
            for upstream in d.upstream_tables:
                # Only add edges for upstream tables managed by Floe;
                # external sources are leaves we don't draw edges to.
                if upstream in names:
                    g.add_edge(upstream, d.name)

        if not nx.is_directed_acyclic_graph(g):
            cycles = list(nx.simple_cycles(g))
            raise ValueError(f"Cycle detected in DIT graph: {cycles}")

        return g

    def topological_order(self) -> list[str]:
        return list(nx.topological_sort(self.graph))

    def downstream_of(self, table: str) -> list[str]:
        """Return all DITs that depend (transitively) on the given table."""
        if table not in self.graph:
            return []
        return list(nx.descendants(self.graph, table))

    def upstream_of(self, table: str) -> list[str]:
        if table not in self.graph:
            return []
        return list(nx.ancestors(self.graph, table))

    def direct_upstream(self, table: str) -> list[str]:
        if table not in self.graph:
            return []
        return list(self.graph.predecessors(table))

    def external_sources(self) -> list[str]:
        """Tables referenced but not defined as DITs (leaves of the dependency tree)."""
        sources: set[str] = set()
        for d in self.dits.values():
            for upstream in d.upstream_tables:
                if upstream not in self.dits:
                    sources.add(upstream)
        return sorted(sources)

    def render_ascii(self) -> str:
        """Return a simple ASCII rendering of the DAG."""
        lines = []
        lines.append("DAG (top-down):\n")
        for level, nodes in enumerate(_layered_topological(self.graph)):
            indent = "  " * level
            for n in sorted(nodes):
                upstream = ", ".join(sorted(self.graph.predecessors(n))) or "<source>"
                lines.append(f"{indent}- {n}  (depends on: {upstream})")
        ext = self.external_sources()
        if ext:
            lines.append("\nExternal sources:")
            for s in ext:
                lines.append(f"  - {s}")
        return "\n".join(lines)


def _layered_topological(g: nx.DiGraph) -> list[list[str]]:
    """Group nodes by depth level for layered display."""
    layers: list[list[str]] = []
    g2 = g.copy()
    while g2.number_of_nodes():
        roots = [n for n, d in g2.in_degree() if d == 0]
        if not roots:
            break
        layers.append(roots)
        g2.remove_nodes_from(roots)
    return layers
