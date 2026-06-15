"""Graph-family plugin: graph-colouring / Ramsey-style encodings.

These encode a graph problem where variables map to (edge, colour) or
(vertex, colour) pairs.  The natural decomposition partitions the underlying
graph and uses boundary vertices/edges as separators.  Recovering the graph from
the flat CNF without metadata is unreliable, so this plugin only fires when a
``meta['graph']`` description is supplied; otherwise it defers to the generic
backend (which operates on essentially the same graph).
"""

from __future__ import annotations

from typing import Optional

from ...dagmodel import DagModel


class GraphFamilyPlugin:
    name = "graph"

    def detect(self, cnf, meta=None) -> float:
        # only meta-driven for now
        if meta and isinstance(meta, dict) and meta.get("graph"):
            return 0.8
        return 0.0

    def build(self, cnf, *, target_nodes=8, max_sep=30, reporting=None,
              prune=True, meta=None) -> Optional[DagModel]:
        # Without a concrete partition we cannot beat the generic backend; a
        # full implementation would read meta['graph'] and partition it.
        return None
