"""DAG cost model + feature extraction.

Encodes what makes a Dagster DAG expensive (verified in ``dagster/Dag.cpp``,
``SolutionsInterface.h``, ``TableSolutions.cpp``):

  * The variables on an edge are a vertex separator; up to ``2^width`` partial
    solutions are enumerated and stored per edge.  Cost is **exponential in
    separator width** -- this dominates everything.
  * A node with in-degree ``d > 1`` (a join) takes the **cross-product** of its
    incoming partial-solution sets, i.e. ``2^(sum of incoming widths)``.
  * Work multiplies along a path, so **depth** amplifies cost.

The comparison key is lexicographic so the exponential term dominates ranking:
``(max_sep_width, max_join_bits, total_sep_width, depth)`` -- lower is better.
This lets the multi-backend driver "run several, keep the best" meaningfully.

The :class:`Score` also exposes structural features (parallel width, per-node
sizes, shape) consumed by :mod:`dagmaker.advisor`.
"""

from __future__ import annotations

from typing import Dict, List

from .dagmodel import DagModel


class Score:
    def __init__(self) -> None:
        self.num_nodes = 0
        self.num_edges = 0
        self.max_sep_width = 0       # widest edge separator (the headline cost)
        self.total_sep_width = 0     # sum of edge widths
        self.max_join_bits = 0       # max over nodes of sum of incoming widths
        self.max_join_indeg = 0      # max node in-degree
        self.depth = 0               # longest path length in nodes
        self.parallel_width = 0      # max #nodes sharing a depth level
        self.node_clause_counts: List[int] = []
        self.node_var_counts: List[int] = []
        self.edge_widths: List[int] = []

    @property
    def key(self):
        """Sort key: lower is better.  Exponential separator term first."""
        return (self.max_sep_width, self.max_join_bits,
                self.total_sep_width, self.depth)

    def features(self) -> Dict:
        return {
            "num_nodes": self.num_nodes,
            "num_edges": self.num_edges,
            "max_sep_width": self.max_sep_width,
            "total_sep_width": self.total_sep_width,
            "max_join_bits": self.max_join_bits,
            "max_join_indeg": self.max_join_indeg,
            "depth": self.depth,
            "parallel_width": self.parallel_width,
            "node_clause_counts": self.node_clause_counts,
            "node_var_counts": self.node_var_counts,
            "edge_widths": self.edge_widths,
        }

    def __str__(self) -> str:
        shape = "chain" if self.parallel_width <= 1 else "tree/parallel"
        return ("nodes={} edges={} max_sep={} total_sep={} max_join_bits={} "
                "depth={} parallel_width={} ({})").format(
            self.num_nodes, self.num_edges, self.max_sep_width,
            self.total_sep_width, self.max_join_bits, self.depth,
            self.parallel_width, shape)


def score(model: DagModel, cnf=None) -> Score:
    s = Score()
    s.num_nodes = model.num_nodes
    s.num_edges = len(model.edges)

    # separator widths
    indeg_bits: Dict[int, int] = {i: 0 for i in range(model.num_nodes)}
    indeg: Dict[int, int] = {i: 0 for i in range(model.num_nodes)}
    for (u, v), vars_ in model.edges.items():
        w = len(vars_)
        s.edge_widths.append(w)
        s.total_sep_width += w
        s.max_sep_width = max(s.max_sep_width, w)
        indeg_bits[v] += w
        indeg[v] += 1
    if indeg_bits:
        s.max_join_bits = max(indeg_bits.values())
        s.max_join_indeg = max(indeg.values())

    # depth (longest path) and parallel width via a longest-path layering
    level = _longest_path_levels(model)
    if level:
        s.depth = max(level.values()) + 1
        per_level: Dict[int, int] = {}
        for lv in level.values():
            per_level[lv] = per_level.get(lv, 0) + 1
        s.parallel_width = max(per_level.values())

    # per-node sizes
    s.node_clause_counts = [len(c) for c in model.nodes]
    if cnf is not None:
        local = model.node_local_vars(cnf)
        s.node_var_counts = [len(v) for v in local]

    return s


def _longest_path_levels(model: DagModel) -> Dict[int, int]:
    order = model.topo_order()
    if len(order) != model.num_nodes:
        return {}  # cyclic; depth undefined
    rev = model.reverse_adj()
    level = {i: 0 for i in range(model.num_nodes)}
    for n in order:
        for p in rev[n]:
            if level[p] + 1 > level[n]:
                level[n] = level[p] + 1
    return level
