"""In-memory Dagster DAG: nodes (clause sets), edges (variable sets), reporting.

This is the single source of truth for the ``.dag`` output format and the only
place that serialises it.  The format (verified against ``dagster/Dag.cpp``):

    DAG-FILE
    NODES:<n>
    GRAPH:
    <from>-><to>:<vars>          # one per edge; vars passed parent->child
    CLAUSES:
    <node>:<clause indices>      # one per node
    REPORTING:
    <vars>

All integer lists are rendered with :func:`intervals.compact`, which never emits
``a-a`` (rejected by Dag.cpp:136 for clauses).  Edges are written sorted by
``(from, to)`` and nodes in id order ``0 .. n-1``.
"""

from __future__ import annotations

from typing import Dict, List, Set, Tuple

from . import intervals


class DagModel:
    def __init__(self, n_clauses: int, max_var: int) -> None:
        self.n_clauses = n_clauses
        self.max_var = max_var
        self.nodes: List[Set[int]] = []          # node id -> set of clause indices
        self.edges: Dict[Tuple[int, int], Set[int]] = {}  # (u, v) -> set of vars
        self.reporting: Set[int] = set()

    # ----- mutation -----------------------------------------------------

    def add_node(self, clause_indices=()) -> int:
        nid = len(self.nodes)
        self.nodes.append(set(clause_indices))
        return nid

    def add_edge(self, u: int, v: int, variables=()) -> None:
        self.edges.setdefault((u, v), set()).update(variables)

    # ----- topology helpers --------------------------------------------

    @property
    def num_nodes(self) -> int:
        return len(self.nodes)

    def forward_adj(self) -> Dict[int, List[int]]:
        adj: Dict[int, List[int]] = {i: [] for i in range(self.num_nodes)}
        for (u, v) in self.edges:
            adj[u].append(v)
        return adj

    def reverse_adj(self) -> Dict[int, List[int]]:
        adj: Dict[int, List[int]] = {i: [] for i in range(self.num_nodes)}
        for (u, v) in self.edges:
            adj[v].append(u)
        return adj

    def topo_order(self) -> List[int]:
        """Kahn's algorithm.  Returns fewer than num_nodes entries if cyclic."""
        fwd = self.forward_adj()
        indeg = {i: 0 for i in range(self.num_nodes)}
        for (_, v) in self.edges:
            indeg[v] += 1
        queue = [i for i in range(self.num_nodes) if indeg[i] == 0]
        order: List[int] = []
        while queue:
            n = queue.pop()
            order.append(n)
            for s in fwd[n]:
                indeg[s] -= 1
                if indeg[s] == 0:
                    queue.append(s)
        return order

    def terminals(self) -> List[int]:
        fwd = self.forward_adj()
        return [i for i in range(self.num_nodes) if not fwd[i]]

    def roots(self) -> List[int]:
        rev = self.reverse_adj()
        return [i for i in range(self.num_nodes) if not rev[i]]

    def node_local_vars(self, cnf) -> List[Set[int]]:
        """Variables that appear in each node's own clauses."""
        local: List[Set[int]] = []
        for clauses in self.nodes:
            s: Set[int] = set()
            for c in clauses:
                s.update(cnf.clause_vars(c))
            local.append(s)
        return local

    def neighborhoods(self, cnf) -> List[Set[int]]:
        """Per-node variable neighborhood = local clause vars + inherited (incoming
        edge) vars, accumulated in topological order.  Mirrors the definition the
        validator / cost model use for the subset invariant."""
        local = self.node_local_vars(cnf)
        neigh: List[Set[int]] = [set(s) for s in local]
        rev = self.reverse_adj()
        for n in self.topo_order():
            for p in rev[n]:
                neigh[n] |= self.edges[(p, n)]
        return neigh

    # ----- serialisation ------------------------------------------------

    def to_string(self) -> str:
        out = ["DAG-FILE", "NODES:{}".format(self.num_nodes), "GRAPH:"]
        for (u, v) in sorted(self.edges):
            out.append("{}->{}:{}".format(u, v, intervals.compact(self.edges[(u, v)])))
        out.append("CLAUSES:")
        for n in range(self.num_nodes):
            clause_str = intervals.compact(self.nodes[n])
            # guard: Dag.cpp rejects a clause range a-a; compact() guarantees bare
            # singletons, so this just documents the invariant.
            out.append("{}:{}".format(n, clause_str))
        out.append("REPORTING:")
        out.append(intervals.compact(self.reporting))
        return "\n".join(out) + "\n"

    def write(self, path: str) -> None:
        with open(path, "w") as f:
            f.write(self.to_string())

    # ----- parsing (round-trip / reading existing DAGs) -----------------

    @classmethod
    def from_string(cls, text: str, n_clauses: int = 0, max_var: int = 0) -> "DagModel":
        lines = [ln.rstrip("\n") for ln in text.splitlines()]
        if not lines or lines[0] != "DAG-FILE":
            raise ValueError("missing DAG-FILE header")
        if not lines[1].startswith("NODES:"):
            raise ValueError("missing NODES header")
        n = int(lines[1][len("NODES:"):])
        model = cls(n_clauses, max_var)
        for _ in range(n):
            model.add_node()
        i = 2
        if lines[i] != "GRAPH:":
            raise ValueError("missing GRAPH header")
        i += 1
        while lines[i] != "CLAUSES:":
            head, _, body = lines[i].partition(":")
            u_s, _, v_s = head.partition("->")
            model.add_edge(int(u_s), int(v_s), intervals.expand(body))
            i += 1
        i += 1  # skip CLAUSES:
        while i < len(lines) and lines[i] != "REPORTING:":
            head, _, body = lines[i].partition(":")
            model.nodes[int(head)] = set(intervals.expand(body))
            i += 1
        i += 1  # skip REPORTING:
        report: Set[int] = set()
        while i < len(lines):
            if lines[i]:
                report.update(intervals.expand(lines[i]))
            i += 1
        model.reporting = report
        if not model.max_var and report:
            model.max_var = max(report)
        return model

    @classmethod
    def from_file(cls, path: str, n_clauses: int = 0, max_var: int = 0) -> "DagModel":
        with open(path, "r") as f:
            return cls.from_string(f.read(), n_clauses, max_var)
