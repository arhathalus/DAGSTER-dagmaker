"""Turn a (node clause-sets + topology) spec into a validated DagModel.

This is the ONLY place edge variable sets are computed, so the subset invariant
(``dagster/Dag.cpp`` / ``dag_checker.py``) is enforced in exactly one location.

Default policy is **pass-all-data**: an edge carries the parent's entire variable
neighborhood (its own clause variables plus everything it inherited).  Because
neighborhoods then grow monotonically along every path, ``neigh[parent] ⊆
neigh[child]`` holds by construction and the terminal node sees every variable
needed for REPORTING.

``prune=True`` reproduces dagify's ``--not_pass_all_data`` behaviour: an edge
carries only the variables some downstream node still needs.  This shrinks
separators but deliberately breaks the strict subset invariant, so it is opt-in
and flagged (cf. README: "not recommended for reporting of solutions").

All backends (generic and structure-aware) funnel through :func:`from_topology`.
"""

from __future__ import annotations

from typing import Dict, Iterable, List, Optional, Sequence, Set, Tuple

from .dagmodel import DagModel


def from_topology(cnf,
                  node_clauses: Sequence[Iterable[int]],
                  edges: Sequence[Tuple[int, int]],
                  reporting: Optional[Set[int]] = None,
                  prune: bool = False) -> DagModel:
    model = DagModel(n_clauses=cnf.n_clauses, max_var=cnf.max_var)
    for clauses in node_clauses:
        model.add_node(clauses)
    for (u, v) in edges:
        model.add_edge(u, v)  # variable sets filled in below

    local = model.node_local_vars(cnf)
    order = model.topo_order()
    if len(order) != model.num_nodes:
        raise ValueError("assemble: topology is cyclic")
    fwd = model.forward_adj()

    # Resolve the reporting set up front: pruning must protect reporting
    # variables (they have to reach a terminal to be output), so downstream-need
    # is computed *after* reporting is known.
    if reporting is None:
        reporting = cnf.used_vars()
    reporting = set(reporting)

    # inherited[n] accumulates variables arriving on incoming edges
    inherited: List[Set[int]] = [set() for _ in range(model.num_nodes)]
    neigh: List[Set[int]] = [set() for _ in range(model.num_nodes)]

    downstream = _downstream_vars(model, local, reporting) if prune else None

    for n in order:
        neigh[n] = local[n] | inherited[n]
        for c in fwd[n]:
            passed = neigh[n] if not prune else (neigh[n] & downstream[c])
            model.edges[(n, c)] = set(passed)
            inherited[c] |= passed

    model.reporting = reporting
    return model


def _downstream_vars(model: DagModel, local: List[Set[int]],
                     reporting: Optional[Set[int]]) -> List[Set[int]]:
    """Variables that node n or any of its descendants references (plus the
    reporting set at terminals).  Used only for edge pruning."""
    order = model.topo_order()
    fwd = model.forward_adj()
    report = reporting if reporting is not None else set()
    down: List[Set[int]] = [set(local[n]) for n in range(model.num_nodes)]
    for n in model.terminals():
        down[n] |= report
    for n in reversed(order):
        for c in fwd[n]:
            down[n] |= down[c]
    return down


def chain(cnf, node_clauses: Sequence[Iterable[int]],
          reporting: Optional[Set[int]] = None, prune: bool = False) -> DagModel:
    """Convenience: assemble a linear chain 0 -> 1 -> ... -> k-1."""
    edges = [(i, i + 1) for i in range(len(node_clauses) - 1)]
    return from_topology(cnf, node_clauses, edges, reporting=reporting, prune=prune)


def disjoint_union(cnf, branches: Sequence[Sequence[Iterable[int]]],
                   reporting: Optional[Set[int]] = None,
                   prune: bool = False) -> DagModel:
    """Assemble several independent chains (one per connected component) into a
    single DAG with no edges between branches.  Dagster supports disjoint
    subgraphs (Dag.cpp assigns them separate subgraph indices)."""
    node_clauses: List[Iterable[int]] = []
    edges: List[Tuple[int, int]] = []
    for branch in branches:
        base = len(node_clauses)
        node_clauses.extend(branch)
        edges.extend((base + i, base + i + 1) for i in range(len(branch) - 1))
    return from_topology(cnf, node_clauses, edges, reporting=reporting, prune=prune)
