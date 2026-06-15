"""Connected-components pre-pass over the primal graph.

Two clauses are connected when they share a variable.  Each connected component
is an independent subproblem: variables in different components never co-occur,
so the components can be solved fully in parallel with a **zero-width separator**
between them.  This is "free parallelism" -- the cheapest kind in Dagster's cost
model -- and it also bounds the work the expensive backends must do (run them
per component).

Implementation: union-find (path compression + union by rank) over variables,
which is near-linear.  Uses ``scipy.sparse.csgraph.connected_components`` when
SciPy is available (faster on huge instances), else the stdlib path.

Empty clauses (no variables) are unconstrained and belong to no component; they
are returned separately so the caller can attach them to a terminal node.
"""

from __future__ import annotations

from typing import List, NamedTuple, Set


class Component(NamedTuple):
    clauses: Set[int]   # clause indices in this component
    variables: Set[int]  # variables in this component


class Decomposition(NamedTuple):
    components: List[Component]   # sorted by clause count, descending
    free_clauses: Set[int]        # clauses with no variables (belong nowhere)


def connected_components(cnf) -> Decomposition:
    try:
        return _scipy_components(cnf)
    except ImportError:
        return _union_find_components(cnf)


# ---------------------------------------------------------------------------

def _union_find_components(cnf) -> Decomposition:
    n = cnf.max_var
    parent = list(range(n + 1))
    rank = [0] * (n + 1)

    def find(x: int) -> int:
        root = x
        while parent[root] != root:
            root = parent[root]
        while parent[x] != root:  # path compression
            parent[x], x = root, parent[x]
        return root

    def union(a: int, b: int) -> None:
        ra, rb = find(a), find(b)
        if ra == rb:
            return
        if rank[ra] < rank[rb]:
            ra, rb = rb, ra
        parent[rb] = ra
        if rank[ra] == rank[rb]:
            rank[ra] += 1

    free: Set[int] = set()
    for c in range(cnf.n_clauses):
        vs = cnf.clause_vars(c)
        if len(vs) == 0:
            free.add(c)
            continue
        first = vs[0]
        for v in vs[1:]:
            union(first, v)

    return _group_by_root(cnf, find, free)


def _group_by_root(cnf, find, free: Set[int]) -> Decomposition:
    comp_clauses = {}
    comp_vars = {}
    for c in range(cnf.n_clauses):
        vs = cnf.clause_vars(c)
        if len(vs) == 0:
            continue
        root = find(vs[0])
        comp_clauses.setdefault(root, set()).add(c)
        cv = comp_vars.setdefault(root, set())
        cv.update(vs)
    comps = [Component(clauses=comp_clauses[r], variables=comp_vars[r])
             for r in comp_clauses]
    comps.sort(key=lambda comp: len(comp.clauses), reverse=True)
    return Decomposition(components=comps, free_clauses=free)


def _scipy_components(cnf) -> Decomposition:
    import numpy as np  # noqa: F401  (import error -> stdlib fallback)
    from scipy.sparse import coo_matrix
    from scipy.sparse.csgraph import connected_components as cc

    # variable-incidence matrix (clauses x vars); two vars connected if they
    # share a clause -> use vars x vars adjacency via incidence^T @ incidence.
    rows = []
    cols = []
    free: Set[int] = set()
    for c in range(cnf.n_clauses):
        vs = cnf.clause_vars(c)
        if len(vs) == 0:
            free.add(c)
            continue
        for v in vs:
            rows.append(c)
            cols.append(v)
    if not rows:
        return Decomposition(components=[], free_clauses=free)
    data = np.ones(len(rows), dtype=np.int8)
    inc = coo_matrix((data, (rows, cols)),
                     shape=(cnf.n_clauses, cnf.max_var + 1)).tocsr()
    var_adj = (inc.T @ inc)  # vars x vars, nonzero where vars co-occur
    n_comp, labels = cc(var_adj, directed=False)

    label_of = {}
    for c in range(cnf.n_clauses):
        vs = cnf.clause_vars(c)
        if len(vs) == 0:
            continue
        label_of[c] = int(labels[vs[0]])

    comp_clauses = {}
    comp_vars = {}
    for c, lab in label_of.items():
        comp_clauses.setdefault(lab, set()).add(c)
        comp_vars.setdefault(lab, set()).update(cnf.clause_vars(c))
    comps = [Component(clauses=comp_clauses[l], variables=comp_vars[l])
             for l in comp_clauses]
    comps.sort(key=lambda comp: len(comp.clauses), reverse=True)
    return Decomposition(components=comps, free_clauses=free)
