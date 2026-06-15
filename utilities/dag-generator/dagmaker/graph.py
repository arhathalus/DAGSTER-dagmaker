"""Primal-graph helpers over a :class:`CnfIndex`.

The primal graph has one vertex per variable and an edge between two variables
that share a clause.  We never need the full graph at once for the cost model
(separators are read off clause-assignment intervals), but the min-degree
elimination ordering does need per-variable adjacency, so this builds it on
demand -- optionally restricted to one connected component's variables to keep
memory bounded.
"""

from __future__ import annotations

from typing import Dict, Optional, Set


def build_adjacency(cnf, variables: Optional[Set[int]] = None) -> Dict[int, Set[int]]:
    """Return ``{var: set(neighbour vars)}``.

    If ``variables`` is given, only those vertices are included (neighbours
    outside the set are dropped); pass a connected component's variable set to
    process it in isolation.  A variable that appears only in unit clauses has
    an empty neighbour set (degree 0) but is still present.
    """
    restrict = variables is not None
    adj: Dict[int, Set[int]] = {}
    if restrict:
        for v in variables:
            adj[v] = set()
    for c in range(cnf.n_clauses):
        vs = cnf.clause_vars(c)
        if len(vs) < 2:
            # still register a lone unit-clause variable as a degree-0 vertex
            if not restrict:
                for v in vs:
                    adj.setdefault(v, set())
            continue
        for i in range(len(vs)):
            vi = vs[i]
            if restrict and vi not in adj:
                continue
            ai = adj.setdefault(vi, set()) if not restrict else adj[vi]
            for j in range(len(vs)):
                if i == j:
                    continue
                vj = vs[j]
                if restrict and vj not in adj:
                    continue
                ai.add(vj)
    return adj
