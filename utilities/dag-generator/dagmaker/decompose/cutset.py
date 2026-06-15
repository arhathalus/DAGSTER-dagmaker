"""Cutset / backdoor backend (overlap decomposition, inspired by c_pro).

Instead of partitioning clauses, this **shares** them: a small set of high-degree
"hub" variables is chosen as the interface, and

  * node 0 solves the clauses that mention a hub (constraining the hubs early),
  * node 1 (the terminal) solves **all** clauses, with the hubs fixed by node 0.

Only the hub variables are passed on the edge, so the separator is exactly the
hub count -- independent of the reporting set.  This sidesteps the
reporting/separator tradeoff that forces the partition backends to a single node
on densely-coupled problems: because the terminal contains every clause, all
variables are resolved there authoritatively even though clauses overlap.

Hub selection defaults to clause-occurrence count (free from the CSR index, no
adjacency build -- fast even at 10^6 clauses).  The hub count is capped at the
separator budget so the result always fits.
"""

from __future__ import annotations

from typing import Optional

from ..dagmodel import DagModel


def build(cnf, hubs: int = 32, max_sep: Optional[int] = None,
          reporting=None) -> Optional[DagModel]:
    k = hubs if max_sep is None else min(hubs, max_sep)
    if k <= 0:
        return None

    # rank variables by clause-occurrence count (a cheap connectivity proxy)
    ranked = sorted((v for v in range(1, cnf.max_var + 1) if cnf.var_degree(v) > 0),
                    key=lambda v: cnf.var_degree(v), reverse=True)
    if not ranked:
        return None
    hub_vars = set(ranked[:k])

    node0 = [c for c in range(cnf.n_clauses)
             if any((v in hub_vars) for v in cnf.clause_vars(c))]
    if not node0 or len(node0) == cnf.n_clauses:
        # no constraining subset (or it is the whole problem) -> nothing gained
        return None

    model = DagModel(n_clauses=cnf.n_clauses, max_var=cnf.max_var)
    model.add_node(node0)                       # node 0: clauses touching hubs
    model.add_node(range(cnf.n_clauses))        # node 1: all clauses (terminal)
    model.add_edge(0, 1, hub_vars)              # pass only the hub variables
    model.reporting = set(reporting) if reporting is not None else cnf.used_vars()
    return model
