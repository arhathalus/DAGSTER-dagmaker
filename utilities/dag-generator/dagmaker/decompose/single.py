"""Single-node decomposition -- the guaranteed-valid floor.

Produces a DAG with one node containing every clause and no edges, reporting all
variables.  Equivalent to ``utilities/dumb_dag_generator.py`` but built through
the :class:`DagModel`.  Used as the fallback when no useful decomposition exists
(one fat connected blob, ``--nodes 1``, or a budget that forbids any cut) and as
a correctness baseline in tests.
"""

from __future__ import annotations

from typing import Optional, Set

from ..dagmodel import DagModel


def build(cnf, reporting: Optional[Set[int]] = None) -> DagModel:
    model = DagModel(n_clauses=cnf.n_clauses, max_var=cnf.max_var)
    model.add_node(range(cnf.n_clauses))
    model.reporting = set(reporting) if reporting is not None else cnf.used_vars()
    return model
