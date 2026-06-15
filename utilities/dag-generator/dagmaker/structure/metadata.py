"""Tier A: structure from metadata (the default, highest-fidelity source).

Two interchangeable inputs, both yielding ordered clause groups that become DAG
nodes:

  * **Inline DIMACS comments** -- the convention the repo's own encoders already
    emit (``Benchmarks/sudoku/dag_gen.py``): a ``c <label>`` line precedes a run
    of clauses; consecutive clauses share the active label.  Distinct labels
    partition the clauses into semantic groups.
  * **`.meta` JSON sidecar** -- ``{"groups": [{"label","clauses","vars"}],
    "reporting": "...", "time_index": {...}, "grid": {...}}`` with clause/var
    lists in the usual range notation.

Groups are ordered to minimise the inter-group separator (min-degree on the small
group-interaction graph) and then assembled into a chain.
"""

from __future__ import annotations

import json
from typing import List, Optional, Set, Tuple

from .. import intervals
from ..decompose.elimination import min_degree_order
from ._common import chain_from_ordered_groups
from ..dagmodel import DagModel


def groups_from_comments(cnf) -> List[Tuple[str, Set[int]]]:
    """Build (label, clauses) groups from ``c <label>`` comment runs.

    A marker at clause index ``i`` labels clauses ``[i, next_marker_i)``.
    Returns an empty list if the comments don't usefully partition the clauses.
    """
    markers = cnf.comment_markers
    if not markers:
        return []
    # boundaries: clause index at each marker, plus the end
    spans: List[Tuple[str, int, int]] = []
    for idx, (cstart, label) in enumerate(markers):
        cend = markers[idx + 1][0] if idx + 1 < len(markers) else cnf.n_clauses
        if cend > cstart and label:
            spans.append((label, cstart, cend))
    if not spans:
        return []
    # merge consecutive spans with identical labels
    groups: List[Tuple[str, Set[int]]] = []
    for label, a, b in spans:
        if groups and groups[-1][0] == label:
            groups[-1][1].update(range(a, b))
        else:
            groups.append((label, set(range(a, b))))
    # require that groups actually cover all clauses and there are >= 2 of them
    covered = set()
    for _, cs in groups:
        covered |= cs
    if len(groups) < 2 or len(covered) != cnf.n_clauses:
        return []
    return groups


def groups_from_sidecar(path: str, cnf) -> Tuple[List[Tuple[str, Set[int]]], Optional[Set[int]]]:
    with open(path, "r") as f:
        data = json.load(f)
    groups: List[Tuple[str, Set[int]]] = []
    for g in data.get("groups", []):
        clauses = set(intervals.expand(str(g["clauses"]))) if "clauses" in g else set()
        groups.append((g.get("label", "g{}".format(len(groups))), clauses))
    reporting = None
    if "reporting" in data:
        reporting = set(intervals.expand(str(data["reporting"])))
    return groups, reporting


def order_groups(cnf, groups: List[Tuple[str, Set[int]]]) -> List[Set[int]]:
    """Order groups to keep the inter-group separator small, via min-degree on
    the (small) group-interaction graph."""
    gvars: List[Set[int]] = []
    for _, cs in groups:
        s: Set[int] = set()
        for c in cs:
            s.update(cnf.clause_vars(c))
        gvars.append(s)
    var2g = {}
    for i, vs in enumerate(gvars):
        for v in vs:
            var2g.setdefault(v, []).append(i)
    adj = {i: set() for i in range(len(groups))}
    for gs in var2g.values():
        for a in gs:
            for b in gs:
                if a != b:
                    adj[a].add(b)
    order = min_degree_order(adj)
    return [groups[g][1] for g in order]


def try_build(cnf, *, meta=None, target_nodes=8, max_sep=30,
              reporting=None, prune=True) -> Optional[DagModel]:
    groups: List[Tuple[str, Set[int]]] = []
    meta_reporting = None
    if meta:
        groups, meta_reporting = groups_from_sidecar(meta, cnf)
    if not groups:
        groups = groups_from_comments(cnf)
    if not groups:
        return None
    ordered = order_groups(cnf, groups)
    rep = reporting if reporting is not None else meta_reporting
    return chain_from_ordered_groups(cnf, ordered, target_nodes=target_nodes,
                                     reporting=rep, prune=prune)
