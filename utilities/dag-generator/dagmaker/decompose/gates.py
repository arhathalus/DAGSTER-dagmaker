"""Gate / XOR / equivalence-aware decomposition.

Many CNFs are circuit/Tseitin encodings: an "output" variable is *defined* by a
gate over "input" variables (``x <-> AND/OR(...)``), with equivalences and XOR
clusters mixed in.  That induces a definitional layering -- primary inputs, then
gates over them, then gates over those -- and decomposing along those layers
keeps the interface to the signals crossing a layer (often small for "thin"
circuits like a BMC unfolding).

We detect AND/OR gate definitions and equivalences from the signed clauses, order
variables by definitional depth (inputs before the gates that use them), and feed
that order to the existing frontier chunker.  Needs the SIGNED clauses (CnfIndex
keeps only |var|), so ``build`` takes the clause list explicitly; returns
``None`` when no gate structure is found.
"""

from __future__ import annotations

from collections import defaultdict
from typing import Dict, List, Optional, Set

from .. import assemble
from ..decompose import elimination
from ..dagmodel import DagModel


def detect_definitions(clauses: List[List[int]]) -> Dict[int, Set[int]]:
    """Return {output_var: set(input_vars)} for detected AND/OR gate definitions.

    AND gate  x <-> l1 & ... & lk :  long clause (x, -l1, ..., -lk) + binaries (-x, li)
    OR  gate  x <-> l1 | ... | lk :  long clause (-x, l1, ..., lk) + binaries (x, -li)
    """
    binset = set()
    longs = []
    for cl in clauses:
        if len(cl) == 2:
            binset.add(frozenset(cl))
        elif len(cl) >= 3:
            longs.append(cl)
    defines: Dict[int, Set[int]] = {}
    for cl in longs:
        s = set(cl)
        for ox in cl:                       # candidate output literal
            others = [l for l in cl if l != ox]
            # AND: others are the negated inputs; binaries (-ox, -other) must exist
            if all(frozenset({-ox, -o}) in binset for o in others):
                x = abs(ox)
                if x not in defines:
                    defines[x] = {abs(o) for o in others}
                break
            # OR: others are the inputs; binaries (-ox, other) must exist
            if all(frozenset({-ox, o}) in binset for o in others):
                x = abs(ox)
                if x not in defines:
                    defines[x] = {abs(o) for o in others}
                break
    return defines


def _depths(defines: Dict[int, Set[int]], all_vars: Set[int]) -> Dict[int, int]:
    """Definitional depth: primary inputs 0, a gate = 1 + max(input depths).
    Cycles (mutual definitions) are broken by treating a not-yet-resolved input
    as depth 0."""
    depth: Dict[int, int] = {}
    visiting: Set[int] = set()

    def d(v):
        if v in depth:
            return depth[v]
        if v not in defines or v in visiting:
            depth[v] = 0
            return 0
        visiting.add(v)
        depth[v] = 1 + max((d(u) for u in defines[v]), default=0)
        visiting.discard(v)
        return depth[v]

    for v in all_vars:
        d(v)
    return depth


def build(cnf, clauses: List[List[int]], target_nodes: int = 8, max_sep: int = 30,
          prune: bool = True, reporting=None) -> Optional[DagModel]:
    if clauses is None:
        return None
    defines = detect_definitions(clauses)
    if not defines:
        return None  # no gate structure -> let other backends handle it

    used = cnf.used_vars()
    depth = _depths(defines, used)
    # order variables inputs-first by definitional depth (ties by id) -> the
    # frontier across a cut is the set of signals connecting the layers
    order = sorted(used, key=lambda v: (depth.get(v, 0), v))
    chunks = elimination.frontier_chunk(cnf, order, range(cnf.n_clauses),
                                        target_nodes, max_sep)
    if len(chunks) < 2:
        return None
    return assemble.chain(cnf, chunks, reporting=reporting, prune=prune)
