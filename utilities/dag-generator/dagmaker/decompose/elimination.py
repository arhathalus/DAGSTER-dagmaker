"""Primary generic backend: min-degree elimination ordering + frontier chunking.

Why not min-fill (what ``dagify.py`` does): min-fill recomputes fill-in for every
variable each step -- the documented bottleneck at ~10^6 clauses.  We instead use
a **min-degree** ordering with bucket queues and lazy degree updates (no fill
edges materialised), which is near-linear, then cut the ordering into nodes at
low points of the **elimination frontier**.

The key fact that makes this both fast and subset-safe:

  * Assign each clause to the position where its *last* variable is eliminated
    (max rank).  Then a variable is "alive across a cut" iff it occurs in a
    clause on both sides of the cut.
  * The separator width at a cut = number of such alive variables = an
    interval-overlap count computable in one sweep, no elimination game needed.
  * Because variables never un-eliminate, the accumulated neighborhood grows
    monotonically forward, so cutting anywhere yields a valid chain and the
    subset invariant holds by construction (enforced in :mod:`assemble`).

``build`` runs the connected-components pre-pass first (free parallelism) and
chunks each component independently, emitting a disjoint union of chains.
"""

from __future__ import annotations

import heapq
from bisect import bisect_left
from typing import Dict, List, Sequence, Set

from .. import assemble, components, graph
from ..dagmodel import DagModel


def min_degree_order(adj: Dict[int, Set[int]]) -> List[int]:
    """Min-degree elimination order (no fill), via a lazy binary heap.

    Degrees only decrease as neighbours are eliminated.  We push ``(degree, var)``
    and skip stale heap entries; ties break by variable id, so the ordering is
    **deterministic** (important for reproducible golden tests).  O(E log V).
    """
    alive = {v: True for v in adj}
    deg = {v: len(adj[v]) for v in adj}
    heap = [(d, v) for v, d in deg.items()]
    heapq.heapify(heap)

    order: List[int] = []
    while heap:
        d, v = heapq.heappop(heap)
        if not alive[v] or d != deg[v]:
            continue  # stale entry
        alive[v] = False
        order.append(v)
        for u in adj[v]:
            if alive[u]:
                deg[u] -= 1
                heapq.heappush(heap, (deg[u], u))
    return order


def frontier_chunk(cnf, order: Sequence[int], clause_set,
                   target_nodes: int, max_sep: int) -> List[Set[int]]:
    """Split ``clause_set`` into a sequence of clause-sets (chain nodes).

    Cuts are placed to balance clause counts across ``target_nodes`` chunks while
    landing at low-frontier positions; any cut whose separator would exceed
    ``max_sep`` is dropped (merging those chunks), trading parallelism for
    tractability rather than emitting an intractable table.
    """
    npos = len(order)
    if npos == 0:
        return [set(clause_set)]
    rank = {v: i for i, v in enumerate(order)}

    clauses_at: List[List[int]] = [[] for _ in range(npos)]
    first_use = [npos] * npos   # indexed by variable rank
    last_use = [-1] * npos
    for c in clause_set:
        vs = cnf.clause_vars(c)
        ranks = [rank[v] for v in vs if v in rank]
        pos = max(ranks) if ranks else npos - 1
        clauses_at[pos].append(c)
        for rp in ranks:
            if pos < first_use[rp]:
                first_use[rp] = pos
            if pos > last_use[rp]:
                last_use[rp] = pos

    # frontier[i] = #vars alive across the cut between position i and i+1
    add = [0] * npos
    rem = [0] * npos
    for r in range(npos):
        if last_use[r] < 0:
            continue
        add[first_use[r]] += 1
        rem[last_use[r]] += 1
    boundary = [0] * npos
    cum_add = cum_rem = 0
    for i in range(npos):
        cum_add += add[i]
        cum_rem += rem[i]
        boundary[i] = cum_add - cum_rem

    clen = [len(x) for x in clauses_at]
    total = sum(clen)
    cumclause: List[int] = []
    running = 0
    for x in clen:
        running += x
        cumclause.append(running)

    cuts: List[int] = []
    K = max(1, target_nodes)
    if K > 1 and total > 0:
        window = max(1, npos // (2 * K))
        last_cut = -1
        for t in range(1, K):
            target = t * total / K
            center = min(max(bisect_left(cumclause, target), 0), npos - 1)
            lo = max(last_cut + 1, center - window)
            hi = min(npos - 2, center + window)
            if lo > hi:
                continue
            best_i, best_b = lo, boundary[lo]
            for i in range(lo + 1, hi + 1):
                if boundary[i] < best_b:
                    best_i, best_b = i, boundary[i]
            prev_clauses = cumclause[last_cut] if last_cut >= 0 else 0
            if best_b <= max_sep and best_i > last_cut and cumclause[best_i] > prev_clauses:
                cuts.append(best_i)
                last_cut = best_i

    chunks: List[Set[int]] = []
    start = 0
    for b in cuts + [npos - 1]:
        s: Set[int] = set()
        for p in range(start, b + 1):
            s.update(clauses_at[p])
        if s:
            chunks.append(s)
        start = b + 1
    return chunks if chunks else [set(clause_set)]


def decompose_component(cnf, component, target_nodes: int, max_sep: int,
                        order_fn=min_degree_order) -> List[Set[int]]:
    adj = graph.build_adjacency(cnf, set(component.variables))
    order = order_fn(adj)
    return frontier_chunk(cnf, order, component.clauses, target_nodes, max_sep)


def build(cnf, target_nodes: int = 4, max_sep: int = 30, prune: bool = True,
          reporting=None, order_fn=min_degree_order) -> DagModel:
    # prune=True (default) makes the actual edge separator equal the elimination
    # frontier the chunker optimises -- this is what delivers small separators.
    # It carries every shared and reporting variable forward, so it is sound for
    # dagster (it just doesn't satisfy the legacy dag_checker subset invariant).
    dec = components.connected_components(cnf)
    total = sum(len(c.clauses) for c in dec.components) or 1

    branches: List[List[Set[int]]] = []
    for comp in dec.components:
        # allocate node budget proportional to component size (>=1 each)
        k = max(1, round(target_nodes * len(comp.clauses) / total))
        branches.append(decompose_component(cnf, comp, k, max_sep, order_fn))

    # free (variable-less) clauses must still be covered: attach to a terminal
    if dec.free_clauses:
        if branches:
            branches[0][-1] |= set(dec.free_clauses)
        else:
            branches = [[set(dec.free_clauses)]]

    if not branches:
        branches = [[set(range(cnf.n_clauses))]]
    return assemble.disjoint_union(cnf, branches, reporting=reporting, prune=prune)
