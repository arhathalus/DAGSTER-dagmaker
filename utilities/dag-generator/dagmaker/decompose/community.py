"""Community-detection decomposition.

Many encodings (and most industrial CNFs) have modular structure: dense clusters
of variables loosely linked to each other.  We partition the variable-incidence
graph into communities, make each community a node, and let the (few)
cross-community variables be the interface.  Ordering the communities by a
min-degree sweep over the small community-interaction graph keeps the chain
tight.

Communities come from Louvain (networkx) when available, else a deterministic
label-propagation pass in the stdlib.
"""

from __future__ import annotations

from collections import Counter, defaultdict
from typing import Dict, List, Optional, Set

from .. import assemble, graph
from ..decompose.elimination import min_degree_order
from ..dagmodel import DagModel


def _louvain(adj: Dict[int, Set[int]]) -> Optional[List[Set[int]]]:
    try:
        import networkx as nx
        from networkx.algorithms.community import louvain_communities
    except ImportError:
        return None
    G = nx.Graph()
    G.add_nodes_from(adj)
    for v in adj:
        for u in adj[v]:
            if u > v:
                G.add_edge(v, u)
    return [set(c) for c in louvain_communities(G, seed=1)]


def label_propagation(adj: Dict[int, Set[int]], max_iter: int = 30) -> List[Set[int]]:
    """Deterministic label propagation: each vertex takes the most common label
    among its neighbours (ties -> smallest label). Reproducible (no randomness)."""
    label = {v: v for v in adj}
    for _ in range(max_iter):
        changed = False
        for v in sorted(adj):
            if not adj[v]:
                continue
            counts = Counter(label[u] for u in adj[v])
            best = min(counts, key=lambda l: (-counts[l], l))
            if label[v] != best:
                label[v] = best
                changed = True
        if not changed:
            break
    groups: Dict[int, Set[int]] = defaultdict(set)
    for v, l in label.items():
        groups[l].add(v)
    return list(groups.values())


def build(cnf, target_nodes: int = 0, max_sep: int = 30, prune: bool = True,
          reporting=None) -> Optional[DagModel]:
    adj = graph.build_adjacency(cnf, cnf.used_vars())
    if not adj:
        return None
    comms = _louvain(adj) or label_propagation(adj)
    comms = [c for c in comms if c]
    if len(comms) < 2:
        return None  # one community -> nothing to exploit

    comm_of: Dict[int, int] = {}
    for ci, c in enumerate(comms):
        for v in c:
            comm_of[v] = ci

    # assign each clause to the community holding the majority of its variables
    node_clauses: List[Set[int]] = [set() for _ in range(len(comms))]
    for c in range(cnf.n_clauses):
        vs = [v for v in cnf.clause_vars(c)]
        if not vs:
            node_clauses[0].add(c)
            continue
        tally = Counter(comm_of[v] for v in vs if v in comm_of)
        target = tally.most_common(1)[0][0] if tally else 0
        node_clauses[target].add(c)

    # order communities by min-degree on the community-interaction graph
    cadj: Dict[int, Set[int]] = {i: set() for i in range(len(comms))}
    for v in adj:
        cv = comm_of.get(v)
        for u in adj[v]:
            cu = comm_of.get(u)
            if cv is not None and cu is not None and cv != cu:
                cadj[cv].add(cu)
                cadj[cu].add(cv)
    order = min_degree_order(cadj)

    # chain communities in that order, dropping any that ended up empty
    ordered = [node_clauses[i] for i in order if node_clauses[i]]
    if len(ordered) < 2:
        return None
    edges = [(i, i + 1) for i in range(len(ordered) - 1)]
    return assemble.from_topology(cnf, ordered, edges, reporting=reporting, prune=prune)
