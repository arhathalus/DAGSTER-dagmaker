"""Alternative variable orderings for frontier chunking.

The min-degree ordering in :mod:`elimination` is treewidth-oriented; other sweep
orders sometimes find better low-frontier cut points.  Each ordering here feeds
the same ``frontier_chunk`` machinery (so the result is a subset-safe chain), and
``build`` reuses ``elimination.build`` with the chosen ``order_fn``.

Orderings:
  * ``bfs``  -- breadth-first from a pseudo-peripheral vertex (good locality).
  * ``rcm``  -- reverse Cuthill-McKee (bandwidth/frontier minimising), stdlib.
  * ``spectral`` -- sort by the Fiedler vector of the graph Laplacian; uses
    scipy.sparse if available, else dense numpy, else falls back to RCM.
"""

from __future__ import annotations

from collections import deque
from typing import Dict, List, Set

from . import elimination
from ..dagmodel import DagModel


def _pseudo_peripheral(adj: Dict[int, Set[int]]):
    if not adj:
        return None
    start = min(adj, key=lambda v: len(adj[v]))
    seen = _bfs_levels(adj, start)
    far = max(seen, key=lambda v: seen[v]) if seen else start
    return far


def _bfs_levels(adj, start) -> Dict[int, int]:
    level = {start: 0}
    q = deque([start])
    while q:
        v = q.popleft()
        for u in adj[v]:
            if u not in level:
                level[u] = level[v] + 1
                q.append(u)
    return level


def bfs_order(adj: Dict[int, Set[int]]) -> List[int]:
    order: List[int] = []
    visited: Set[int] = set()
    remaining = set(adj)
    while remaining:
        start = _pseudo_peripheral({v: adj[v] for v in remaining}) \
            if len(remaining) < len(adj) else _pseudo_peripheral(adj)
        if start is None or start in visited:
            start = next(iter(remaining))
        q = deque([start])
        visited.add(start)
        remaining.discard(start)
        while q:
            v = q.popleft()
            order.append(v)
            for u in sorted(adj[v], key=lambda x: len(adj[x])):
                if u not in visited:
                    visited.add(u)
                    remaining.discard(u)
                    q.append(u)
    return order


def rcm_order(adj: Dict[int, Set[int]]) -> List[int]:
    # Cuthill-McKee = BFS from a pseudo-peripheral node, neighbours visited in
    # ascending-degree order; reverse the result.
    return list(reversed(bfs_order(adj)))


def spectral_order(adj: Dict[int, Set[int]]) -> List[int]:
    verts = list(adj)
    n = len(verts)
    if n <= 2:
        return verts
    idx = {v: i for i, v in enumerate(verts)}
    try:
        import numpy as np
        try:
            from scipy.sparse import csr_matrix
            from scipy.sparse.linalg import eigsh
            rows, cols = [], []
            for v in verts:
                for u in adj[v]:
                    rows.append(idx[v]); cols.append(idx[u])
            import numpy as _np
            data = _np.ones(len(rows))
            A = csr_matrix((data, (rows, cols)), shape=(n, n))
            deg = _np.asarray(A.sum(axis=1)).ravel()
            from scipy.sparse import diags
            L = diags(deg) - A
            # smallest two eigenvalues; Fiedler = 2nd
            vals, vecs = eigsh(L, k=min(2, n - 1), which="SM")
            fiedler = vecs[:, vals.argsort()[1]]
        except ImportError:
            A = np.zeros((n, n))
            for v in verts:
                for u in adj[v]:
                    A[idx[v], idx[u]] = 1.0
            L = np.diag(A.sum(axis=1)) - A
            vals, vecs = np.linalg.eigh(L)
            fiedler = vecs[:, 1]
        return [verts[i] for i in sorted(range(n), key=lambda i: fiedler[i])]
    except ImportError:
        return rcm_order(adj)


ORDERINGS = {"bfs": bfs_order, "rcm": rcm_order, "spectral": spectral_order}


def build(cnf, method: str = "rcm", target_nodes: int = 8, max_sep: int = 30,
          prune: bool = True, reporting=None) -> DagModel:
    order_fn = ORDERINGS.get(method, rcm_order)
    return elimination.build(cnf, target_nodes=target_nodes, max_sep=max_sep,
                             prune=prune, reporting=reporting, order_fn=order_fn)
