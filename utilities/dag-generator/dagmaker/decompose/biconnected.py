"""Articulation / biconnected-block decomposition.

Where the variable-incidence graph has **cut variables** (articulation points),
removing one disconnects the problem -- so it is a separator of size 1.  This
backend finds the biconnected blocks and the block-cut tree, makes each block a
node, and uses the shared articulation variables as the (tiny) edge separators.

Each clause is a clique in the VIG, so its variables lie within a single block;
we assign the clause there.  When the whole graph is one block (densely coupled,
no cut variable) there is nothing to exploit and we return ``None`` so other
backends handle it.

Block-cut computation uses networkx when available (robust) and an iterative
Tarjan fallback otherwise (no recursion-depth limit at scale).
"""

from __future__ import annotations

from collections import defaultdict, deque
from typing import Dict, List, Optional, Set, Tuple

from .. import assemble, graph
from ..dagmodel import DagModel


def _blocks_articulation(adj: Dict[int, Set[int]]) -> Tuple[List[Set[int]], Set[int]]:
    try:
        import networkx as nx
        G = nx.Graph()
        G.add_nodes_from(adj)
        for v in adj:
            for u in adj[v]:
                if u > v:
                    G.add_edge(v, u)
        blocks = [set(b) for b in nx.biconnected_components(G)]
        art = set(nx.articulation_points(G))
        # nodes in no block (isolated) -> singleton blocks
        covered = set().union(*blocks) if blocks else set()
        for v in adj:
            if v not in covered:
                blocks.append({v})
        return blocks, art
    except ImportError:
        return _tarjan_bcc(adj)


def _tarjan_bcc(adj: Dict[int, Set[int]]) -> Tuple[List[Set[int]], Set[int]]:
    disc: Dict[int, int] = {}
    low: Dict[int, int] = {}
    parent: Dict[int, int] = {}
    timer = [0]
    edge_stack: List[Tuple[int, int]] = []
    blocks: List[Set[int]] = []
    art: Set[int] = set()

    for root in adj:
        if root in disc:
            continue
        disc[root] = low[root] = timer[0]; timer[0] += 1
        stack = [(root, iter(adj[root]))]
        root_children = 0
        while stack:
            v, it = stack[-1]
            advanced = False
            for u in it:
                if u == parent.get(v):
                    continue
                if u not in disc:
                    parent[u] = v
                    edge_stack.append((v, u))
                    disc[u] = low[u] = timer[0]; timer[0] += 1
                    stack.append((u, iter(adj[u])))
                    if v == root:
                        root_children += 1
                    advanced = True
                    break
                elif disc[u] < disc[v]:
                    edge_stack.append((v, u))
                    low[v] = min(low[v], disc[u])
            if not advanced:
                stack.pop()
                if stack:
                    p = stack[-1][0]
                    low[p] = min(low[p], low[v])
                    if low[v] >= disc[p]:
                        if p != root:
                            art.add(p)
                        comp: Set[int] = set()
                        while edge_stack:
                            e = edge_stack.pop()
                            comp.update(e)
                            if e == (p, v) or e == (v, p):
                                break
                        if comp:
                            blocks.append(comp)
        if root_children > 1:
            art.add(root)
        if root not in (b for blk in blocks for b in blk) and not adj[root]:
            blocks.append({root})
    return blocks, art


def build(cnf, target_nodes: int = 0, max_sep: int = 30, prune: bool = True,
          reporting=None) -> Optional[DagModel]:
    adj = graph.build_adjacency(cnf, cnf.used_vars())
    if not adj:
        return None
    blocks, art = _blocks_articulation(adj)
    if len(blocks) < 2:
        return None  # one block -> no cut variable to exploit

    # block id per (non-articulation) variable; articulation vars span blocks
    var_blocks: Dict[int, List[int]] = defaultdict(list)
    for bi, b in enumerate(blocks):
        for v in b:
            var_blocks[v].append(bi)

    # assign each clause to a block containing all its variables (cliques lie in
    # one block; resolve via the non-articulation vars, else by intersection)
    node_clauses: List[Set[int]] = [set() for _ in range(len(blocks))]
    for c in range(cnf.n_clauses):
        vs = [v for v in cnf.clause_vars(c)]
        if not vs:
            node_clauses[0].add(c)
            continue
        target = None
        for v in vs:
            if len(var_blocks[v]) == 1:           # non-articulation -> its block
                target = var_blocks[v][0]
                break
        if target is None:                         # all articulation: intersect
            common = set(var_blocks[vs[0]])
            for v in vs[1:]:
                common &= set(var_blocks[v])
            target = min(common) if common else min(var_blocks[vs[0]])
        node_clauses[target].add(c)

    # build the actual block-cut TREE: an edge between two blocks that share an
    # articulation variable, taken as a spanning tree (block-cut graph is a tree).
    order, edges = _block_tree(blocks, art)
    remapped = _remap(node_clauses, edges, order)
    model = assemble.from_topology(cnf, remapped[0], remapped[1],
                                   reporting=reporting, prune=prune)
    return model


def _block_tree(blocks: List[Set[int]], art: Set[int]):
    """BFS spanning tree of the block-cut graph; returns (block order, tree edges)."""
    art_to_blocks: Dict[int, List[int]] = defaultdict(list)
    for bi, b in enumerate(blocks):
        for v in b & art:
            art_to_blocks[v].append(bi)
    badj: Dict[int, Set[int]] = defaultdict(set)
    for bs in art_to_blocks.values():
        for a in bs:
            for c in bs:
                if a != c:
                    badj[a].add(c)
    order: List[int] = []
    edges: List[Tuple[int, int]] = []
    seen: Set[int] = set()
    for start in range(len(blocks)):
        if start in seen:
            continue
        q = deque([start]); seen.add(start)
        while q:
            x = q.popleft(); order.append(x)
            for y in sorted(badj[x]):
                if y not in seen:
                    seen.add(y); q.append(y)
                    edges.append((x, y))   # parent -> child (tree edge)
    return order, edges


def _remap(node_clauses: List[Set[int]], edges, order):
    """Drop empty blocks and renumber nodes to 0..k-1 in DFS order."""
    keep = [b for b in order if node_clauses[b]]
    newid = {b: i for i, b in enumerate(keep)}
    nodes = [node_clauses[b] for b in keep]
    new_edges = []
    for (u, v) in edges:
        if u in newid and v in newid and newid[u] != newid[v]:
            new_edges.append((newid[u], newid[v]))
    return nodes, new_edges
