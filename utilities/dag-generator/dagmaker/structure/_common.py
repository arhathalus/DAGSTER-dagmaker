"""Shared helpers for structure-aware builders."""

from __future__ import annotations

from typing import List, Sequence, Set

from .. import assemble
from ..dagmodel import DagModel


def merge_consecutive(node_clauses: Sequence[Set[int]], target: int) -> List[Set[int]]:
    """Merge an ordered sequence of clause-sets into at most ``target`` buckets,
    balancing clause counts.  Order is preserved (so a chain stays a chain).
    Fewer groups than ``target`` are returned unchanged."""
    groups = [set(s) for s in node_clauses if s]
    if target <= 0 or len(groups) <= target:
        return groups
    total = sum(len(s) for s in groups)
    per = total / target
    buckets: List[Set[int]] = []
    cur: Set[int] = set()
    cur_count = 0
    for s in groups:
        cur |= s
        cur_count += len(s)
        # close the bucket once it reaches its proportional share (leaving room
        # for the remaining target slots)
        if cur_count >= per * (len(buckets) + 1) and len(buckets) < target - 1:
            buckets.append(cur)
            cur = set()
    if cur:
        buckets.append(cur)
    return buckets


def chain_from_ordered_groups(cnf, ordered: Sequence[Set[int]], *,
                              target_nodes: int, reporting=None,
                              prune: bool = True) -> DagModel:
    """Assemble an ordered list of clause-sets into a chain DAG, merging down to
    ``target_nodes`` if there are more groups than that."""
    nodes = merge_consecutive(list(ordered), target_nodes) if target_nodes else \
        [set(s) for s in ordered if s]
    if not nodes:
        nodes = [set(range(cnf.n_clauses))]
    return assemble.chain(cnf, nodes, reporting=reporting, prune=prune)
