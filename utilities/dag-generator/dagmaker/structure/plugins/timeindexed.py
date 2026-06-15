"""Time-indexed plugin: BMC / planning unfoldings.

Variables are laid out as ``T`` consecutive blocks of ``P`` state variables (step
0, step 1, ...).  The natural decomposition is a **chain, one node per step**,
where the separator between steps is just the state variables linking ``t`` and
``t+1`` -- small, uniform, and far better than anything the generic backend finds
on the flattened primal graph.

The period ``P`` comes from metadata (``meta['time_index']['period']``) when
available, else from :func:`autodetect.detect_period`.
"""

from __future__ import annotations

from typing import List, Optional, Set

from .. import autodetect
from .._common import chain_from_ordered_groups
from ...dagmodel import DagModel


class TimeIndexedPlugin:
    name = "timeindexed"

    def _period(self, cnf, meta) -> (Optional[int], float):
        if meta and isinstance(meta, dict):
            ti = meta.get("time_index")
            if ti and ti.get("period"):
                return int(ti["period"]), 1.0
        return autodetect.detect_period(cnf)

    def detect(self, cnf, meta=None) -> float:
        _, conf = self._period(cnf, meta)
        return conf

    def build(self, cnf, *, target_nodes=8, max_sep=30, reporting=None,
              prune=True, meta=None) -> Optional[DagModel]:
        period, conf = self._period(cnf, meta)
        if not period or conf < 0.5:
            return None
        n_blocks = (cnf.max_var + period - 1) // period

        # assign each clause to the block of its highest variable (so a clause
        # straddling t and t+1 belongs to t+1, where both states are present)
        per_block: List[Set[int]] = [set() for _ in range(n_blocks)]
        for c in range(cnf.n_clauses):
            vs = cnf.clause_vars(c)
            if not vs:
                continue
            blk = (max(vs) - 1) // period
            per_block[blk].add(c)
        # variable-less clauses -> last non-empty block
        free = [c for c in range(cnf.n_clauses) if not cnf.clause_vars(c)]
        if free:
            tgt = max((i for i, b in enumerate(per_block) if b), default=n_blocks - 1)
            per_block[tgt].update(free)

        return chain_from_ordered_groups(cnf, per_block, target_nodes=target_nodes,
                                         reporting=reporting, prune=prune)
