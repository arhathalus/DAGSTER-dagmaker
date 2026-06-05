"""Tier-C structure detection from raw CNF regularities.

Encoders number variables systematically, which leaves detectable fingerprints:

  * Time-indexed (BMC / planning): variables are laid out in ``T`` consecutive
    blocks of ``P`` "state" variables; almost every clause stays within one block
    or straddles two adjacent blocks (the transition relation).  We recover the
    period ``P`` by testing divisors of ``max_var`` and scoring how banded the
    clauses are.
  * Grid (Sudoku / CSP): ``max_var`` factors as ``N*N`` or ``N*N*N`` and clauses
    are block-regular.

These are heuristics; each returns a confidence so the dispatcher can fall back
to the generic backend when unsure.
"""

from __future__ import annotations

import math
from typing import List, Optional, Tuple


def _divisors(n: int) -> List[int]:
    ds = set()
    i = 1
    while i * i <= n:
        if n % i == 0:
            ds.add(i)
            ds.add(n // i)
        i += 1
    return sorted(ds)


def detect_period(cnf, sample: int = 4000,
                  min_blocks: int = 3, max_blocks: int = 100000) -> Tuple[Optional[int], float]:
    """Return ``(period, confidence)`` for a time-indexed layout, or ``(None, 0)``.

    ``period`` P partitions variables ``1..max_var`` into ``max_var/P`` blocks;
    confidence is the fraction of sampled clauses whose variables span at most two
    adjacent blocks.
    """
    if cnf.max_var < min_blocks or cnf.n_clauses == 0:
        return None, 0.0
    # candidate periods: divisors giving a sane block count, plus a few rounded
    cands = [d for d in _divisors(cnf.max_var)
             if min_blocks <= cnf.max_var // d <= max_blocks]
    if not cands:
        return None, 0.0
    step = max(1, cnf.n_clauses // sample)
    sampled = range(0, cnf.n_clauses, step)

    best_p, best_score = None, 0.0
    for p in cands:
        good = 0
        total = 0
        for c in sampled:
            vs = cnf.clause_vars(c)
            if len(vs) == 0:
                continue
            total += 1
            blocks = {(v - 1) // p for v in vs}
            if max(blocks) - min(blocks) <= 1:
                good += 1
        if total == 0:
            continue
        score = good / total
        # prefer larger periods (fewer, more meaningful blocks) on ties
        if score > best_score + 1e-9 or (abs(score - best_score) <= 1e-9 and
                                         best_p is not None and p > best_p):
            best_p, best_score = p, score
    return best_p, best_score


def detect_grid(cnf) -> Tuple[Optional[int], float]:
    """Return ``(N, confidence)`` if ``max_var`` looks like an ``N*N*N`` (or
    ``N*N``) grid encoding, else ``(None, 0)``."""
    n = cnf.max_var
    if n <= 0:
        return None, 0.0
    cube = round(n ** (1.0 / 3.0))
    for cand in (cube - 1, cube, cube + 1):
        if cand >= 2 and cand ** 3 == n:
            return cand, 0.9
    sq = int(math.isqrt(n))
    if sq >= 2 and sq * sq == n:
        return sq, 0.6
    return None, 0.0
