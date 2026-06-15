#!/usr/bin/env python3
"""Generators for a corpus of SAT problems with KNOWN structure.

Each generator returns ``(clauses, n_vars, meta)`` where ``clauses`` is a list of
signed-literal lists and ``meta`` describes the structure + the dagmaker backend
expected to exploit it.  Used by ``run_corpus.py`` (regression harness) and can
also be written to disk as CNF + ``.meta`` sidecars.

Structure classes (and the backend each should reward):
  chain_bmc       -> structure:timeindexed / ordering   (sequential, thin chain)
  grid_coloring   -> ordering / elimination             (grid graph, sep ~ side)
  tree_constraints-> biconnected                         (tree = all bridges, sep 1)
  modular         -> community                           (planted clusters)
  components      -> elimination                          (disjoint -> free parallel)
  expander        -> cutset                               (random k-SAT, no structure)
  pigeonhole      -> cutset / single                      (dense symmetric, UNSAT)
  banded_xor      -> ordering / elimination               (windowed parity, banded)
"""

from __future__ import annotations

import random
from itertools import combinations
from typing import Dict, List, Tuple

Clauses = List[List[int]]


def _meta(family: str, recommended: str, **extra) -> Dict:
    m = {"family": family, "recommended": recommended}
    m.update(extra)
    return m


def chain_bmc(T: int = 12, P: int = 8, seed: int = 1) -> Tuple[Clauses, int, Dict]:
    random.seed(seed)
    def var(s, i): return s * P + i
    cl: Clauses = []
    for t in range(T):
        for _ in range(P + 2):
            a, b, c = random.sample(range(1, P + 1), 3)
            cl.append([random.choice([1, -1]) * var(t, a),
                       random.choice([1, -1]) * var(t, b),
                       random.choice([1, -1]) * var(t, c)])
        if t < T - 1:
            for i in range(1, P + 1):
                cl.append([-var(t, i), var(t + 1, i)])     # state transition
    return cl, T * P, _meta("sequential", "timeindexed", period=P, steps=T)


def grid_coloring(S: int = 6, seed: int = 1) -> Tuple[Clauses, int, Dict]:
    # one boolean per cell; adjacent cells must differ (2-colour grid).
    def v(x, y): return x * S + y + 1
    cl: Clauses = []
    for x in range(S):
        for y in range(S):
            if x + 1 < S:
                cl += [[v(x, y), v(x + 1, y)], [-v(x, y), -v(x + 1, y)]]
            if y + 1 < S:
                cl += [[v(x, y), v(x, y + 1)], [-v(x, y), -v(x, y + 1)]]
    return cl, S * S, _meta("grid", "ordering", side=S)


def tree_constraints(n: int = 30, seed: int = 1) -> Tuple[Clauses, int, Dict]:
    random.seed(seed)
    cl: Clauses = []
    for v in range(2, n + 1):
        p = random.randint(1, v - 1)                       # random tree edge
        cl += [[v, p], [-v, -p]]                            # differ along edge
    return cl, n, _meta("tree", "biconnected", nodes=n)


def modular(k: int = 3, sz: int = 8, seed: int = 2) -> Tuple[Clauses, int, Dict]:
    random.seed(seed)
    groups = [list(range(i * sz + 1, (i + 1) * sz + 1)) for i in range(k)]
    cl: Clauses = []
    for g in groups:
        for _ in range(5 * sz):
            a, b, c = random.sample(g, 3)
            cl.append([random.choice([1, -1]) * a, random.choice([1, -1]) * b,
                       random.choice([1, -1]) * c])
    for i in range(k - 1):                                  # one bridge per pair
        cl.append([groups[i][-1], groups[i + 1][0]])
    return cl, k * sz, _meta("modular", "community", clusters=k, size=sz)


def components(k: int = 4, sz: int = 10, seed: int = 3) -> Tuple[Clauses, int, Dict]:
    random.seed(seed)
    cl: Clauses = []
    for i in range(k):
        base = i * sz
        for _ in range(4 * sz):
            a, b, c = random.sample(range(base + 1, base + sz + 1), 3)
            cl.append([random.choice([1, -1]) * a, random.choice([1, -1]) * b,
                       random.choice([1, -1]) * c])
    return cl, k * sz, _meta("components", "elimination", parts=k)


def expander(n: int = 40, ratio: float = 4.2, seed: int = 4) -> Tuple[Clauses, int, Dict]:
    random.seed(seed)
    cl: Clauses = []
    for _ in range(int(ratio * n)):
        a, b, c = random.sample(range(1, n + 1), 3)
        cl.append([random.choice([1, -1]) * a, random.choice([1, -1]) * b,
                   random.choice([1, -1]) * c])
    return cl, n, _meta("random", "cutset", ratio=ratio)


def pigeonhole(holes: int = 6, seed: int = 0) -> Tuple[Clauses, int, Dict]:
    pigeons = holes + 1
    def v(p, h): return p * holes + h + 1
    cl: Clauses = []
    for p in range(pigeons):                                # each pigeon in a hole
        cl.append([v(p, h) for h in range(holes)])
    for h in range(holes):                                  # no two pigeons share
        for p, q in combinations(range(pigeons), 2):
            cl.append([-v(p, h), -v(q, h)])
    return cl, pigeons * holes, _meta("symmetric", "cutset", holes=holes, unsat=True)


def banded_xor(n: int = 40, k: int = 3, seed: int = 5) -> Tuple[Clauses, int, Dict]:
    # parity constraints over consecutive windows -> banded (chain-like) structure
    random.seed(seed)
    cl: Clauses = []
    for start in range(1, n - k + 2):
        win = list(range(start, start + k))
        b = random.randint(0, 1)
        for signs in range(2 ** k):
            neg = bin(signs).count("1")
            if (neg % 2) == (0 if b == 0 else 1):
                continue                                    # keep clauses of right parity
            cl.append([(-1 if (signs >> i) & 1 else 1) * win[i] for i in range(k)])
    return cl, n, _meta("algebraic", "ordering", window=k)


GENERATORS = {
    "chain_bmc": chain_bmc,
    "grid_coloring": grid_coloring,
    "tree_constraints": tree_constraints,
    "modular": modular,
    "components": components,
    "expander": expander,
    "pigeonhole": pigeonhole,
    "banded_xor": banded_xor,
}
