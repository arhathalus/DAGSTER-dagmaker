"""Grid plugin: Sudoku / grid-CSP encodings.

When ``max_var`` factors as ``N*N*N`` (cell x value, as in the repo's
``Benchmarks/sudoku``) or ``N*N``, variables carry a row coordinate.  We slice by
**row bands** and chain them -- a generalisation of the hand-written
``Benchmarks/sudoku/dag_gen.py``.

Note: densely-coupled grids (Sudoku's column/box "all-different" constraints tie
every row together) may still yield wide separators; the pipeline scores this
candidate against the generic backend and keeps whichever is better, so a weak
grid split never makes the result worse.
"""

from __future__ import annotations

from typing import List, Optional, Set

from .. import autodetect
from .._common import chain_from_ordered_groups
from ...dagmodel import DagModel


class GridPlugin:
    name = "grid"

    def _shape(self, cnf, meta):
        if meta and isinstance(meta, dict) and meta.get("grid"):
            g = meta["grid"]
            return int(g["n"]), int(g.get("dims", 3)), 0.95
        n, conf = autodetect.detect_grid(cnf)
        if n is None:
            return None, None, 0.0
        dims = 3 if n ** 3 == cnf.max_var else 2
        return n, dims, conf

    def detect(self, cnf, meta=None) -> float:
        _, _, conf = self._shape(cnf, meta)
        return conf

    def _row_of_var(self, v: int, n: int, dims: int) -> int:
        # var numbering (Sudoku): v = (r-1)*N*N + (c-1)*N + (val-1) + 1
        if dims == 3:
            return (v - 1) // (n * n)
        return (v - 1) // n  # 2D: row-major

    def build(self, cnf, *, target_nodes=8, max_sep=30, reporting=None,
              prune=True, meta=None) -> Optional[DagModel]:
        n, dims, conf = self._shape(cnf, meta)
        if n is None or conf < 0.5:
            return None

        per_row: List[Set[int]] = [set() for _ in range(n)]
        for c in range(cnf.n_clauses):
            vs = cnf.clause_vars(c)
            if not vs:
                continue
            row = max(self._row_of_var(v, n, dims) for v in vs)
            row = min(row, n - 1)
            per_row[row].add(c)
        free = [c for c in range(cnf.n_clauses) if not cnf.clause_vars(c)]
        if free:
            tgt = max((i for i, b in enumerate(per_row) if b), default=n - 1)
            per_row[tgt].update(free)

        return chain_from_ordered_groups(cnf, per_row, target_nodes=target_nodes,
                                         reporting=reporting, prune=prune)
