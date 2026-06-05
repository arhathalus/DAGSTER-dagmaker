#!/usr/bin/env python3
"""CNF generator: pack an SxSxS cube (default 5) with Y-pentacubes (exact cover).

The Y-pentacube: a straight bar of 4 unit cubes, plus a 5th cube attached to the
SECOND cube of the bar, perpendicular to it (volume 5).  For S=5 the cube has 125
cells and a packing uses exactly 25 pieces.

Encoding (standard exact cover):
  * One Boolean variable per legal PLACEMENT (orientation x translation that fits
    inside the cube).  Variable p is true iff that placement is used.
  * For every cell: EXACTLY-ONE of the placements covering it is used
    (at-least-one OR + pairwise at-most-one).  Exact cover over the cells implies
    exactly 25 pieces, so no separate piece-count constraint is needed.

The DIMACS output is annotated: each cell's clauses are grouped under a
``c CELL x y z`` comment, and a ``c PLACE <var>: <cells>`` map is emitted, so a
structure-aware DAG generator can recover the spatial layout.
"""

from __future__ import annotations

import argparse
import itertools
from typing import Dict, FrozenSet, List, Tuple

Cell = Tuple[int, int, int]

# Base Y-pentacube: bar of 4 along z at (0,0,0..3); foot on the 2nd cube (z=1),
# poking out in +x.
BASE: List[Cell] = [(0, 0, 0), (0, 0, 1), (0, 0, 2), (0, 0, 3), (1, 0, 1)]


def _det3(M) -> int:
    return (M[0][0] * (M[1][1] * M[2][2] - M[1][2] * M[2][1])
            - M[0][1] * (M[1][0] * M[2][2] - M[1][2] * M[2][0])
            + M[0][2] * (M[1][0] * M[2][1] - M[1][1] * M[2][0]))


def proper_rotations() -> List:
    """The 24 proper rotation matrices of the cube (signed permutations, det +1)."""
    mats = []
    for perm in itertools.permutations(range(3)):
        for signs in itertools.product((1, -1), repeat=3):
            M = [[0, 0, 0], [0, 0, 0], [0, 0, 0]]
            for i in range(3):
                M[i][perm[i]] = signs[i]
            if _det3(M) == 1:
                mats.append(M)
    return mats


def _apply(M, c: Cell) -> Cell:
    return tuple(M[i][0] * c[0] + M[i][1] * c[1] + M[i][2] * c[2] for i in range(3))


def _normalize(cells) -> FrozenSet[Cell]:
    mx = min(c[0] for c in cells)
    my = min(c[1] for c in cells)
    mz = min(c[2] for c in cells)
    return frozenset((c[0] - mx, c[1] - my, c[2] - mz) for c in cells)


def orientations() -> List[FrozenSet[Cell]]:
    """Distinct orientations of the Y-pentacube under the 24 rotations.

    The piece is planar, so its mirror image is reachable by an out-of-plane
    rotation -- the 24 rotations already include all chiral variants."""
    seen = set()
    for M in proper_rotations():
        seen.add(_normalize([_apply(M, c) for c in BASE]))
    return sorted(seen, key=lambda s: sorted(s))


def placements(S: int) -> List[List[Cell]]:
    """All legal (orientation x translation) placements fully inside the cube."""
    out: List[List[Cell]] = []
    for orient in orientations():
        ex = max(c[0] for c in orient)
        ey = max(c[1] for c in orient)
        ez = max(c[2] for c in orient)
        for tx in range(S - ex):
            for ty in range(S - ey):
                for tz in range(S - ez):
                    out.append([(c[0] + tx, c[1] + ty, c[2] + tz) for c in orient])
    return out


def cell_id(c: Cell, S: int) -> int:
    return c[0] * S * S + c[1] * S + c[2]


def generate(S: int, out_path: str) -> None:
    orients = orientations()
    places = placements(S)
    n_cells = S ** 3

    # cell -> list of placement variable ids (1-based) covering it
    cover: Dict[int, List[int]] = {cid: [] for cid in range(n_cells)}
    for pi, cells in enumerate(places):
        var = pi + 1
        for c in cells:
            cover[cell_id(c, S)].append(var)

    clauses: List[List[int]] = []
    # annotate clause groups per cell so a DAG generator can see the geometry
    clause_comments: Dict[int, str] = {}

    def add(cl, comment=None):
        if comment is not None:
            clause_comments[len(clauses)] = comment
        clauses.append(cl)

    for cid in range(n_cells):
        x, y, z = cid // (S * S), (cid // S) % S, cid % S
        vs = cover[cid]
        add(vs[:], "CELL {} {} {}".format(x, y, z))          # at-least-one
        for i in range(len(vs)):
            for j in range(i + 1, len(vs)):
                add([-vs[i], -vs[j]])                          # at-most-one (pairwise)

    n_vars = len(places)
    with open(out_path, "w") as f:
        f.write("c Y-pentacube packing of a {0}x{0}x{0} cube (exact cover)\n".format(S))
        f.write("c {} orientations, {} placements (variables), {} cells\n"
                .format(len(orients), len(places), n_cells))
        # placement map: var -> the 5 cells it occupies (as cell ids)
        for pi, cells in enumerate(places):
            f.write("c PLACE {}: {}\n".format(
                pi + 1, ",".join(str(cell_id(c, S)) for c in cells)))
        f.write("p cnf {} {}\n".format(n_vars, len(clauses)))
        for i, cl in enumerate(clauses):
            if i in clause_comments:
                f.write("c {}\n".format(clause_comments[i]))
            f.write(" ".join(map(str, cl)) + " 0\n")

    print("orientations: {}".format(len(orients)))
    print("placements (variables): {}".format(len(places)))
    print("cells: {}  clauses: {}".format(n_cells, len(clauses)))
    print("wrote {}".format(out_path))


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Y-pentacube cube-packing CNF generator")
    ap.add_argument("--size", type=int, default=5, help="cube side length S (default 5)")
    ap.add_argument("--out", default="ypack.cnf", help="output CNF path")
    ap.add_argument("--show", action="store_true", help="print the orientations and exit")
    args = ap.parse_args()
    if args.show:
        for i, o in enumerate(orientations()):
            print(i, sorted(o))
    else:
        generate(args.size, args.out)
