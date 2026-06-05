#!/usr/bin/env python3
"""Build a spatial slab-chain DAG for counting Y-pentacube packings.

The exact-cover CNF (from ``ypack_gen.py``) emits, per cell in id order, one
at-least-one clause then its pairwise at-most-one clauses -- so clause indices
are grouped by cell, and a spatial slab is a *contiguous* clause range.

We cut the cube into slabs along an axis, put each cell's clauses in the slab
that owns the cell, and chain the slabs.  A placement that spans a slab boundary
appears in several slabs' clauses, so it becomes an interface (separator)
variable -- this is the transfer-matrix "profile" the count factorises over.
Counting is then: run dagster with ``-e 1`` (enumerate) along the chain.

Usage:
    ypack_dag.py --cnf ypack5.cnf --out ypack5_slab.dag [--axis z] [--thickness 1]
"""

from __future__ import annotations

import argparse
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "utilities", "dag-generator"))

import ypack_gen as g
from dagmaker.cnf import CnfIndex
from dagmaker import assemble, validate, scorer


def clause_to_cell(S: int):
    """Reconstruct, in CNF generation order, the cell each clause belongs to."""
    places = g.placements(S)
    n_cells = S ** 3
    cover = {c: [] for c in range(n_cells)}
    for pi, cells in enumerate(places):
        for c in cells:
            cover[g.cell_id(c, S)].append(pi + 1)
    mapping = []
    for cid in range(n_cells):
        mapping.append(cid)                       # at-least-one clause
        deg = len(cover[cid])
        mapping.extend([cid] * (deg * (deg - 1) // 2))  # pairwise at-most-one
    return mapping


def slab_of_cell(cid: int, S: int, axis: int, thickness: int) -> int:
    coord = (cid // (S * S), (cid // S) % S, cid % S)[axis]
    return coord // thickness


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Slab-chain DAG for Y-pentacube counting")
    ap.add_argument("--cnf", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--size", type=int, default=5, help="cube side S")
    ap.add_argument("--axis", choices=["x", "y", "z"], default="z")
    ap.add_argument("--thickness", type=int, default=1, help="layers per slab")
    ap.add_argument("--binary", default="./dagster")
    args = ap.parse_args()

    S = args.size
    axis = {"x": 0, "y": 1, "z": 2}[args.axis]
    cnf = CnfIndex.from_file(args.cnf)
    assert cnf.n_clauses == len(clause_to_cell(S)), \
        "CNF clause count {} != reconstructed {} (size mismatch?)".format(
            cnf.n_clauses, len(clause_to_cell(S)))

    c2cell = clause_to_cell(S)
    n_slabs = (S + args.thickness - 1) // args.thickness
    node_clauses = [[] for _ in range(n_slabs)]
    for ci, cell in enumerate(c2cell):
        node_clauses[slab_of_cell(cell, S, axis, args.thickness)].append(ci)
    node_clauses = [n for n in node_clauses if n]  # drop empties

    edges = [(i, i + 1) for i in range(len(node_clauses) - 1)]
    # count needs full packings distinguished -> report all placement variables
    model = assemble.from_topology(cnf, node_clauses, edges,
                                   reporting=set(range(1, cnf.max_var + 1)), prune=True)
    rep = validate.validate(model, cnf, strict=False)
    sc = scorer.score(model, cnf)
    model.write(args.out)

    print("slab-chain DAG along {} (thickness {}): {}".format(args.axis, args.thickness, sc))
    print("validation:", "OK" if rep.ok else "PROBLEMS")
    for p in rep.problems:
        print("  -", p)
    print("wrote", args.out)
    print("\ncount with:")
    print("  mpirun -n <W> {} -m 0 -e 1 -g 1 -c minisat {} {} -o out.sols"
          .format(args.binary, args.out, args.cnf))
    print("  (then: number of packings = number of solution lines in out.sols)")
