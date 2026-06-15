#!/usr/bin/env python3
"""Boundary-cell ("transfer-matrix") encoding of Y-pentacube packing + slab DAG.

Standard exact cover uses placement variables, which span up to 4 layers, so any
slab cut shares hundreds of placement variables -> a huge DAG interface.  This
encoding instead sweeps layer by layer and carries a thin BOUNDARY PROFILE.

For each cut after layer k and each cell c in layers k+1..k+3 (a piece protrudes
at most 3 layers), a carry variable

    f[k][c]  ==  "c is covered by a placement owned in some layer <= k"

is defined incrementally.  A placement is OWNED by the layer of its lowest cell
and appears only in that layer's clauses; its effect on higher layers travels
solely through the carry variables.  Hence:

  * node k (= layer k) owns: layer-k placements + carry vars f[k][.]
  * the interface between node k and node k+1 is exactly { f[k][c] } (<= 75 vars
    for S=5), the transfer-matrix profile -- not the placements.

Coverage of cell c (layer L): exactly-one of { f[L-1][c] } u { layer-L placements
covering c }.  Recurrence: f[k][c] <-> f[k-1][c] OR (layer-k placements covering
c), with at-most-one among those contributors so two pieces can't overlap at c.

All carry variables are functionally determined by the placements, so the model
count is identical to the plain encoding (each packing extends to exactly one
assignment).  Writes both the CNF and the slab-chain DAG.
"""

from __future__ import annotations

import argparse
import os
import sys
from collections import defaultdict
from typing import Dict, List

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "utilities", "dag-generator"))

import ypack_gen as g


def exactly_one(lits: List[int]) -> List[List[int]]:
    cls = [list(lits)]                      # at-least-one
    for a in range(len(lits)):
        for b in range(a + 1, len(lits)):
            cls.append([-lits[a], -lits[b]])  # at-most-one (pairwise)
    return cls


def build(S: int, cnf_out: str, dag_out: str) -> None:
    places = g.placements(S)
    P = len(places)
    minz = [min(c[2] for c in cells) for cells in places]

    # cell -> {owner_layer j: [placement vars with min-z j that cover the cell]}
    cover: Dict[int, Dict[int, List[int]]] = defaultdict(lambda: defaultdict(list))
    for i, cells in enumerate(places):
        j = minz[i]
        for c in cells:
            cover[g.cell_id(c, S)][j].append(i + 1)

    # carry variables f[k][cell] for k in 0..S-2, cell in layers k+1..min(k+3,S-1)
    nv = P
    fvar: Dict[tuple, int] = {}
    for k in range(S - 1):
        for L in range(k + 1, min(k + 3, S - 1) + 1):
            for x in range(S):
                for y in range(S):
                    nv += 1
                    fvar[(k, g.cell_id((x, y, L), S))] = nv
    n_vars = nv

    # clauses, grouped by owning node (= layer)
    node_clauses: List[List[List[int]]] = [[] for _ in range(S)]

    def add(k, cl):
        node_clauses[k].append(cl)

    for k in range(S):
        # (1) coverage of each cell at layer k
        for x in range(S):
            for y in range(S):
                cid = g.cell_id((x, y, k), S)
                sources = []
                if (k - 1, cid) in fvar:
                    sources.append(fvar[(k - 1, cid)])   # covered from below
                sources += cover[cid].get(k, [])         # a layer-k piece covers it
                for cl in exactly_one(sources):
                    add(k, cl)
        # (2) recurrence defining the carry for cells ahead (layers k+1..k+3)
        for L in range(k + 1, min(k + 3, S - 1) + 1):
            for x in range(S):
                for y in range(S):
                    cid = g.cell_id((x, y, L), S)
                    fk = fvar[(k, cid)]
                    contrib = []
                    if (k - 1, cid) in fvar:
                        contrib.append(fvar[(k - 1, cid)])
                    contrib += cover[cid].get(k, [])
                    if not contrib:
                        add(k, [-fk])                    # nothing can cover it yet
                        continue
                    add(k, [-fk] + contrib)              # fk -> OR(contrib)
                    for c in contrib:
                        add(k, [-c, fk])                 # contrib -> fk
                    for a in range(len(contrib)):        # at most one contributor
                        for b in range(a + 1, len(contrib)):
                            add(k, [-contrib[a], -contrib[b]])

    # flatten in node order, recording each node's contiguous clause range
    flat: List[List[int]] = []
    ranges: List[range] = []
    for k in range(S):
        start = len(flat)
        flat.extend(node_clauses[k])
        ranges.append(range(start, len(flat)))

    # write CNF (node markers + placement map for the verifier)
    with open(cnf_out, "w") as f:
        f.write("c Y-pentacube packing {0}x{0}x{0}: boundary-cell (transfer-matrix) encoding\n".format(S))
        f.write("c {} placement vars, {} carry vars, {} clauses\n".format(P, n_vars - P, len(flat)))
        for pi, cells in enumerate(places):
            f.write("c PLACE {}: {}\n".format(pi + 1, ",".join(str(g.cell_id(c, S)) for c in cells)))
        f.write("p cnf {} {}\n".format(n_vars, len(flat)))
        for k in range(S):
            f.write("c NODE {}\n".format(k))
            for cl in node_clauses[k]:
                f.write(" ".join(map(str, cl)) + " 0\n")

    # build the slab-chain DAG from the recorded ranges
    from dagmaker.cnf import CnfIndex
    from dagmaker import assemble, validate, scorer
    cnf = CnfIndex.from_file(cnf_out)
    node_clause_sets = [list(r) for r in ranges]
    edges = [(i, i + 1) for i in range(S - 1)]
    # report the terminal layer's placements only (local to terminal -> no
    # placement is dragged across the thin carry interface)
    terminal_report = set(cover_layer_placements(places, minz, S - 1))
    model = assemble.from_topology(cnf, node_clause_sets, edges,
                                   reporting=terminal_report or None, prune=True)
    rep = validate.validate(model, cnf, strict=False)
    sc = scorer.score(model, cnf)
    model.write(dag_out)

    print("boundary encoding: {} vars ({} placements + {} carry), {} clauses"
          .format(n_vars, P, n_vars - P, len(flat)))
    print("slab DAG: {}".format(sc))
    print("validation:", "OK" if rep.ok else "PROBLEMS")
    for p in rep.problems[:6]:
        print("  -", p)
    print("wrote {} and {}".format(cnf_out, dag_out))


def cover_layer_placements(places, minz, layer):
    return [i + 1 for i in range(len(places)) if minz[i] == layer]


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Boundary-cell encoding + slab DAG for Y-pentacube packing")
    ap.add_argument("--size", type=int, default=5)
    ap.add_argument("--cnf", default="ypack5_b.cnf")
    ap.add_argument("--dag", default="ypack5_b.dag")
    args = ap.parse_args()
    build(args.size, args.cnf, args.dag)
