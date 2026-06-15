#!/usr/bin/env python3
"""Collect a cube_matrix HPC run into a plain-vs-share speedup table.

After `matrix.py --profile hpc --emit-hpc DIR ... && sbatch DIR/cube_array.slurm`
completes, point this at DIR. It joins three things written by the run:

  cells.tsv           task id -> (problem, mode, ranks, cubes)   [from emit]
  cell_<task>.out     a 'CUBEMATRIX task=.. rc=.. wall=..' line  [from each array task]
  sol_<task>.txt      the solution (non-empty => SAT)            [from dagster -o]

and prints, per problem, the wall time of each mode plus the share-vs-plain
speedup, flagging any verdict disagreement between modes (which would be a bug).

  python3 collect.py DIR              # table to stdout
  python3 collect.py DIR --csv out.csv

A task with no CUBEMATRIX line hit the SLURM --time limit (verdict TIMEOUT).
"""

import argparse
import csv
import os
import re
import sys

PLAIN, SHARE = "cadical", "cadical+share"   # the modes compared by --modes 5,10


def task_result(d, task, expected_cubes=None):
    """Return (verdict, wall_seconds, rc). verdict in SAT/UNSAT/ERR(n)/TIMEOUT."""
    out = os.path.join(d, "cell_%s.out" % task)
    rc, wall = None, None
    if os.path.exists(out):
        m = re.search(r"CUBEMATRIX task=\S+ rc=(-?\d+) wall=(\d+)", open(out, errors="replace").read())
        if m:
            rc, wall = int(m.group(1)), int(m.group(2))
    if rc is None:
        return "TIMEOUT", wall, rc          # no completion line => killed by --time
    if rc != 0:
        return "ERR(%d)" % rc, wall, rc
    sol = os.path.join(d, "sol_%s.txt" % task)
    v = "SAT" if (os.path.exists(sol) and os.path.getsize(sol) > 0) else "UNSAT"
    return v, wall, rc


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("dir", help="the cube_matrix --emit-hpc directory")
    ap.add_argument("--csv", help="also write the per-problem table to this CSV")
    args = ap.parse_args()

    cells = os.path.join(args.dir, "cells.tsv")
    if not os.path.exists(cells):
        sys.exit("no cells.tsv in %s (is this a cube_matrix --emit-hpc dir?)" % args.dir)

    # task id -> metadata, enriched with verdict/wall
    by_problem = {}          # problem -> {mode -> result dict}
    with open(cells) as f:
        for r in csv.DictReader(f, delimiter="\t"):
            verdict, wall, rc = task_result(args.dir, r["task"])
            by_problem.setdefault(r["problem"], {})[r["mode"]] = dict(
                mode=r["mode"], ranks=int(r["ranks"]), cubes=int(r.get("cubes", 0) or 0),
                verdict=verdict, wall=wall, rc=rc)

    hdr = "%-16s %5s %6s  %-8s %8s   %-8s %8s   %8s  %s" % (
        "problem", "ranks", "cubes", "plain", "plain(s)", "share", "share(s)", "speedup", "note")
    print(hdr); print("-" * len(hdr))
    csv_rows, mismatches, speedups = [], 0, []
    for p in sorted(by_problem):
        m = by_problem[p]
        plain, share = m.get(PLAIN), m.get(SHARE)
        # ranks/cubes for display (prefer plain's)
        ref = plain or share or next(iter(m.values()))
        pv = plain["verdict"] if plain else "-"
        sv = share["verdict"] if share else "-"
        pw = plain["wall"] if plain else None
        sw = share["wall"] if share else None
        note = ""
        sp = ""
        if plain and share and pv in ("SAT", "UNSAT") and sv in ("SAT", "UNSAT"):
            if pv != sv:
                note = "<-- VERDICT MISMATCH (bug!)"; mismatches += 1
            elif pw and sw and sw > 0:
                ratio = pw / sw
                sp = "%.2fx" % ratio
                speedups.append(ratio)
        elif plain and share and (pv.startswith(("TIMEOUT", "ERR")) or sv.startswith(("TIMEOUT", "ERR"))):
            note = "(one side did not finish)"
        print("%-16s %5d %6d  %-8s %8s   %-8s %8s   %8s  %s" % (
            p, ref["ranks"], ref["cubes"], pv, pw if pw is not None else "-",
            sv, sw if sw is not None else "-", sp, note))
        csv_rows.append(dict(problem=p, ranks=ref["ranks"], cubes=ref["cubes"],
                             plain_verdict=pv, plain_secs=pw, share_verdict=sv,
                             share_secs=sw, speedup=sp.rstrip("x") if sp else ""))

    # summary
    print()
    if speedups:
        geo = 1.0
        for s in speedups:
            geo *= s
        geo **= (1.0 / len(speedups))
        print("share vs plain over %d finished pair(s): geomean %.2fx, best %.2fx, worst %.2fx"
              % (len(speedups), geo, max(speedups), min(speedups)))
    else:
        print("no problem finished under BOTH plain and share (raise --time, or use harder/easier instances)")
    if mismatches:
        print("WARNING: %d verdict mismatch(es) between plain and share -- investigate (clause sharing must be sound)" % mismatches)

    if args.csv:
        with open(args.csv, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(csv_rows[0].keys())); w.writeheader(); w.writerows(csv_rows)
        print("wrote %s" % args.csv)


if __name__ == "__main__":
    main()
