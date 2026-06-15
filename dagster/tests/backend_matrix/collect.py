#!/usr/bin/env python3
"""Collect a backend_matrix HPC run into a correctness + backend-scoreboard report.

After `matrix.py --profile hpc --emit-hpc DIR && sbatch DIR/matrix_array.slurm`
completes, point this at DIR. It joins what the run wrote:

  cells.tsv          task -> (problem, family, size, dag, backend, ranks, expected)  [emit]
  cell_<task>.out    a 'BACKENDMATRIX task=.. rc=.. wall=..' line                    [each task]
  sol_<task>.txt     the solution (non-empty => SAT)                                 [dagster -o]

and reports two things:

  CORRECTNESS  per problem, every backend on every DAG must agree on SAT/UNSAT.
               Ground truth = the single-node DAG's verdict (CNF undecomposed).
               Any definite verdict disagreeing with it is a BUG (flagged, non-zero exit).
               The generator's declared `expected` is shown for context (advisory).

  SCOREBOARD   per backend: #solved / #timeout / #error and median solve time, so you
               can see which backend is fastest/most-robust at scale.

  python3 collect.py DIR
  python3 collect.py DIR --csv out.csv      # also dump the per-cell table

A task with no BACKENDMATRIX line hit the SLURM --time limit (verdict TIMEOUT).
"""

import argparse
import csv
import os
import re
import statistics
import sys


def task_result(d, task):
    """Return (verdict, wall_seconds, rc). verdict in SAT/UNSAT/ERR(n)/TIMEOUT."""
    out = os.path.join(d, "cell_%s.out" % task)
    rc, wall = None, None
    if os.path.exists(out):
        m = re.search(r"BACKENDMATRIX task=\S+ rc=(-?\d+) wall=(\d+)", open(out, errors="replace").read())
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
    ap.add_argument("dir", help="the backend_matrix --emit-hpc directory")
    ap.add_argument("--csv", help="also write the per-cell table to this CSV")
    args = ap.parse_args()

    cells = os.path.join(args.dir, "cells.tsv")
    if not os.path.exists(cells):
        sys.exit("no cells.tsv in %s (is this a backend_matrix --emit-hpc dir?)" % args.dir)

    recs = []
    with open(cells) as f:
        for r in csv.DictReader(f, delimiter="\t"):
            v, w, rc = task_result(args.dir, r["task"])
            r["verdict"], r["wall"], r["rc"] = v, w, rc
            recs.append(r)

    by_problem = {}
    for r in recs:
        by_problem.setdefault(r["problem"], []).append(r)

    DEFINITE = ("SAT", "UNSAT")

    def ground_truth(cs):
        gt_set = {r["verdict"] for r in cs if r["dag"] == "single" and r["verdict"] in DEFINITE}
        if not gt_set:                                  # no single-node cell decided -> any consensus
            gt_set = {r["verdict"] for r in cs if r["verdict"] in DEFINITE}
        return gt_set.pop() if len(gt_set) == 1 else ("CONFLICT" if len(gt_set) > 1 else "?")

    gt_by_problem = {p: ground_truth(cs) for p, cs in by_problem.items()}

    # ---- CORRECTNESS ----------------------------------------------------
    print("=== CORRECTNESS (every backend/DAG must agree per problem) ===\n")
    failures = 0
    for p in sorted(by_problem):
        cs = by_problem[p]
        expected = cs[0].get("expected", "?")
        gt = gt_by_problem[p]

        disagree = [r for r in cs if r["verdict"] in DEFINITE and gt in DEFINITE and r["verdict"] != gt]
        undecided = [r for r in cs if r["verdict"] not in DEFINITE]
        note = ""
        if expected != "?" and gt in DEFINITE and expected != gt:
            note = "  (declared expected=%s; trusting measured)" % expected
        status = "OK all %d cell(s) agree=%s" % (len(cs), gt)
        if gt == "CONFLICT":
            status = "CONFLICT on single-node DAG -- backends disagree on the undecomposed CNF!"; failures += 1
        elif disagree:
            status = "%d/%d DISAGREE vs ground truth %s" % (len(disagree), len(cs), gt); failures += len(disagree)
        print("  %-24s %s%s" % (p, status, note))
        for r in disagree:
            print("       <-- %-13s dag=%-8s says %s (expected %s)" % (r["backend"], r["dag"], r["verdict"], gt))
        if undecided:
            tos = ["%s/%s=%s" % (r["backend"], r["dag"], r["verdict"]) for r in undecided]
            print("       (undecided: %s)" % ", ".join(tos[:8]) + (" ..." if len(tos) > 8 else ""))

    # ---- SCOREBOARD -----------------------------------------------------
    print("\n=== BACKEND SCOREBOARD ===\n")
    backends = {}
    for r in recs:
        b = backends.setdefault(r["backend"], dict(solved=0, timeout=0, err=0, walls=[]))
        if r["verdict"] in DEFINITE:
            b["solved"] += 1
            if r["wall"] is not None:
                b["walls"].append(r["wall"])
        elif r["verdict"] == "TIMEOUT":
            b["timeout"] += 1
        else:
            b["err"] += 1
    print("  %-14s %7s %8s %6s %9s %9s" % ("backend", "solved", "timeout", "err", "median(s)", "total(s)"))
    for b in sorted(backends, key=lambda b: -backends[b]["solved"]):
        s = backends[b]
        med = "%.0f" % statistics.median(s["walls"]) if s["walls"] else "-"
        tot = "%.0f" % sum(s["walls"]) if s["walls"] else "-"
        print("  %-14s %7d %8d %6d %9s %9s" % (b, s["solved"], s["timeout"], s["err"], med, tot))

    # ---- per-problem fastest --------------------------------------------
    print("\n=== fastest backend per problem (correct verdicts only) ===\n")
    for p in sorted(by_problem):
        gt = gt_by_problem[p]
        # only count cells that agree with ground truth -- a fast WRONG answer is not a win
        fin = [r for r in by_problem[p] if r["verdict"] in DEFINITE and r["wall"] is not None
               and (gt not in DEFINITE or r["verdict"] == gt)]
        if not fin:
            print("  %-24s (none finished)" % p); continue
        best = min(fin, key=lambda r: r["wall"])
        print("  %-24s %6ds  %s / %s" % (p, best["wall"], best["backend"], best["dag"]))

    print()
    print("CORRECTNESS: %s" % ("PASS -- all backends agree everywhere" if failures == 0
                               else "%d disagreement(s) -- INVESTIGATE (a backend or the DAG machinery is wrong)" % failures))

    if args.csv:
        cols = ["problem", "family", "size", "dag", "backend", "ranks", "expected", "verdict", "wall", "rc"]
        with open(args.csv, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=cols, extrasaction="ignore")
            w.writeheader(); w.writerows(recs)
        print("wrote %s" % args.csv)

    sys.exit(1 if failures else 0)


if __name__ == "__main__":
    main()
