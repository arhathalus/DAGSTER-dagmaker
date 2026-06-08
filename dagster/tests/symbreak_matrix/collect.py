#!/usr/bin/env python3
"""Collect a symbreak_matrix HPC run: verdict-preservation (soundness) + speedup.

After `matrix.py --profile hpc --emit-hpc DIR && sbatch DIR/symbreak_array.slurm`
completes, point this at DIR. It joins what the run wrote:

  cells.tsv          task -> (problem, backend, level, ranks, DAG shape)   [emit]
  cell_<task>.out    a 'SYMBREAK task=.. rc=.. wall=..' line               [each task]
  sol_<task>.txt     the solution (non-empty => SAT)                       [dagster -o]

and reports, per (problem, backend) across the four symmetry-breaking levels
(none / light / full / dag):

  SOUNDNESS  every level must agree on SAT/UNSAT. 'none' (unbroken) is the
             reference; a level disagreeing with it means symmetry breaking
             changed the answer == UNSOUND (flagged, non-zero exit).
  SPEEDUP    each level's wall time and its speedup vs 'none' -- does breaking
             help, and which level wins? (the dag-aware level trades a smaller
             search for a more parallel DAG, which is the whole point.)
  SHAPE      nodes / max_sep / parallel_width / kept-vs-dropped breaking clauses
             per level, so you can see what each level did to the DAG.

  python3 collect.py DIR
  python3 collect.py DIR --csv out.csv

A task with no SYMBREAK line hit the SLURM --time limit (verdict TIMEOUT).
"""

import argparse
import csv
import os
import re
import sys

LEVELS = ["none", "light", "full", "dag"]     # display order
DEFINITE = ("SAT", "UNSAT")


def task_result(d, task):
    out = os.path.join(d, "cell_%s.out" % task)
    rc, wall = None, None
    if os.path.exists(out):
        m = re.search(r"SYMBREAK task=\S+ rc=(-?\d+) wall=(\d+)", open(out, errors="replace").read())
        if m:
            rc, wall = int(m.group(1)), int(m.group(2))
    if rc is None:
        return "TIMEOUT", wall, rc
    if rc != 0:
        return "ERR(%d)" % rc, wall, rc
    sol = os.path.join(d, "sol_%s.txt" % task)
    return ("SAT" if (os.path.exists(sol) and os.path.getsize(sol) > 0) else "UNSAT"), wall, rc


def shape(r):
    if r.get("kept") not in (None, ""):
        brk = "kept %s/dropped %s" % (r.get("kept"), r.get("dropped"))
    elif r.get("generators") not in (None, ""):
        brk = "%s gens" % r.get("generators")
    else:
        brk = "-"
    return "n=%s sep=%s pw=%s  %s" % (r.get("nodes") or "?", r.get("max_sep") or "?",
                                      r.get("parallel_width") or "?", brk)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("dir")
    ap.add_argument("--csv")
    args = ap.parse_args()

    cells = os.path.join(args.dir, "cells.tsv")
    if not os.path.exists(cells):
        sys.exit("no cells.tsv in %s (is this a symbreak_matrix --emit-hpc dir?)" % args.dir)

    recs = []
    with open(cells) as f:
        for r in csv.DictReader(f, delimiter="\t"):
            v, w, rc = task_result(args.dir, r["task"])
            r["verdict"], r["wall"], r["rc"] = v, w, rc
            recs.append(r)

    # group by (problem, backend) -> {level: rec}
    groups = {}
    for r in recs:
        groups.setdefault((r["problem"], r["backend"]), {})[r["level"]] = r

    unsound = 0
    helped = 0          # groups where some breaking level beat 'none'
    best_level_count = {}
    for (prob, backend) in sorted(groups):
        lv = groups[(prob, backend)]
        base = lv.get("none")
        base_v = base["verdict"] if base else "?"
        base_w = base["wall"] if base and base["wall"] is not None else None
        print("== %-14s [%s]   baseline none=%s%s ==" %
              (prob, backend, base_v, ("  %ds" % base_w if base_w is not None else "")))
        print("   %-6s %-8s %8s %9s   %s" % ("level", "verdict", "wall", "speedup", "DAG shape / breaking"))
        finished = []
        for level in LEVELS:
            r = lv.get(level)
            if not r:
                continue
            v, w = r["verdict"], r["wall"]
            flag = ""
            if v in DEFINITE and base_v in DEFINITE and v != base_v:
                flag = "  <-- UNSOUND (breaking changed the verdict!)"; unsound += 1
            sp = ""
            if base_w and w and w > 0 and v in DEFINITE:
                sp = "%.2fx" % (base_w / w)
            # count toward "fastest" only if sound (agrees with the unbroken baseline)
            if v in DEFINITE and w is not None and (base_v not in DEFINITE or v == base_v):
                finished.append((w, level))
            print("   %-6s %-8s %8s %9s   %s%s" %
                  (level, v, (w if w is not None else "-"), sp, shape(r), flag))
        # which level was fastest (correct) for this (problem,backend)?
        if finished:
            best = min(finished)
            best_level_count[best[1]] = best_level_count.get(best[1], 0) + 1
            if base_w and best[0] < base_w:
                helped += 1
        print()

    # ---- summary --------------------------------------------------------
    print("=== SUMMARY ===")
    print("fastest level (count of (problem,backend) groups it won):")
    for lvl in LEVELS:
        if lvl in best_level_count:
            print("   %-6s %d" % (lvl, best_level_count[lvl]))
    ngroups = len(groups)
    print("breaking beat 'none' in %d / %d group(s)." % (helped, ngroups))
    print("SOUNDNESS: %s" % ("PASS -- symmetry breaking was verdict-preserving everywhere"
                             if unsound == 0 else
                             "%d UNSOUND case(s) -- a breaking level changed SAT/UNSAT; INVESTIGATE" % unsound))

    if args.csv:
        cols = ["problem", "backend", "level", "ranks", "verdict", "wall",
                "nodes", "max_sep", "parallel_width", "kept", "dropped", "generators"]
        with open(args.csv, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=cols, extrasaction="ignore")
            w.writeheader(); w.writerows(recs)
        print("wrote %s" % args.csv)

    sys.exit(1 if unsound else 0)


if __name__ == "__main__":
    main()
