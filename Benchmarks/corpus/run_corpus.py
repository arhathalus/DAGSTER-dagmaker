#!/usr/bin/env python3
"""Corpus regression harness: generate each structured instance, run dagmaker,
and check the structure-appropriate backend finds a small separator.

  python run_corpus.py            # run the regression, print a table
  python run_corpus.py --write D  # also write <D>/<class>.cnf + .meta + manifest.json

Uses dagmaker's pipeline directly (no subprocess) with search-style reporting
(reporting=set()), so separators reflect the structure rather than full-output
carrying.  Exits non-zero if any class regresses.
"""

from __future__ import annotations

import argparse
import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "utilities", "dag-generator"))

from dagmaker.cnf import CnfIndex
from dagmaker import pipeline
import generators as G

# class -> (acceptable winning-backend name prefixes, separator bound given meta)
EXPECT = {
    "chain_bmc":        (("structure:timeindexed", "ordering", "elimination", "gates"),
                         lambda m: 3 * m["period"]),
    "grid_coloring":    (("ordering", "elimination"), lambda m: 4 * m["side"]),
    "tree_constraints": (("biconnected", "elimination", "ordering"), lambda m: 3),
    "modular":          (("community", "biconnected"), lambda m: 2 * m["clusters"]),
    "components":       (("elimination", "ordering"), lambda m: 1),
    "expander":         (("cutset", "single"), lambda m: 10 ** 9),
    "pigeonhole":       (("cutset", "single"), lambda m: 10 ** 9),
    "banded_xor":       (("ordering", "elimination", "gates"), lambda m: 12),
}


def candidate(result, *prefixes):
    """Best (smallest max_sep) valid candidate whose name starts with a prefix."""
    cands = [c for c in result.candidates if c.report.ok
             and any(c.name.startswith(p) for p in prefixes)]
    return min(cands, key=lambda c: c.score.max_sep_width) if cands else None


def run(write=None, verbose=True):
    manifest = {}
    failures = 0
    if verbose:
        print("%-17s %-22s %7s  %7s  %5s  %s" %
              ("class", "best backend", "best_sep", "exp_sep", "ok", "note"))
        print("-" * 80)

    for name, gen in G.GENERATORS.items():
        clauses, nv, meta = gen()
        cnf = CnfIndex.from_clauses(clauses, nv)
        res = pipeline.generate(cnf, reporting=set(), signed_clauses=clauses,
                                max_sep=10 ** 9, target_nodes=8)
        prefixes, bound_fn = EXPECT[name]
        bound = bound_fn(meta)
        best = res.best
        exp = candidate(res, *prefixes)
        exp_sep = exp.score.max_sep_width if exp else None

        # negative controls: correct outcome is "no genuine structure" -- there is
        # NO multi-node candidate with a small separator (any split is wide).
        if name in ("expander", "pigeonhole"):
            neg_thr = max(10, nv // 5)
            small_multinode = [c for c in res.candidates if c.report.ok
                               and c.score.num_nodes >= 2
                               and c.score.max_sep_width <= neg_thr]
            ok = not small_multinode
            exp_sep = min((c.score.max_sep_width for c in res.candidates
                           if c.report.ok and c.score.num_nodes >= 2), default=0)
            note = "best multi-node sep={} (> {} -> no structure)".format(exp_sep, neg_thr)
        # disjoint components: the win is free PARALLELISM (width), not separator.
        elif name == "components":
            ec = candidate(res, "elimination", "ordering")
            ok = ec is not None and ec.score.parallel_width >= meta["parts"]
            exp_sep = ec.score.parallel_width if ec else None
            note = "parallel_width={} (>= {} parts)".format(
                exp_sep, meta["parts"]) if ec else "no component split"
        # decomposable: the expected backend achieves a small separator.
        else:
            ok = exp is not None and exp_sep <= bound
            note = "via {}".format(exp.name) if exp else "expected backend missing/invalid"
        failures += 0 if ok else 1
        if verbose:
            print("%-17s %-22s %7s  %7s  %5s  %s" %
                  (name, best.name, exp_sep if exp_sep is not None else "-",
                   bound if bound < 10 ** 8 else "-", "OK" if ok else "FAIL", note))

        if write:
            os.makedirs(write, exist_ok=True)
            base = os.path.join(write, name)
            with open(base + ".cnf", "w") as f:
                f.write("c corpus class: {} ({})\n".format(name, meta))
                f.write("p cnf {} {}\n".format(nv, len(clauses)))
                for cl in clauses:
                    f.write(" ".join(map(str, cl)) + " 0\n")
            with open(base + ".meta", "w") as f:
                json.dump(meta, f, indent=2)
            manifest[name] = {"cnf": name + ".cnf", "meta": meta,
                              "recommended": meta["recommended"]}

    if write:
        with open(os.path.join(write, "manifest.json"), "w") as f:
            json.dump(manifest, f, indent=2)
        if verbose:
            print("\nwrote corpus + manifest.json to", write)

    if verbose:
        print("\n{}/{} classes OK".format(len(G.GENERATORS) - failures, len(G.GENERATORS)))
    return failures


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--write", default=None, help="directory to write CNF+.meta+manifest")
    args = ap.parse_args()
    sys.exit(1 if run(write=args.write) else 0)


if __name__ == "__main__":
    main()
