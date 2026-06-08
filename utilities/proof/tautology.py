#!/usr/bin/env python3
"""Certify that a march cube set is EXHAUSTIVE -- the cube-split tautology proof.

Milestone 1 of the proof work (utilities/cube/PROOF_SCOPE.md). Cube-and-conquer is
sound only if the cubes cover the whole space: every total assignment must extend
some cube, i.e. ⋁_i C_i is a tautology. Equivalently, the *negated-cubes CNF*
⋀_i ¬C_i (one clause per cube, the cube's literals negated) is UNSATISFIABLE.

So the tautology proof is just a DRAT refutation of the negated-cubes CNF -- which
`certify.py` produces and checks. This tool builds that CNF from an `.icnf` and
certifies it:

  EXHAUSTIVE  -> negated-cubes CNF is UNSAT and the proof verifies. The cube split
                is certified complete; combined with per-cube refutations
                (formula ⊢ ¬C_i) this yields a full cube-and-conquer UNSAT proof
                (the composition is Milestone 2).
  GAP         -> negated-cubes CNF is SAT: drat-trim returns a model = a total
                assignment to the split variables covered by NO cube. That march
                configuration does not yield a self-contained tautology.

  python3 tautology.py cubes.icnf
  python3 tautology.py cubes.icnf --keep   # keep negated-cubes CNF + proof
"""

import argparse
import os
import sys
import tempfile

import certify   # sibling module (same dir)


def read_cubes(icnf):
    """Parse 'a <lits> 0' cube lines. Returns (cubes, max_var)."""
    cubes, mx = [], 0
    with open(icnf, errors="replace") as f:
        for ln in f:
            ln = ln.strip()
            if not ln or ln[0] != "a":
                continue
            lits = [int(x) for x in ln.split()[1:] if x != "0"]
            if lits:
                cubes.append(lits)
                mx = max(mx, max(abs(l) for l in lits))
    return cubes, mx


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("icnf", help="march cube file (lines 'a <lits> 0')")
    ap.add_argument("--ranks", type=int, default=2)
    ap.add_argument("--timeout", type=int, default=600)
    ap.add_argument("--keep", action="store_true")
    args = ap.parse_args()

    cubes, mx = read_cubes(args.icnf)
    if not cubes:
        sys.exit("no cubes found in %s" % args.icnf)
    print("tautology check: %s  (%d cubes, %d split vars)" % (args.icnf, len(cubes), mx))

    # negated-cubes CNF: one clause per cube = the cube's literals negated.
    work = tempfile.mkdtemp(prefix="tautology_")
    neg = os.path.join(work, "negcubes.cnf")
    with open(neg, "w") as f:
        f.write("p cnf %d %d\n" % (mx, len(cubes)))
        for c in cubes:
            f.write(" ".join(str(-l) for l in c) + " 0\n")

    r = certify.certify(neg, ranks=args.ranks, timeout=args.timeout, keep=args.keep)
    print("[*] negated-cubes CNF -> %s%s" %
          (r["verdict"], ("  (proof %d bytes)" % r["bytes"]) if r["verdict"] == "UNSAT" else ""))

    if r["verdict"] == "UNSAT" and r["verified"]:
        print("\nRESULT: cube split is EXHAUSTIVE -- tautology proof VERIFIED")
        if args.keep:
            print("        negated-cubes CNF: %s   proof: %s" % (neg, r["proof"]))
        rc = 0
    elif r["verdict"] == "SAT":
        print("\nRESULT: NOT EXHAUSTIVE -- an assignment to the split variables is covered "
              "by no cube;\n        this march configuration has gaps (cannot self-certify).")
        rc = 1
    else:
        print("\nRESULT: %s -- could not certify exhaustiveness%s" %
              (r["verdict"], "" if r["verified"] else " (proof not verified)"))
        rc = 2
    if not args.keep:
        import shutil; shutil.rmtree(work, ignore_errors=True)
    sys.exit(rc)


if __name__ == "__main__":
    main()
