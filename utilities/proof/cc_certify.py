#!/usr/bin/env python3
"""Certify a cube-and-conquer UNSAT result end to end (Milestone 2).

A cube-and-conquer refutation of `formula` over cubes C_1..C_n rests on two facts:

  (1) every cube is refuted:   for all i,  formula ∧ C_i  is UNSAT
  (2) the cubes are exhaustive: ⋁_i C_i is a tautology  (⋀_i ¬C_i is UNSAT)

  =>  formula is UNSAT.

This driver certifies BOTH with machine-checked DRAT proofs and reports the
conclusion:

  (1) for each cube C_i, build  formula ∧ C_i  (the formula plus the cube's literals
      as unit clauses) and certify it UNSAT with a drat-trim-checked proof
      (certify.py). If any cube is SAT, the formula is SAT and that cube's model is
      a witness -- reported, not a failure of the tool.
  (2) certify the cube set exhaustive (tautology.py: ⋀_i ¬C_i UNSAT, checked).

Every clause-level step is checked by drat-trim; the only thing outside the checker
is the one-line cube-and-conquer meta-inference (1)+(2) => UNSAT, which is the
definition of cube-and-conquer. (A single monolithic DRAT that inlines even that
step needs CaDiCaL assumption-mode proof emission -- see PROOF_SCOPE.md; this
per-cube bundle is what large-scale C&C certification, e.g. Pythagorean, actually
uses, and it is the natural fit for Dagster's distributed conquer.)

  python3 cc_certify.py formula.cnf cubes.icnf
  python3 cc_certify.py formula.cnf cubes.icnf --max-cubes 8   # cap (smoke test)
"""

import argparse
import os
import sys
import tempfile

import certify
import tautology


def read_formula(path):
    """Return (nvars, nclauses, body_text) where body_text is the clause lines."""
    nv = nc = 0
    body = []
    with open(path, errors="replace") as f:
        for line in f:
            s = line.strip()
            if not s or s[0] == "c":
                continue
            if s.startswith("p cnf"):
                t = s.split(); nv, nc = int(t[2]), int(t[3]); continue
            body.append(s if s.endswith("0") else s + " 0")
    return nv, nc, body


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("formula", help="the CNF being refuted")
    ap.add_argument("icnf", help="march cube file (lines 'a <lits> 0')")
    ap.add_argument("--ranks", type=int, default=2)
    ap.add_argument("--timeout", type=int, default=600, help="per-cube solve timeout (s)")
    ap.add_argument("--max-cubes", type=int, default=None, help="certify only the first N cubes (smoke test)")
    args = ap.parse_args()

    nv, nc, body = read_formula(args.formula)
    cubes, mx = tautology.read_cubes(args.icnf)
    if not cubes:
        sys.exit("no cubes in %s" % args.icnf)
    nv = max(nv, mx)
    todo = cubes if args.max_cubes is None else cubes[:args.max_cubes]
    print("cube-and-conquer certify: %s  (%d vars, %d clauses)  x  %d cube(s)%s\n"
          % (args.formula, nv, nc, len(todo),
             "" if args.max_cubes is None else " [capped from %d]" % len(cubes)))

    # ---- (1) every cube refuted -----------------------------------------
    work = tempfile.mkdtemp(prefix="cc_certify_")
    refuted = 0
    sat_witness = None
    failed = []
    for i, cube in enumerate(todo):
        cnf_i = os.path.join(work, "cube_%d.cnf" % i)
        with open(cnf_i, "w") as f:
            f.write("p cnf %d %d\n" % (nv, nc + len(cube)))
            for cl in body:
                f.write(cl + "\n")
            for lit in cube:                          # cube literals as unit clauses
                f.write("%d 0\n" % lit)
        r = certify.certify(cnf_i, ranks=args.ranks, timeout=args.timeout)
        tag = "UNSAT verified" if (r["verdict"] == "UNSAT" and r["verified"]) else r["verdict"]
        mark = "ok" if (r["verdict"] == "UNSAT" and r["verified"]) else "<--"
        print("   cube %-4d (|C|=%-3d) -> %-16s %s" % (i, len(cube), tag, mark))
        if r["verdict"] == "UNSAT" and r["verified"]:
            refuted += 1
        elif r["verdict"] == "SAT":
            sat_witness = i; break
        else:
            failed.append((i, r["verdict"], r["verified"]))
    import shutil
    shutil.rmtree(work, ignore_errors=True)

    if sat_witness is not None:
        print("\nRESULT: formula is SATISFIABLE -- cube %d extends to a model "
              "(so the UNSAT claim is FALSE)." % sat_witness)
        sys.exit(3)
    if failed:
        print("\nRESULT: %d cube(s) NOT certified UNSAT (timeout / unverified) -- "
              "incomplete; rerun with a larger --timeout or investigate." % len(failed))
        sys.exit(2)
    if args.max_cubes is not None:
        print("\n[capped run] %d/%d cubes refuted; skipping exhaustiveness. "
              "Drop --max-cubes for a full certificate." % (refuted, len(cubes)))
        sys.exit(0)

    # ---- (2) cubes exhaustive -------------------------------------------
    print("\n   all %d cubes refuted (drat-trim verified). checking exhaustiveness ..." % refuted)
    work2 = tempfile.mkdtemp(prefix="cc_taut_")
    neg = os.path.join(work2, "negcubes.cnf")
    with open(neg, "w") as f:
        f.write("p cnf %d %d\n" % (mx, len(cubes)))
        for c in cubes:
            f.write(" ".join(str(-l) for l in c) + " 0\n")
    rt = certify.certify(neg, ranks=args.ranks, timeout=args.timeout)
    shutil.rmtree(work2, ignore_errors=True)
    exhaustive = (rt["verdict"] == "UNSAT" and rt["verified"])
    print("   exhaustiveness (negated-cubes CNF) -> %s" %
          ("UNSAT verified -- cubes cover the space" if exhaustive else rt["verdict"]))

    if exhaustive:
        print("\nRESULT: UNSAT CERTIFIED -- all %d cubes refuted (drat-trim) AND the cube set is\n"
              "        exhaustive (drat-trim); by cube-and-conquer, %s is UNSAT."
              % (len(cubes), os.path.basename(args.formula)))
        sys.exit(0)
    print("\nRESULT: cubes refuted but NOT exhaustive (%s) -- the cube set has a gap; "
          "not a complete certificate." % rt["verdict"])
    sys.exit(1)


if __name__ == "__main__":
    main()
