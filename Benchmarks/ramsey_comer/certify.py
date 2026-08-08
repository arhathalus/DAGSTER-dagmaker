#!/usr/bin/env python3
"""Certify the "no Cayley representation of the M-colour Ramsey algebra on any
group of order <= K" result with checked DRAT proofs.

For every group (from GAP's exhaustive SmallGroups enumeration) of each order in a
range, encode the Comer/Cayley representation problem, solve with standalone
CaDiCaL emitting a DRAT proof, and check that proof with drat-trim. A group that
comes back UNSAT with a VERIFIED proof is a machine-checked theorem: that group
admits no such Cayley colouring. (A SAT would be a representation -- it would
resolve the open case.)

Two modes:
  default    -- encode WITH colour value-precedence symmetry breaking. Fast and
                feasible over the whole range. The resulting theorem then rests on
                one standard lemma: value precedence is VERDICT-PRESERVING for the
                M fully-interchangeable colours (any colour permutation maps a
                representation to a representation, so requiring colours to appear
                in precedence order keeps the lex-least member of each orbit; hence
                the broken CNF is SAT iff the unbroken one is). Sound (Crawford et
                al.); so UNSAT of the broken CNF => no representation.
  --airtight -- encode WITHOUT symmetry breaking, so a certified UNSAT rests on
                nothing but the checked proof. Bulletproof but expensive: proofs
                are ~20-1000x larger and some highly-symmetric groups (e.g. the
                order-32 modular group) don't solve in reasonable time.

  certify.py -M 8 --range 15 48 [--airtight] [--timeout 300] [--proofs DIR]
"""
import argparse
import os
import subprocess
import sys
import tempfile

import comer

HERE = os.path.dirname(os.path.abspath(__file__))
CADICAL = os.path.join(HERE, "..", "..", "dagster", "cadical_solver", "cadical", "build", "cadical")
KISSAT = os.path.join(HERE, "..", "..", "utilities", "kissat", "build", "kissat")
DRAT_TRIM = os.path.join(HERE, "..", "..", "utilities", "proof", "drat-trim")

# Both emit an ASCII DRAT proof (drat-trim reads it directly) via "--no-binary".
# Kissat (SAT-competition winner) is markedly stronger than CaDiCaL on the harder
# larger-order instances -- use it to push the frontier past order 48.
SOLVERS = {"cadical": CADICAL, "kissat": KISSAT}


def certify_group(G, M, symbreak, timeout, proofs_dir=None, solver="cadical"):
    """Encode -> solver(+DRAT) -> drat-trim. Returns dict(verdict, verified, bytes)."""
    enc, *_ = comer.encode(G, M, symbreak=symbreak)
    cnf = tempfile.mktemp(suffix=".cnf")
    proof = tempfile.mktemp(suffix=".drat")
    comer.write_dimacs(enc, cnf)
    binary = SOLVERS[solver]
    try:
        # ASCII DRAT (--no-binary) so drat-trim reads it directly; -q quiet.
        try:
            rc = subprocess.call([binary, "-q", "--no-binary", cnf, proof], timeout=timeout,
                                 stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        except subprocess.TimeoutExpired:
            return dict(verdict="TIMEOUT", verified=False, bytes=0, nvars=enc.nvars, ncls=len(enc.clauses))
        if rc == 10:
            return dict(verdict="SAT", verified=False, bytes=0, nvars=enc.nvars, ncls=len(enc.clauses))
        if rc != 20:
            return dict(verdict="ERR(%d)" % rc, verified=False, bytes=0, nvars=enc.nvars, ncls=len(enc.clauses))
        nbytes = os.path.getsize(proof) if os.path.exists(proof) else 0
        p = subprocess.run([DRAT_TRIM, cnf, proof], capture_output=True, text=True)
        verified = "s VERIFIED" in p.stdout
        if proofs_dir and verified:
            os.makedirs(proofs_dir, exist_ok=True)
            os.replace(proof, os.path.join(proofs_dir, "%s.drat" % G.name()))
        return dict(verdict="UNSAT", verified=verified, bytes=nbytes,
                    nvars=enc.nvars, ncls=len(enc.clauses))
    finally:
        for f in (cnf, proof):
            if os.path.exists(f):
                os.remove(f)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("-M", "--colours", type=int, required=True)
    ap.add_argument("--range", nargs=2, type=int, metavar=("LO", "HI"), required=True)
    ap.add_argument("--airtight", action="store_true",
                    help="encode WITHOUT symmetry breaking, so a certified UNSAT rests on nothing "
                         "but the checked proof (bulletproof but expensive -- large proofs, some "
                         "groups time out). Default: WITH value-precedence breaking (feasible; the "
                         "theorem then also uses the standard 'value precedence is verdict-preserving' lemma).")
    ap.add_argument("--timeout", type=int, default=300)
    ap.add_argument("--proofs", metavar="DIR", help="keep the verified .drat proofs here")
    ap.add_argument("--solver", choices=sorted(SOLVERS), default="cadical",
                    help="DRAT-emitting solver (default cadical; kissat is stronger on the "
                         "harder larger-order instances).")
    args = ap.parse_args()
    if not os.path.exists(SOLVERS[args.solver]):
        sys.exit("solver binary not found: %s (build it first)" % SOLVERS[args.solver])

    if not comer.gap_available():
        print("WARNING: GAP not on PATH -> using the non-exhaustive fallback catalogue "
              "(the result would NOT be a complete 'every group' statement).", file=sys.stderr)

    print("Certifying: M=%d, groups of order %d..%d, solver=%s, mode=%s\n"
          % (args.colours, args.range[0], args.range[1], args.solver,
             "airtight (no symmetry breaking)" if args.airtight else "value-precedence breaking"))
    print("  %-4s %-12s %-8s %-9s %8s %10s" % ("n", "group", "verdict", "proof?", "vars", "proofKB"))
    print("  " + "-" * 62)
    total = certified = 0
    sat = []
    unverified = []
    for order in range(args.range[0], args.range[1] + 1):
        for G in comer.catalog(order):
            r = certify_group(G, args.colours, symbreak=not args.airtight,
                              timeout=args.timeout, proofs_dir=args.proofs, solver=args.solver)
            total += 1
            mark = ""
            if r["verdict"] == "UNSAT" and r["verified"]:
                certified += 1; mark = "VERIFIED"
            elif r["verdict"] == "UNSAT":
                mark = "unverified!"; unverified.append(G.name())
            elif r["verdict"] == "SAT":
                mark = "*** SAT ***"; sat.append(G.name())
            else:
                mark = r["verdict"]
            print("  %-4d %-12s %-8s %-9s %8d %10.1f"
                  % (order, G.name(), r["verdict"], mark, r["nvars"], r["bytes"] / 1024.0))
    print("  " + "-" * 62)
    print("\n%d groups; %d certified UNSAT (checked DRAT proofs)." % (total, certified))
    if sat:
        print("*** REPRESENTATION(S) FOUND: %s -- resolves the case! ***" % ", ".join(sat))
    if unverified:
        print("!! UNSAT but proof NOT verified (investigate): %s" % ", ".join(unverified))
    if certified == total and not sat:
        print("THEOREM (machine-checked): no Cayley representation of the %d-colour Ramsey algebra"
              % args.colours)
        print("on any group of order in %d..%d.%s"
              % (args.range[0], args.range[1], "" if args.airtight else
                 "  [modulo the standard lemma: value-precedence breaking is verdict-preserving]"))
    sys.exit(0 if (certified == total and not sat) else 1)


if __name__ == "__main__":
    main()
