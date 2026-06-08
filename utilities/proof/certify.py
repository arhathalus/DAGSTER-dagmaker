#!/usr/bin/env python3
"""Certify a CNF's UNSAT verdict with a machine-checked DRAT proof.

Milestone 0 of the proof work (see utilities/cube/PROOF_SCOPE.md): solve a CNF as a
single Dagster node with CaDiCaL emitting a DRAT proof, then verify that proof with
drat-trim. This turns an UNSAT *verdict* into a *checked theorem* -- independent of
Dagster's DAG/cube machinery (the single-node run is just the backend + proof).

  python3 certify.py problem.cnf
  python3 certify.py problem.cnf --ranks 2 --keep   # keep the .drat proof

SAT instances need no proof (the model is the certificate); this reports SAT and
stops. Hard instances may need cube-and-conquer proofs (a later milestone) -- this
tool is the single-solver certifier.
"""

import argparse
import os
import subprocess
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(os.path.dirname(HERE))
DAGSTER = os.path.join(REPO_ROOT, "dagster", "dagster")
DRAT_TRIM = os.path.join(HERE, "drat-trim")

ENV = dict(os.environ)
ENV["LD_LIBRARY_PATH"] = "/usr/local/lib:" + ENV.get("LD_LIBRARY_PATH", "")
ENV["OMPI_MCA_btl"] = "self,tcp"


def cnf_dims(path):
    with open(path, errors="replace") as f:
        for line in f:
            if line.startswith("p cnf"):
                t = line.split()
                return int(t[2]), int(t[3])
    sys.exit("no 'p cnf' header in %s" % path)


def certify(cnf, ranks=2, timeout=600, drat_trim=DRAT_TRIM, keep=False):
    """Solve `cnf` as a single Dagster node with a DRAT proof and check it.

    Returns dict(verdict=SAT|UNSAT|TIMEOUT|ERR, verified=bool, proof=path-or-None,
    bytes=int). UNSAT+verified means the verdict is a checked theorem; SAT means a
    model exists (self-certifying, no proof). Reusable by tautology.py et al."""
    import shutil
    for tool in (DAGSTER, drat_trim):
        if not os.path.exists(tool):
            sys.exit("missing %s (build it first)" % tool)
    nv, nc = cnf_dims(cnf)
    work = tempfile.mkdtemp(prefix="certify_")
    try:
        dag = os.path.join(work, "single.dag")
        with open(dag, "w") as f:
            f.write("DAG-FILE\nNODES:1\nGRAPH:\nCLAUSES:\n0:0-%d\nREPORTING:\n1-%d\n" % (nc - 1, nv))
        proof = os.path.join(work, "proof.drat")
        sol = os.path.join(work, "sol.txt")
        cmd = ["mpirun", "--oversubscribe", "-n", str(ranks), "-x", "LD_LIBRARY_PATH",
               "-x", "OMPI_MCA_btl", DAGSTER, "--backend", "cadical", "--proof", proof,
               "-e", "0", dag, cnf, "-o", sol]
        try:
            rc = subprocess.call(cmd, env=ENV, timeout=timeout,
                                 stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        except subprocess.TimeoutExpired:
            return dict(verdict="TIMEOUT", verified=False, proof=None, bytes=0)
        if rc != 0:
            return dict(verdict="ERR(%d)" % rc, verified=False, proof=None, bytes=0)
        if os.path.exists(sol) and os.path.getsize(sol) > 0:
            return dict(verdict="SAT", verified=False, proof=None, bytes=0)
        parts = sorted(p for p in os.listdir(work) if p.startswith("proof.drat."))
        if not parts:
            return dict(verdict="UNSAT", verified=False, proof=None, bytes=0)
        pf = os.path.join(work, parts[0])
        nbytes = os.path.getsize(pf)
        p = subprocess.run([drat_trim, cnf, pf], capture_output=True, text=True)
        verified = "s VERIFIED" in p.stdout
        kept = None
        if keep:
            kept = cnf + ".drat"
            shutil.copy(pf, kept)
        return dict(verdict="UNSAT", verified=verified, proof=kept, bytes=nbytes)
    finally:
        if not keep:
            shutil.rmtree(work, ignore_errors=True)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("cnf")
    ap.add_argument("--ranks", type=int, default=2, help="mpirun ranks (master + workers); 2 = one worker")
    ap.add_argument("--timeout", type=int, default=600, help="solve timeout (s)")
    ap.add_argument("--drat-trim", default=DRAT_TRIM, help="path to the drat-trim binary")
    ap.add_argument("--keep", action="store_true", help="keep the proof + DAG artifacts")
    args = ap.parse_args()

    nv, nc = cnf_dims(args.cnf)
    print("certify %s  (%d vars, %d clauses)" % (args.cnf, nv, nc))
    r = certify(args.cnf, ranks=args.ranks, timeout=args.timeout,
                drat_trim=args.drat_trim, keep=args.keep)
    print("[1] solve     -> %s" % r["verdict"])
    if r["verdict"] == "SAT":
        print("[2] certify   -> SAT needs no DRAT proof; the model is the certificate.")
        print("\nRESULT: SAT (model is self-certifying)")
        return
    if r["verdict"] in ("TIMEOUT",) or r["verdict"].startswith("ERR"):
        print("\nRESULT: %s (no proof)" % r["verdict"]); sys.exit(2)
    print("[2] proof     -> %d bytes%s" % (r["bytes"], ("  kept: " + r["proof"]) if r["proof"] else ""))
    print("[3] drat-trim -> %s" % ("s VERIFIED" if r["verified"] else "NOT verified"))
    print("\nRESULT: %s" % ("UNSAT VERIFIED -- the verdict is a checked theorem"
                            if r["verified"] else "UNSAT but proof NOT verified -- INVESTIGATE"))
    sys.exit(0 if r["verified"] else 1)


if __name__ == "__main__":
    main()
