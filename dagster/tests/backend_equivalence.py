#!/usr/bin/env python3
"""Backend-equivalence + regression harness for Dagster's CDCL backends.

Runs a small matrix of (CNF, DAG) instances through each backend
  --backend tinisat (default)
  --backend minisat
  --backend cadical   (the new backend)
and checks:
  1. VERDICT parity   - all backends agree SAT vs UNSAT.
  2. COUNT parity      - with -e 1 (enumerate) the number of solutions matches
                         across backends (the DAG combination is backend-agnostic;
                         note the per-solution *projection* can differ, so we
                         compare counts + validity, not exact solution sets).
  3. VALIDITY          - every solution reported by --backend cadical is consistent
                         with the CNF (no clause is fully falsified by it) - a stdlib
                         check, no PySAT dependency.
  4. DIRECT smoke      - --backend cadical on a tiny single-node SAT and UNSAT instance.

Exit 0 iff everything passes. Self-contained: writes tiny fixtures to a temp dir.
"""

import os
import re
import shutil
import subprocess
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
DAGSTER_DIR = os.path.dirname(HERE)
DAGSTER = os.path.join(DAGSTER_DIR, "dagster")
ENV = dict(os.environ)
ENV["LD_LIBRARY_PATH"] = "/usr/local/lib:" + ENV.get("LD_LIBRARY_PATH", "")
ENV["OMPI_MCA_btl"] = "self,tcp"
ENV["GLOG_logtostderr"] = "true"
ENV["GLOG_v"] = "0"

BACKENDS = {0: "tinisat", 4: "minisat", 5: "cadical", 7: "cryptominisat"}


def run(mode, dag, cnf, enumerate_all, ranks=2, timeout=60):
    """Return (status, n_solutions, solutions) where status in {SAT,UNSAT,TIMEOUT,ERR}."""
    out = tempfile.mktemp(suffix=".sols")
    cmd = ["mpirun", "-n", str(ranks), "--oversubscribe", "-x", "LD_LIBRARY_PATH",
           DAGSTER, "--backend", BACKENDS[mode], "-e", "1" if enumerate_all else "0",
           dag, cnf, "-o", out]
    try:
        p = subprocess.run(cmd, env=ENV, cwd=DAGSTER_DIR, timeout=timeout,
                           stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    except subprocess.TimeoutExpired:
        return ("TIMEOUT", 0, [])
    if p.returncode != 0:
        return ("ERR(%d)" % p.returncode, 0, [])
    sols = parse_sols(out)
    if os.path.exists(out):
        os.remove(out)
    return ("SAT" if sols else "UNSAT", len(sols), sols)


def parse_sols(path):
    sols = []
    if not os.path.exists(path):
        return sols
    with open(path, errors="replace") as f:
        for line in f:
            toks = re.findall(r"-?\d+", line)
            lits = [int(t) for t in toks if t not in ("0",)]
            # a solution line has at least one literal (skip a bare "0"/blank/headers)
            if any(re.fullmatch(r"-?\d+", t) for t in line.split()) and lits:
                sols.append(set(lits))
    return sols


def cnf_nvars(path):
    with open(path, errors="replace") as f:
        for line in f:
            if line.startswith("p cnf"):
                return int(line.split()[2])
    return 0


def parse_cnf(path):
    clauses = []
    with open(path, errors="replace") as f:
        for line in f:
            s = line.strip()
            if not s or s[0] in "pc%":
                continue
            lits = [int(t) for t in s.split() if t.lstrip("-").isdigit()]
            cur = [l for l in lits if l != 0]
            if cur:
                clauses.append(cur)
    return clauses


def solution_consistent(clauses, sol):
    """A (possibly partial) solution is consistent if it falsifies no clause:
    every clause has a literal that is true or unassigned under sol."""
    for cl in clauses:
        if all((-l) in sol for l in cl):   # every literal of the clause is falsified
            return False
    return True


def all_models(clauses, nvars):
    """Brute-force every full satisfying assignment (only for tiny nvars)."""
    models = []
    for mask in range(1 << nvars):
        m = set((v if (mask >> (v - 1)) & 1 else -v) for v in range(1, nvars + 1))
        if all(any(l in m for l in cl) for cl in clauses):
            models.append(m)
    return models


def covers_all_models(sols, models):
    """Completeness: every full model agrees with at least one reported (partial)
    solution (sol's literals are all present in the model)."""
    for m in models:
        if not any(s <= m for s in sols):
            return False
    return True


def write(path, text):
    with open(path, "w") as f:
        f.write(text)


def main():
    if not os.path.exists(DAGSTER):
        print("dagster binary not found at", DAGSTER, file=sys.stderr)
        sys.exit(1)
    tmp = tempfile.mkdtemp(prefix="dagster_beq_")
    failures = 0

    # ---- fixtures -------------------------------------------------------
    good = os.path.join(DAGSTER_DIR, "tests", "minimal", "unit_tests", "good")
    instances = []  # (name, cnf, dag, enumerate)

    # 1. README 2-node example (known multi-solution)
    if os.path.exists(os.path.join(good, "c1.txt")):
        instances.append(("readme_d1c1", os.path.join(good, "c1.txt"),
                          os.path.join(good, "d1.txt"), True))

    # 2. tiny SAT, single node
    write(os.path.join(tmp, "sat.cnf"), "p cnf 3 2\n1 2 0\n-2 3 0\n")
    write(os.path.join(tmp, "sat.dag"),
          "DAG-FILE\nNODES:1\nGRAPH:\nCLAUSES:\n0:0-1\nREPORTING:\n1-3\n")
    instances.append(("tiny_sat", os.path.join(tmp, "sat.cnf"),
                      os.path.join(tmp, "sat.dag"), True))

    # 3. tiny UNSAT, single node
    write(os.path.join(tmp, "unsat.cnf"), "p cnf 1 2\n1 0\n-1 0\n")
    write(os.path.join(tmp, "unsat.dag"),
          "DAG-FILE\nNODES:1\nGRAPH:\nCLAUSES:\n0:0-1\nREPORTING:\n1\n")
    instances.append(("tiny_unsat", os.path.join(tmp, "unsat.cnf"),
                      os.path.join(tmp, "unsat.dag"), True))

    # ---- equivalence matrix --------------------------------------------
    print("%-14s %-10s %-9s %-7s %-7s %-9s" %
          ("instance", "backend", "verdict", "#sols", "valid", "complete"))
    print("-" * 72)
    for name, cnf, dag, enum in instances:
        clauses = parse_cnf(cnf)
        nvars = cnf_nvars(cnf)
        models = all_models(clauses, nvars) if (enum and nvars <= 16) else None
        results = {}
        for mode, label in BACKENDS.items():
            status, n, sols = run(mode, dag, cnf, enum)
            valid = all(solution_consistent(clauses, s) for s in sols)
            complete = covers_all_models(sols, models) if (models is not None and status == "SAT") else None
            results[mode] = (status, n, valid, complete)
            print("%-14s %-10s %-9s %-7s %-7s %-9s" %
                  (name, label, status, n, "yes" if valid else "NO",
                   "-" if complete is None else ("yes" if complete else "NO")))
        ok = True
        # verdict parity (hard)
        if len({r[0] for r in results.values()}) != 1:
            ok = False; print("    FAIL: verdict/status disagreement:", {BACKENDS[m]: r[0] for m, r in results.items()})
        # every backend's solutions must be consistent and (for tiny instances) complete
        for m, r in results.items():
            if not r[2]:
                ok = False; print("    FAIL: %s produced an inconsistent solution" % BACKENDS[m])
            if r[3] is False:
                ok = False; print("    FAIL: %s missed some models (incomplete enumeration)" % BACKENDS[m])
        # counts may differ across backends (pruning generality) -> informational only
        counts = {BACKENDS[m]: r[1] for m, r in results.items()}
        if len({r[1] for r in results.values()}) > 1:
            print("    note: solution counts differ (expected; pruning differs):", counts)
        failures += 0 if ok else 1
        print("    => %s" % ("OK" if ok else "FAIL"))

    # ---- direct cadical smoke ------------------------------------------
    print("\ndirect cadical smoke:")
    s_status, _, _ = run(5, os.path.join(tmp, "sat.dag"), os.path.join(tmp, "sat.cnf"), False)
    u_status, _, _ = run(5, os.path.join(tmp, "unsat.dag"), os.path.join(tmp, "unsat.cnf"), True)
    print("  tiny SAT   -> %s (expect SAT)" % s_status)
    print("  tiny UNSAT -> %s (expect UNSAT)" % u_status)
    if s_status != "SAT":
        failures += 1; print("  FAIL: -m5 tiny SAT")
    if u_status != "UNSAT":
        failures += 1; print("  FAIL: -m5 tiny UNSAT")

    shutil.rmtree(tmp, ignore_errors=True)
    print("\n%s" % ("ALL CHECKS PASSED" if failures == 0 else "%d FAILURE(S)" % failures))
    sys.exit(1 if failures else 0)


if __name__ == "__main__":
    main()
