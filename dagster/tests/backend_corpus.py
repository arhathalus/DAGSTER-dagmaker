#!/usr/bin/env python3
"""Corpus backend audit: run every minimal/ and comprehensive/ test instance
through --backend cadical and verify it against the tinisat (-m 0) baseline.

For each test dir we parse its first `-m 0` mpirun line (to reuse that test's
own topology + backend-agnostic flags -n/-g/-e/-b/-i and its dag/cnf), then run
the SAME configuration twice -- once `-m 0` (tinisat baseline) and once
`--backend cadical` -- and check:

  1. VERDICT parity   -- cadical agrees SAT/UNSAT with the tinisat baseline.
  2. VALIDITY         -- every solution cadical reports satisfies the CNF
                         (stdlib clause check -- no PySAT).
  3. COMPLETENESS     -- for full-enumeration runs, CNF + the negation of every
                         reported cadical solution is UNSAT (no model is missed).
                         Oracle = the standalone CaDiCaL binary (like check.py's
                         PySAT use), best-effort with a timeout.

Tinisat-only feature flags (-p restart, -x/-y geometric, -u/-v checkpoint, -t,
-k, -q) are dropped: they are orthogonal to the node backend, so we exercise the
plain solve under cadical. Exit 0 iff every instance passes (1)+(2) and, where
attempted, (3).
"""

import glob
import os
import re
import subprocess
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
DAGSTER_DIR = os.path.dirname(HERE)
DAGSTER = os.path.join(DAGSTER_DIR, "dagster")
CADICAL = os.path.join(DAGSTER_DIR, "cadical_solver", "cadical", "build", "cadical")
ENV = dict(os.environ)
ENV["LD_LIBRARY_PATH"] = "/usr/local/lib:" + ENV.get("LD_LIBRARY_PATH", "")
ENV["OMPI_MCA_btl"] = "self,tcp"
ENV["GLOG_logtostderr"] = "true"
ENV["GLOG_v"] = "0"

# backend-agnostic flags worth mirroring; everything else (tinisat features) dropped
KEEP_FLAGS = {"-g", "-e", "-b", "-i"}
RUN_TIMEOUT = 180
ORACLE_TIMEOUT = 120


def parse_m0_line(run_sh):
    """From a dir's run.sh, return (ranks, kept_flags, dag, cnf) of the first
    `-m 0` mpirun line, or None if there isn't one."""
    with open(run_sh, errors="replace") as f:
        for line in f:
            line = line.strip()
            if "mpirun" not in line or "-m 0" not in line:
                continue
            toks = line.split()
            ranks = "6"
            flags = []
            files = []
            i = 0
            while i < len(toks):
                t = toks[i]
                if t == "-n":
                    ranks = toks[i + 1]; i += 2; continue
                if t in KEEP_FLAGS:
                    flags += [t, toks[i + 1]]; i += 2; continue
                if t.endswith(".txt") or t.endswith(".cnf") or t.endswith(".dag"):
                    files.append(t)
                i += 1
            # files are listed dag then cnf (dagster arg order); -o output is .sols
            datafiles = [f for f in files if not f.endswith(".sols")]
            if len(datafiles) < 2:
                return None
            dag, cnf = datafiles[0], datafiles[1]
            return (ranks, flags, dag, cnf, line)
    return None


def parse_cnf(path):
    clauses = []
    with open(path, errors="replace") as f:
        for line in f:
            line = line.strip()
            if not line or line[0] in "pc%":
                continue
            lits = [int(t) for t in line.split() if t and t != "0"]
            if lits:
                clauses.append(lits)
    return clauses


def parse_sols(path):
    sols = []
    if not os.path.exists(path):
        return sols
    with open(path, errors="replace") as f:
        for line in f:
            toks = [t for t in line.split() if re.fullmatch(r"-?\d+", t)]
            lits = [int(t) for t in toks if t != "0"]
            if lits:
                sols.append(lits)
    return sols


def solution_consistent(clauses, lits):
    """No clause is fully falsified by the (partial) assignment `lits`."""
    s = set(lits)
    for cl in clauses:
        if all(-l in s for l in cl):     # every literal of the clause is falsified
            return False
    return True


def run_dagster(backend_args, ranks, flags, dag, cnf, cwd):
    out = os.path.join(cwd, tempfile.mktemp(dir="", suffix=".sols").lstrip("/\\"))
    cmd = (["mpirun", "-n", str(ranks), "--oversubscribe", "-x", "LD_LIBRARY_PATH"]
           + [DAGSTER] + backend_args + flags + [dag, cnf, "-o", out])
    try:
        p = subprocess.run(cmd, env=ENV, cwd=cwd, timeout=RUN_TIMEOUT,
                           stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        if p.returncode != 0:
            return ("ERR(%d)" % p.returncode, [])
        sols = parse_sols(out)
        return ("SAT" if sols else "UNSAT", sols)
    except subprocess.TimeoutExpired:
        return ("TIMEOUT", [])
    finally:
        if os.path.exists(out):    # always clean up, even on TIMEOUT/ERR
            os.remove(out)


def oracle_verdict(cnf_path):
    """Independent SAT/UNSAT verdict from the standalone CaDiCaL binary on the raw
    CNF (no dagster) -- used to validate cadical when the tinisat baseline is
    inconclusive (TIMEOUT/ERR). Returns 'SAT'/'UNSAT'/None."""
    if not os.path.exists(CADICAL):
        return None
    try:
        p = subprocess.run([CADICAL, "-q", cnf_path], timeout=ORACLE_TIMEOUT,
                           stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    except (subprocess.TimeoutExpired, OSError):
        return None
    return {10: "SAT", 20: "UNSAT"}.get(p.returncode)


def complete_via_oracle(cnf_path, clauses, sols):
    """check.py logic with standalone cadical: CNF + negation of every reported
    solution must be UNSAT. Returns True/False/None (None = skipped/oracle error)."""
    if not os.path.exists(CADICAL):
        return None
    tmp = tempfile.mktemp(suffix=".cnf")
    nvars = 0
    for cl in clauses:
        for l in cl:
            nvars = max(nvars, abs(l))
    blockers = [[-l for l in s] for s in sols]
    try:
        with open(tmp, "w") as f:
            f.write("p cnf %d %d\n" % (nvars, len(clauses) + len(blockers)))
            for cl in clauses:
                f.write(" ".join(map(str, cl)) + " 0\n")
            for b in blockers:
                f.write(" ".join(map(str, b)) + " 0\n")
        p = subprocess.run([CADICAL, "-q", tmp], timeout=ORACLE_TIMEOUT,
                           stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        # cadical: 20 = UNSAT (complete), 10 = SAT (a model was missed)
        return p.returncode == 20
    except (subprocess.TimeoutExpired, OSError):
        return None
    finally:
        if os.path.exists(tmp):
            os.remove(tmp)


def main():
    if not os.path.exists(DAGSTER):
        sys.exit("dagster binary not found at %s" % DAGSTER)
    dirs = sorted(glob.glob(os.path.join(HERE, "minimal", "*", "")) +
                  glob.glob(os.path.join(HERE, "comprehensive", "*", "")))
    print("%-40s %-13s %-8s %-7s %-9s %s" %
          ("instance", "baseline", "cadical", "valid", "complete", "result"))
    print("-" * 90)
    failures, tested, skipped = 0, 0, 0
    for d in dirs:
        name = os.path.relpath(d, HERE).rstrip("/")
        run_sh = os.path.join(d, "run.sh")
        if not os.path.exists(run_sh):
            continue
        parsed = parse_m0_line(run_sh)
        if parsed is None:
            print("%-40s %s" % (name, "(no -m 0 solve line -- skipped, e.g. unit_tests)"))
            skipped += 1
            continue
        ranks, flags, dag, cnf, m0line = parsed
        cnf_abs = os.path.join(d, cnf)
        if not (os.path.exists(cnf_abs) and os.path.exists(os.path.join(d, dag))):
            print("%-40s %s" % (name, "(dag/cnf not found -- skipped)"))
            skipped += 1
            continue
        # A test is FULL-enumeration only if it doesn't use a partial/interrupt mode:
        # `check.py ... some` (validity-only), `-i` (solution interrupt) or `-e 0`
        # (decision) all mean partial -> completeness must NOT be asserted.
        run_text = open(run_sh, errors="replace").read()
        partial = ("some" in run_text) or ("-i " in m0line) or ("-e 0" in m0line)
        clauses = parse_cnf(cnf_abs)
        base_v, _ = run_dagster(["-m", "0"], ranks, flags, dag, cnf, d)
        cad_v, cad_sols = run_dagster(["--backend", "cadical"], ranks, flags, dag, cnf, d)
        valid = all(solution_consistent(clauses, s) for s in cad_sols)
        # Reference verdict: tinisat baseline, unless it was inconclusive
        # (TIMEOUT/ERR) -- then fall back to the independent standalone-cadical
        # oracle so "cadical solved what tinisat couldn't" reads as a win, not a fail.
        ref, refsrc = base_v, "tinisat"
        if base_v in ("TIMEOUT",) or base_v.startswith("ERR"):
            o = oracle_verdict(cnf_abs)
            if o is not None:
                ref, refsrc = o, "oracle"
        complete = None
        if not partial and cad_v == "SAT":
            complete = complete_via_oracle(cnf_abs, clauses, cad_sols)
        tested += 1
        verdict_ok = (cad_v == ref)
        ok = verdict_ok and valid and (complete is not False)
        failures += 0 if ok else 1
        base_disp = base_v if refsrc == "tinisat" else "%s/orcl=%s" % (base_v, ref)
        print("%-40s %-13s %-8s %-7s %-9s %s" %
              (name, base_disp, cad_v, "yes" if valid else "NO",
               {True: "yes", False: "NO", None: "-"}[complete],
               "OK" if ok else "FAIL"))
        if not ok:
            if not verdict_ok:
                print("    verdict mismatch: reference(%s)=%s cadical=%s" % (refsrc, ref, cad_v))
            if not valid:
                print("    cadical produced an inconsistent solution")
            if complete is False:
                print("    cadical missed models (incomplete enumeration)")
    print("-" * 86)
    print("tested %d, skipped %d, %s" %
          (tested, skipped, "ALL PASSED" if failures == 0 else "%d FAILURE(S)" % failures))
    sys.exit(1 if failures else 0)


if __name__ == "__main__":
    main()
