#!/usr/bin/env python3
"""Backend x Problem x DAG test matrix for Dagster.

For every (problem, DAG variant, solver mode) cell this:
  * runs dagster,
  * parses the verdict (SAT/UNSAT) and wall-clock time,
  * enforces VERDICT PARITY  -- all backends/modes must agree on SAT vs UNSAT
    (and agree with the problem's known answer when one is declared); solution
    *counts* may legitimately differ (pruning generality differs per backend),
  * on tiny instances additionally checks COMPLETENESS by brute force,
  * writes results.csv and a per-class timing summary.

Two profiles:
  --profile local   sized for a ~7-8 core workstation: small/medium instances,
                    ranks capped at 8, short timeouts, run serially right here.
  --profile hpc     NOT run locally -- expands the (larger) matrix into a SLURM
                    job array via --emit-hpc DIR, one array task per cell, with
                    big rank counts, gnovelty (-k) sweeps and long timeouts.

The heavy lifting (verdict/validity/completeness checkers) is shared with the
existing tests/backend_equivalence.py contract; this file is self-contained and
stdlib-only so it runs anywhere the binary does.

Examples:
  python3 matrix.py --profile local                 # run the local matrix
  python3 matrix.py --profile local --quick         # tiny+small only, fastest
  python3 matrix.py --profile hpc --emit-hpc ./hpc  # generate SLURM jobs
"""

import argparse
import csv
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time

HERE = os.path.dirname(os.path.abspath(__file__))
DAGSTER_DIR = os.path.dirname(os.path.dirname(HERE))          # .../dagster (the build dir)
REPO_ROOT = os.path.dirname(DAGSTER_DIR)                       # .../dagster (repo root)
DAGSTER_BIN = os.path.join(DAGSTER_DIR, "dagster")
VENV_PY = os.path.join(REPO_ROOT, ".venv", "bin", "python")
DAGMAKE = os.path.join(REPO_ROOT, "utilities", "dag-generator", "dagmake.py")
CORPUS_DIR = os.path.join(REPO_ROOT, "Benchmarks", "corpus")
SU_CNF = os.path.join(REPO_ROOT, "reports", "dagster_tutorials_youtube", "su.cnf")

ENV = dict(os.environ)
ENV["LD_LIBRARY_PATH"] = "/usr/local/lib:" + ENV.get("LD_LIBRARY_PATH", "")
ENV["OMPI_MCA_btl"] = "self,tcp"
ENV["GLOG_logtostderr"] = "true"
ENV["GLOG_v"] = "0"

# mode -> (label, needs_sls_helpers)
MODES = {
    0: ("tinisat", False),
    1: ("tinisat+sls", True),
    4: ("minisat", False),
    5: ("cadical", False),
    6: ("cadical+sls", True),
    7: ("cryptominisat", False),
    8: ("minisat+sls", True),
    9: ("cryptominisat+sls", True),
    11: ("glucose", False),   # IPASIR backend; opt-in via --modes (needs ipasir_solver/libipasirglucose.so)
    12: ("lingeling", False), # IPASIR backend; opt-in via --modes (needs ipasir_solver/libipasirlingeling.so)
}


# Dagster is driven with the orthogonal flag interface (--backend/--sls), not the
# legacy numeric -m selector. Derive the flags from the MODES label ("tinisat+sls"
# -> --backend tinisat --sls -k K).
def mode_flags(mode, k):
    label, needs_sls = MODES[mode]
    flags = ["--backend", label.split("+")[0]]
    if needs_sls:
        flags += ["--sls", "-k", str(k)]
    return flags


# --------------------------------------------------------------------------
# problem sourcing
# --------------------------------------------------------------------------
def write_dimacs(path, clauses, nvars):
    with open(path, "w") as f:
        f.write("p cnf %d %d\n" % (nvars, len(clauses)))
        for cl in clauses:
            f.write(" ".join(str(l) for l in cl) + " 0\n")


def corpus_problems(size_filter):
    """Build CNFs from Benchmarks/corpus/generators.py. Returns list of dicts."""
    out = []
    if CORPUS_DIR not in sys.path:
        sys.path.insert(0, CORPUS_DIR)
    try:
        import generators as G
    except Exception as e:
        print("  (corpus generators unavailable: %s)" % e, file=sys.stderr)
        return out
    # (fn, kwargs, size_class) -- params kept small so the local matrix is fast
    specs = [
        (G.chain_bmc, dict(T=8, P=6), "small"),
        (G.grid_coloring, dict(S=5), "small"),
        (G.tree_constraints, dict(n=24), "small"),
        (G.modular, dict(k=3, sz=6), "small"),
        (G.components, dict(k=4, sz=8), "small"),
        (G.banded_xor, dict(n=30, k=3), "small"),
        (G.pigeonhole, dict(holes=5), "small"),          # UNSAT
        (G.expander, dict(n=36, ratio=4.2), "medium"),
    ]
    for fn, kw, size in specs:
        if size not in size_filter:
            continue
        try:
            clauses, nvars, meta = fn(**kw)
        except Exception as e:
            print("  (skip %s: %s)" % (fn.__name__, e), file=sys.stderr)
            continue
        out.append(dict(
            name="corpus_%s" % fn.__name__,
            clauses=clauses, nvars=nvars,
            expected=("UNSAT" if meta.get("unsat") else "SAT"),
            family=meta.get("family", "?"),
            size=size,
        ))
    return out


def tiny_problems():
    return [
        dict(name="tiny_sat", clauses=[[1, 2], [-2, 3]], nvars=3, expected="SAT",
             family="tiny", size="tiny"),
        dict(name="tiny_unsat", clauses=[[1], [-1]], nvars=1, expected="UNSAT",
             family="tiny", size="tiny"),
    ]


GENERATED_MANIFEST = os.path.join(REPO_ROOT, "Benchmarks", "generated", "manifest.tsv")


def real_problems(size_filter):
    out = []
    if "medium" in size_filter and os.path.exists(SU_CNF):
        out.append(dict(name="su", cnf_path=SU_CNF, expected="SAT",
                        family="sudoku", size="medium"))
    # Generated benchmark corpus (Benchmarks/generate_benchmarks.py): costas /
    # determinant / ramsey instances, each labelled by an INDEPENDENT oracle
    # (standalone CaDiCaL on the raw CNF -- not Dagster). Only oracle-confirmed
    # SAT/UNSAT rows become regression data; this automatically excludes the
    # 'open' research targets (verdict OPEN) and the 'hard' frontier instances
    # the oracle could not decide (verdict TIMEOUT).
    if os.path.exists(GENERATED_MANIFEST):
        with open(GENERATED_MANIFEST) as f:
            for row in csv.DictReader(f, delimiter="\t"):
                if row["verdict"] not in ("SAT", "UNSAT") or row["size"] not in size_filter:
                    continue
                if row.get("model") == "INVALID":   # never trust an unvalidated SAT model
                    continue
                cnf = os.path.join(REPO_ROOT, row["cnf"])
                if os.path.exists(cnf):
                    out.append(dict(name=row["name"], cnf_path=cnf, expected=row["verdict"],
                                    family=row["family"], size=row["size"]))
    return out


def materialise_cnf(prob, workdir):
    """Ensure prob has a 'cnf_path' on disk and 'nvars'."""
    if "cnf_path" in prob:
        if "nvars" not in prob:
            prob["nvars"] = cnf_nvars(prob["cnf_path"])
        return prob["cnf_path"]
    path = os.path.join(workdir, prob["name"] + ".cnf")
    write_dimacs(path, prob["clauses"], prob["nvars"])
    prob["cnf_path"] = path
    return path


# --------------------------------------------------------------------------
# DAG variants
# --------------------------------------------------------------------------
def _range(a, b):
    # dagster's DAG parser rejects degenerate "a-a" ranges (in CLAUSES and
    # REPORTING alike), so collapse them to a single index.
    return str(a) if a == b else "%d-%d" % (a, b)


def single_node_dag(path, nclauses, nvars):
    """A guaranteed-valid 1-node DAG reporting every variable."""
    with open(path, "w") as f:
        f.write("DAG-FILE\nNODES:1\nGRAPH:\nCLAUSES:\n0:%s\nREPORTING:\n%s\n"
                % (_range(0, nclauses - 1), _range(1, nvars)))


def make_dags(prob, workdir, node_targets):
    """Return list of (dag_label, dag_path). Always includes the 1-node baseline;
    adds dagmake variants (--nodes N and a --search variant) when dagmake works."""
    cnf = prob["cnf_path"]
    nvars = prob["nvars"]
    nclauses = count_clauses(cnf)
    dags = []
    base = os.path.join(workdir, prob["name"] + "_single.dag")
    single_node_dag(base, nclauses, nvars)
    dags.append(("single", base))
    if not (os.path.exists(VENV_PY) and os.path.exists(DAGMAKE)):
        return dags
    for n in node_targets:
        dp = os.path.join(workdir, "%s_n%d.dag" % (prob["name"], n))
        if run_dagmake(cnf, dp, n, search=False):
            dags.append(("nodes%d" % n, dp))
    dp = os.path.join(workdir, "%s_search.dag" % prob["name"])
    if run_dagmake(cnf, dp, max(node_targets), search=True):
        dags.append(("search", dp))
    return dags


def run_dagmake(cnf, dag_out, nodes, search):
    cmd = [VENV_PY, DAGMAKE, "--nodes", str(nodes), cnf, dag_out, "--quiet"]
    if search:
        cmd.insert(3, "--search")
    try:
        p = subprocess.run(cmd, env=ENV, cwd=os.path.dirname(DAGMAKE),
                           stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=120)
        return p.returncode == 0 and os.path.exists(dag_out) and os.path.getsize(dag_out) > 0
    except Exception:
        return False


# --------------------------------------------------------------------------
# parsing / checking helpers (shared contract with backend_equivalence.py)
# --------------------------------------------------------------------------
def cnf_nvars(path):
    with open(path, errors="replace") as f:
        for line in f:
            if line.startswith("p cnf"):
                return int(line.split()[2])
    return 0


def count_clauses(path):
    n = 0
    with open(path, errors="replace") as f:
        for line in f:
            s = line.strip()
            if not s or s[0] in "pc%":
                continue
            if s.split()[-1] == "0":
                n += 1
    return n


def parse_sols(path):
    sols = []
    if not os.path.exists(path):
        return sols
    with open(path, errors="replace") as f:
        for line in f:
            lits = [int(t) for t in re.findall(r"-?\d+", line) if t != "0"]
            if lits:
                sols.append(set(lits))
    return sols


def parse_cnf(path):
    clauses = []
    with open(path, errors="replace") as f:
        for line in f:
            s = line.strip()
            if not s or s[0] in "pc%":
                continue
            cur = [int(t) for t in s.split() if t.lstrip("-").isdigit() and t != "0"]
            if cur:
                clauses.append(cur)
    return clauses


def solution_consistent(clauses, sol):
    for cl in clauses:
        if all((-l) in sol for l in cl):
            return False
    return True


# --------------------------------------------------------------------------
# running
# --------------------------------------------------------------------------
def ranks_for(mode, profile, dag_nodes):
    """How many MPI ranks to launch for this mode under this profile."""
    _, needs_sls = MODES[mode]
    if needs_sls:
        # master + (1 worker + k gnovelty helpers)
        return min(profile["max_ranks"], 2 + profile["sls_k"])
    # master + up to one worker per node (capped)
    return min(profile["max_ranks"], max(2, 1 + dag_nodes))


def dag_node_count(dag_path):
    try:
        with open(dag_path, errors="replace") as f:
            for line in f:
                if line.strip().upper().startswith("NODES:"):
                    return int(line.split(":")[1])
    except Exception:
        pass
    return 1


def run_cell(mode, dag, cnf, out, ranks, k, enumerate_all, timeout):
    cmd = (["mpirun", "-n", str(ranks), "--oversubscribe", "-x", "LD_LIBRARY_PATH",
            "-x", "OMPI_MCA_btl", DAGSTER_BIN]
           + mode_flags(mode, k)
           + ["-e", "1" if enumerate_all else "0"])
    cmd += [dag, cnf, "-o", out]
    t0 = time.time()
    try:
        p = subprocess.run(cmd, env=ENV, cwd=DAGSTER_DIR, timeout=timeout,
                           stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    except subprocess.TimeoutExpired:
        return ("TIMEOUT", 0, [], timeout)
    dt = time.time() - t0
    if p.returncode != 0:
        return ("ERR(%d)" % p.returncode, 0, [], dt)
    sols = parse_sols(out)
    if os.path.exists(out):
        os.remove(out)
    return ("SAT" if sols else "UNSAT", len(sols), sols, dt)


# --------------------------------------------------------------------------
# profiles
# --------------------------------------------------------------------------
PROFILES = {
    "local": dict(max_ranks=8, sls_k=2, timeout=60,
                  sizes={"tiny", "small", "medium"},
                  modes=[0, 4, 5, 7, 1, 6, 8, 9], node_targets=[2, 4]),
    "quick": dict(max_ranks=6, sls_k=2, timeout=30,
                  sizes={"tiny", "small"},
                  modes=[0, 4, 5, 7], node_targets=[2]),
    "hpc": dict(max_ranks=128, sls_k=8, timeout=7200,
                sizes={"small", "medium", "large"},
                modes=[0, 1, 4, 5, 6, 7, 8, 9], node_targets=[4, 8, 16]),
}


# --------------------------------------------------------------------------
# local run
# --------------------------------------------------------------------------
def run_local(profile, workdir, results_csv):
    probs = tiny_problems() + corpus_problems(profile["sizes"]) + real_problems(profile["sizes"])
    print("collected %d problems; modes=%s; max_ranks=%d"
          % (len(probs), profile["modes"], profile["max_ranks"]))
    rows = []
    failures = 0
    for prob in probs:
        materialise_cnf(prob, workdir)
        clauses = parse_cnf(prob["cnf_path"])
        nvars = prob["nvars"]
        tiny = prob["size"] == "tiny"
        models = brute_models(clauses, nvars) if tiny else None
        dags = make_dags(prob, workdir, profile["node_targets"])
        print("\n== %-22s (%s, %s, %d vars) -- %d DAG(s), expect %s =="
              % (prob["name"], prob["family"], prob["size"], nvars, len(dags), prob["expected"]))
        verdicts = {}   # (mode,dag) -> verdict
        # the 1-node DAG carries the full CNF with no decomposition, so its
        # verdict is ground truth; everything else is judged against it.
        reference = None
        for dag_label, dag_path in dags:
            nodes = dag_node_count(dag_path)
            for mode in profile["modes"]:
                ranks = ranks_for(mode, profile, nodes)
                if ranks > profile["max_ranks"]:
                    continue
                enum = tiny  # enumerate on tiny for completeness, race otherwise
                verdict, nsol, sols, dt = run_cell(
                    mode, dag_path, prob["cnf_path"], os.path.join(workdir, "out.txt"),
                    ranks, profile["sls_k"], enum, profile["timeout"])
                valid = all(solution_consistent(clauses, s) for s in sols)
                complete = (models is not None and verdict == "SAT"
                            and all(any(s <= m for s in sols) for m in models))
                verdicts[(mode, dag_label)] = verdict
                rows.append(dict(problem=prob["name"], family=prob["family"],
                                 size=prob["size"], dag=dag_label, nodes=nodes,
                                 mode=mode, backend=MODES[mode][0], ranks=ranks,
                                 verdict=verdict, nsols=nsol, valid=valid,
                                 complete=("" if models is None else complete),
                                 seconds=round(dt, 3)))
                definite = verdict in ("SAT", "UNSAT")
                if dag_label == "single" and definite:
                    if reference is None:
                        reference = verdict
                    elif verdict != reference:
                        # backends disagree on the undecomposed CNF -> real bug
                        reference = reference  # keep first; flag below
                flag = ""
                if definite and reference is not None and verdict != reference:
                    flag = "  <-- DISAGREES vs single-node ground truth"; failures += 1
                if not valid:
                    flag += "  <-- INVALID SOL"; failures += 1
                if models is not None and verdict == "SAT" and not complete:
                    flag += "  <-- INCOMPLETE"; failures += 1
                if verdict.startswith("ERR"):       # crash / backend not built -- flag it (counted in health below)
                    flag += "  <-- ERR (crashed / backend unavailable)"
                elif verdict == "TIMEOUT":
                    flag += "  <-- TIMEOUT"
                print("   %-13s %-9s ranks=%d  %-7s sols=%-4d %6.2fs%s"
                      % (MODES[mode][0], dag_label, ranks, verdict, nsol, dt, flag))
        # cross-mode verdict parity on each DAG
        by_dag = {}
        for (mode, dl), v in verdicts.items():
            by_dag.setdefault(dl, set()).add(v if v in ("SAT", "UNSAT") else "ERR")
        for dl, vs in by_dag.items():
            real = vs - {"ERR"}
            if len(real) > 1:
                print("   PARITY FAIL on dag=%s: %s" % (dl, real)); failures += 1
        # advisory only: the generator's guessed label vs measured ground truth
        if reference is not None and prob["expected"] != "?" and reference != prob["expected"]:
            print("   note: declared expected=%s but ground truth=%s (trusting measured)"
                  % (prob["expected"], reference))

    # Backend health: a backend that ERRORs, or never produces a definite verdict,
    # is a FAILURE -- not silently excluded. This is the "tests passed but the
    # backend wasn't built / crashed" trap (an unavailable backend aborts -> ERR,
    # which used to be dropped from the parity check). TIMEOUT is not counted as an
    # error here (it's a real run that ran out of time), but is reported.
    health = {}
    for r in rows:
        h = health.setdefault(r["backend"], {"solved": 0, "err": 0, "timeout": 0})
        v = r["verdict"]
        if v in ("SAT", "UNSAT"):
            h["solved"] += 1
        elif v == "TIMEOUT":
            h["timeout"] += 1
        else:
            h["err"] += 1
    print("\n--- backend health (did each backend actually solve anything?) ---")
    for be in sorted(health):
        h = health[be]
        note = ""
        if h["solved"] == 0:
            note = "  <-- NEVER produced a result (crashed / backend not built?)"; failures += 1
        elif h["err"]:
            note = "  <-- %d ERROR(S)" % h["err"]; failures += 1
        print("   %-14s solved=%-3d err=%-3d timeout=%-3d%s"
              % (be, h["solved"], h["err"], h["timeout"], note))

    with open(results_csv, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()) if rows else
                           ["problem", "mode", "verdict", "seconds"])
        w.writeheader()
        for r in rows:
            w.writerow(r)
    print("\nwrote %s (%d rows)" % (results_csv, len(rows)))
    timing_summary(rows)
    print("\n%s" % ("ALL CHECKS PASSED" if failures == 0 else "%d FAILURE(S)" % failures))
    return failures


def brute_models(clauses, nvars):
    if nvars > 16:
        return None
    models = []
    for mask in range(1 << nvars):
        m = set((v if (mask >> (v - 1)) & 1 else -v) for v in range(1, nvars + 1))
        if all(any(l in m for l in cl) for cl in clauses):
            models.append(m)
    return models


def timing_summary(rows):
    print("\n--- fastest backend per problem (race-to-first cells) ---")
    by_prob = {}
    for r in rows:
        if r["verdict"] not in ("SAT", "UNSAT"):
            continue
        by_prob.setdefault(r["problem"], []).append((r["seconds"], r["backend"], r["dag"]))
    for prob, cells in sorted(by_prob.items()):
        best = min(cells)
        print("  %-22s best %6.2fs  (%s / %s)" % (prob, best[0], best[1], best[2]))


# --------------------------------------------------------------------------
# HPC: emit a SLURM job array
# --------------------------------------------------------------------------
SLURM_TMPL = """#!/bin/bash
#SBATCH --job-name=dagster_matrix
#SBATCH --array=0-{last}
#SBATCH --nodes=1
#SBATCH --ntasks={max_ranks}
#SBATCH --time={hms}
#SBATCH --output={outdir}/cell_%a.out

# One array task per (problem, dag, mode) cell. Edit partition/account as needed.
export LD_LIBRARY_PATH=/usr/local/lib:$LD_LIBRARY_PATH
export OMPI_MCA_btl=self,tcp

CELLS=({cells})
LINE="${{CELLS[$SLURM_ARRAY_TASK_ID]}}"
# LINE = "RANKS DAG CNF <dagster flags...>"  (flags last so they absorb the rest)
read RANKS DAG CNF FLAGS <<< "$LINE"
SECONDS=0
srun -n $RANKS {bin} $FLAGS -e 0 "$DAG" "$CNF" -o {outdir}/sol_${{SLURM_ARRAY_TASK_ID}}.txt
RC=$?
# machine-readable line for collect.py (a missing line => the task hit the SLURM time limit)
echo "BACKENDMATRIX task=$SLURM_ARRAY_TASK_ID rc=$RC wall=$SECONDS"
"""


def emit_hpc(profile, outdir):
    os.makedirs(outdir, exist_ok=True)
    prob_dir = os.path.join(outdir, "problems")
    os.makedirs(prob_dir, exist_ok=True)
    probs = corpus_problems(profile["sizes"]) + real_problems(profile["sizes"])
    cells, meta = [], []
    for prob in probs:
        materialise_cnf(prob, prob_dir)
        dags = make_dags(prob, prob_dir, profile["node_targets"])
        for dag_label, dag_path in dags:
            nodes = dag_node_count(dag_path)
            for mode in profile["modes"]:
                ranks = ranks_for(mode, profile, nodes)
                flags = " ".join(mode_flags(mode, profile["sls_k"]))
                meta.append((len(cells), prob["name"], prob.get("family", ""), prob.get("size", ""),
                             dag_label, MODES[mode][0], ranks, prob.get("expected", "?")))
                cells.append("%d %s %s %s" % (ranks, dag_path, prob["cnf_path"], flags))
    hms = "%02d:%02d:00" % (profile["timeout"] // 3600, (profile["timeout"] % 3600) // 60)
    script = SLURM_TMPL.format(
        last=max(0, len(cells) - 1), max_ranks=profile["max_ranks"], hms=hms,
        outdir=os.path.abspath(outdir), bin=DAGSTER_BIN,
        cells=" ".join('"%s"' % c for c in cells))
    job = os.path.join(outdir, "matrix_array.slurm")
    with open(job, "w") as f:
        f.write(script)
    # machine-readable index for collect.py (task id -> what was run)
    manifest = os.path.join(outdir, "cells.tsv")
    with open(manifest, "w") as f:
        f.write("task\tproblem\tfamily\tsize\tdag\tbackend\tranks\texpected\n")
        for t, p, fam, sz, dl, be, r, exp in meta:
            f.write("%d\t%s\t%s\t%s\t%s\t%s\t%d\t%s\n" % (t, p, fam, sz, dl, be, r, exp))
    print("emitted %d cells" % len(cells))
    print("  job script : %s" % job)
    print("  manifest   : %s" % manifest)
    print("  submit with : sbatch %s" % job)
    print("  collect with: python3 collect.py %s" % os.path.abspath(outdir))


# --------------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--profile", choices=list(PROFILES), default="local")
    ap.add_argument("--quick", action="store_true", help="shortcut for --profile quick")
    ap.add_argument("--modes", help="comma list overriding the profile's modes, e.g. 5,11,12 "
                    "(0 tinisat 1 +sls 4 minisat 5 cadical 6 +sls 7 cms 8/9 +sls 11 glucose 12 lingeling)")
    ap.add_argument("--emit-hpc", metavar="DIR", help="(hpc) write SLURM array + problems to DIR")
    ap.add_argument("--results", default=os.path.join(HERE, "results.csv"))
    ap.add_argument("--keep", action="store_true", help="keep the scratch workdir")
    args = ap.parse_args()

    if args.quick:
        args.profile = "quick"
    profile = dict(PROFILES[args.profile])   # copy so --modes override doesn't mutate the global
    if args.modes:
        profile["modes"] = [int(x) for x in args.modes.split(",")]

    if not os.path.exists(DAGSTER_BIN):
        print("dagster binary not found at %s (run make)" % DAGSTER_BIN, file=sys.stderr)
        sys.exit(1)

    if args.profile == "hpc" or args.emit_hpc:
        eprof = dict(PROFILES["hpc"])
        if args.modes:
            eprof["modes"] = [int(x) for x in args.modes.split(",")]
        outdir = args.emit_hpc or os.path.join(HERE, "hpc")
        emit_hpc(eprof, outdir)
        sys.exit(0)

    workdir = tempfile.mkdtemp(prefix="dagster_matrix_")
    try:
        failures = run_local(profile, workdir, args.results)
    finally:
        if args.keep:
            print("workdir kept at %s" % workdir)
        else:
            shutil.rmtree(workdir, ignore_errors=True)
    sys.exit(1 if failures else 0)


if __name__ == "__main__":
    main()
