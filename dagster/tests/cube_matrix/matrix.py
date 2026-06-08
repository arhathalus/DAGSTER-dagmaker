#!/usr/bin/env python3
"""Cube-and-conquer test + benchmark harness for Dagster.

For each problem this runs the full cube pipeline and then the Dagster conquer,
checking correctness and measuring scaling:

  cube.py  (sanitize -> symmetry break -> march)  ->  cubes + formula + conquer DAG
  dagster --cubes  (master seeds cubes -> workers conquer formula+cube in parallel)

Checks (all RC-AWARE -- a non-zero dagster exit is ERR/TIMEOUT, never UNSAT):
  * CORRECTNESS: the cube-and-conquer verdict must equal the single-node ground
    truth of the same (sanitized/broken) formula -- for every conquer mode tested,
    including SLS-guided conquer (cadical+sls), which is the "no deadlock/crash" check.
  * BENCHMARK: cube count, conquer wall-clock per mode, and the speedup vs a plain
    single-node solve of the formula.

Profiles:
  --profile local : ~8-core box, run here.
  --profile hpc   : emit a SLURM job array (--emit-hpc DIR).

Modes (conquer engine): 5 cadical, 6 cadical+SLS, 10 cadical+CLAUSE SHARING.
  `--modes` names the configurations under test; each is dispatched via the flag
  interface (e.g. 10 -> `--backend cadical --share`), not the legacy -m selector.
  Clause sharing dedicates one rank as a hub that relays learned clauses between
  conquer workers. `--modes 5,10` is the head-to-head sharing benchmark:
  the speedup column then reads "share vs plain" (10 relative to 5). Gains show on
  HARD UNSAT with many cores -- the `hpc` profile includes a 'hard' track (a
  pigeonhole resolution ladder, holes 10-14) exactly for this: pigeonhole proofs
  are exponential, so conquer workers re-derive the same lemmas and sharing pays
  off. The 'small' toy problems finish in <1s so sharing is ~neutral there; use
  them only for the correctness/no-deadlock check.

Run the headline clause-sharing measurement with:
  python3 matrix.py --profile hpc --emit-hpc ./hpc --modes 5,10
  sbatch ./hpc/cube_array.slurm

Examples:
  python3 matrix.py --profile local
  python3 matrix.py --profile local --modes 5,6      # cadical, cadical+SLS
  python3 matrix.py --profile local --modes 5,10     # plain vs clause sharing
  python3 matrix.py --profile hpc --emit-hpc ./hpc --modes 5,10
"""

import argparse
import csv
import os
import re
import subprocess
import sys
import tempfile
import time

HERE = os.path.dirname(os.path.abspath(__file__))
DAGSTER_DIR = os.path.dirname(os.path.dirname(HERE))
REPO_ROOT = os.path.dirname(DAGSTER_DIR)
DAGSTER_BIN = os.path.join(DAGSTER_DIR, "dagster")
VENV_PY = os.path.join(REPO_ROOT, ".venv", "bin", "python")
CUBE_PY = os.path.join(REPO_ROOT, "utilities", "cube", "cube.py")
CORPUS_DIR = os.path.join(REPO_ROOT, "Benchmarks", "corpus")

ENV = dict(os.environ)
ENV["LD_LIBRARY_PATH"] = "/usr/local/lib:" + ENV.get("LD_LIBRARY_PATH", "")
ENV["OMPI_MCA_btl"] = "self,tcp"
ENV["GLOG_logtostderr"] = "true"
ENV["GLOG_v"] = "0"

MODE_LABEL = {0: "tinisat", 4: "minisat", 5: "cadical", 6: "cadical+sls",
              7: "cryptominisat", 8: "minisat+sls", 9: "cms+sls",
              10: "cadical+share"}
SLS_MODES = {1, 6, 8, 9}
SHARE_MODES = {10}            # cadical + clause hub (one rank is the hub)

# Dagster is driven with the orthogonal flag interface (--backend/--sls/--share),
# not the legacy numeric -m selector. `--modes` here is just a concise way to name
# the configurations under test; each maps to its flag form below.
def mode_flags(mode, k):
    base = {0:  ["--backend", "tinisat"],
            4:  ["--backend", "minisat"],
            5:  ["--backend", "cadical"],
            6:  ["--backend", "cadical", "--sls"],
            7:  ["--backend", "cryptominisat"],
            8:  ["--backend", "minisat", "--sls"],
            9:  ["--backend", "cryptominisat", "--sls"],
            10: ["--backend", "cadical", "--share"]}[mode]
    if mode in SLS_MODES:
        base += ["-k", str(k)]
    return base


# ---- problems (each: name, gen->(clauses,nvars), symbreak, march cutoff, size) ----
def _blocks(k, sz):
    cl = []
    for i in range(k):
        vs = [i * sz + j + 1 for j in range(sz)]
        cl.append(vs[:])
        for a in range(sz):
            for b in range(a + 1, sz):
                cl.append([-vs[a], -vs[b]])
    return cl, k * sz


def problems(sizes):
    out = []
    if CORPUS_DIR not in sys.path:
        sys.path.insert(0, CORPUS_DIR)
    G = None
    try:
        import generators as G
    except Exception as e:
        print("  (corpus unavailable: %s)" % e, file=sys.stderr)
    # (name, fn, symbreak, march_depth, size). symbreak 'full' for symmetric;
    # 'none'+depth for hard/asymmetric (force cubing with a shallow cutoff).
    specs = [("blocks5x3", lambda: _blocks(5, 3), "full", None, "small")]
    if G:
        specs += [
            ("pigeonhole6", lambda: G.pigeonhole(holes=5)[:2], "none", 6, "small"),
            ("pigeonhole7", lambda: G.pigeonhole(holes=6)[:2], "none", 6, "small"),
            ("grid6",       lambda: G.grid_coloring(S=6)[:2], "none", 5, "small"),
            ("modular",     lambda: G.modular(k=3, sz=6)[:2], "none", 5, "small"),
        ]
        # HARD UNSAT ladder: pigeonhole resolution is exponential, so each conquer
        # worker re-derives the same cardinality lemmas -- the canonical case where
        # clause sharing (-m 10 / --share) should pay off. symbreak 'none' keeps it
        # hard (breaking would collapse it); depth None -> cube count auto-tuned to
        # the rank count (see _cube_target). Spans a difficulty band so the HPC run
        # reveals where sharing helps.
        specs += [("pigeonhole%d" % (h + 1), (lambda h=h: G.pigeonhole(holes=h)[:2]),
                   "none", None, "hard") for h in (10, 11, 12, 13, 14)]
    res = []
    for name, fn, sb, depth, size in specs:
        if size not in sizes:
            continue
        try:
            cl, nv = fn()
        except Exception as e:
            print("  (skip %s: %s)" % (name, e), file=sys.stderr); continue
        res.append(dict(name=name, clauses=cl, nvars=nv, symbreak=sb, depth=depth, size=size))
    return res


# NOTE: the generated corpus's hard ramsey instances are deliberately NOT pulled in
# here. march's lookahead cubing is far too slow on those structured ~10^4-variable
# formulas (a single pass times out), so they are not cube-and-conquer material.
# They are exercised instead by backend_matrix (DAG decomposition, no march), where
# they already run. The hard track here is the pigeonhole ladder above.


def write_dimacs(path, clauses, nvars):
    with open(path, "w") as f:
        f.write("p cnf %d %d\n" % (nvars, len(clauses)))
        for c in clauses:
            f.write(" ".join(map(str, c)) + " 0\n")


def cnf_dims(path):
    with open(path, errors="replace") as f:
        for line in f:
            if line.startswith("p cnf"):
                p = line.split(); return int(p[2]), int(p[3])
    return 0, 0


def rng(a, b):
    return str(a) if a == b else "%d-%d" % (a, b)


def _single_dag(formula, workdir, name):
    """Write & return a single-node conquer DAG over `formula` (all clauses,
    report every var). Used both as the conquer DAG and the ground-truth DAG."""
    v, c = cnf_dims(formula)
    dag = os.path.join(workdir, name + ".conquer.dag")
    with open(dag, "w") as f:
        f.write("DAG-FILE\nNODES:1\nGRAPH:\nCLAUSES:\n0:%s\nREPORTING:\n%s\n" % (rng(0, c - 1), rng(1, v)))
    return dag


def run_cube(cnf, symbreak, depth, workdir, target=None):
    """Run cube.py. Returns dict with status in {cubes, solved-SAT, solved-UNSAT}
    plus cubes/formula/n_cubes. cube.py emits a machine-readable STATUS line.
    target (cube count) auto-tunes the march depth; takes precedence over depth."""
    base = os.path.join(workdir, os.path.splitext(os.path.basename(cnf))[0])
    cubes = base + ".icnf"
    formula = base + ".cube.cnf"
    cmd = [VENV_PY, CUBE_PY, cnf, "-o", cubes, "--final-cnf", formula,
           "--symbreak", symbreak, "--quiet"]
    if target is not None:
        cmd += ["--target-cubes", str(target)]
    elif depth is not None:
        cmd += ["--march-depth", str(depth)]
    try:
        p = subprocess.run(cmd, env=ENV, capture_output=True, text=True, timeout=600)
    except subprocess.TimeoutExpired:
        return dict(ok=False, err="cube.py timeout")
    m = re.search(r"STATUS (\S+)\s+CUBES (\d+)", p.stdout)
    if not m:
        return dict(ok=False, err=(p.stdout + p.stderr)[-200:])
    status, n = m.group(1), int(m.group(2))
    if status == "cubes":
        return dict(ok=True, status="cubes", cubes=cubes, formula=formula, n_cubes=n)
    if status.startswith("solved"):       # march solved it directly -- a valid outcome
        return dict(ok=True, status=status, formula=formula, n_cubes=0)
    return dict(ok=False, err="cube.py status %s" % status)


def run_dagster(mode, dag, cnf, ranks, timeout, cubes=None, k=2):
    out = os.path.join(tempfile.gettempdir(), "_cube_sol.txt")
    if os.path.exists(out):
        os.remove(out)
    cmd = (["mpirun", "-n", str(ranks), "--oversubscribe", "-x", "LD_LIBRARY_PATH",
            "-x", "OMPI_MCA_btl", DAGSTER_BIN]
           + mode_flags(mode, k) + ["-e", "0"])
    if cubes:
        cmd += ["--cubes", cubes]
    cmd += [dag, cnf, "-o", out]
    t0 = time.time()
    try:
        p = subprocess.run(cmd, env=ENV, cwd=DAGSTER_DIR, timeout=timeout,
                           stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    except subprocess.TimeoutExpired:
        return ("TIMEOUT", timeout)
    dt = time.time() - t0
    if p.returncode != 0:
        return ("ERR(%d)" % p.returncode, dt)
    v = "SAT" if (os.path.exists(out) and os.path.getsize(out) > 0) else "UNSAT"
    if os.path.exists(out):
        os.remove(out)
    return (v, dt)


PROFILES = {
    "local": dict(max_ranks=8, timeout=60, sizes={"small"}, modes=[5, 6], sls_k=2),
    "hpc":   dict(max_ranks=64, timeout=3600, sizes={"small", "medium", "large", "hard"},
                  modes=[5, 6], sls_k=8),
}


def ranks_for(profile, mode, n_cubes):
    if mode in SLS_MODES:                 # master + (worker + k helpers); 1 conquer group here
        return min(profile["max_ranks"], 2 + profile["sls_k"])
    if mode in SHARE_MODES:               # master + W conquer workers + 1 clause hub (>= 3 total)
        workers = min(n_cubes, profile["max_ranks"] - 2)
        return min(profile["max_ranks"], max(3, 2 + max(1, workers)))
    return min(profile["max_ranks"], max(2, 1 + min(n_cubes, profile["max_ranks"] - 1)))


def materialise(prob, workdir):
    """Return a CNF path for `prob`: a corpus instance carries cnf_path (used in
    place); an in-memory instance is written to workdir."""
    if prob.get("cnf_path"):
        return prob["cnf_path"]
    cnf0 = os.path.join(workdir, prob["name"] + ".cnf")
    write_dimacs(cnf0, prob["clauses"], prob["nvars"])
    return cnf0


def cube_target(prob, profile):
    """Hard instances: auto-tune the cube count to the rank budget (~8 cubes per
    worker) for load balance. Others use their fixed march depth."""
    if prob.get("size") == "hard" and prob.get("depth") is None:
        return 8 * profile["max_ranks"]
    return None


def run_local(profile, modes, workdir, results_csv):
    probs = problems(profile["sizes"])
    print("collected %d problems; modes=%s\n" % (len(probs), [MODE_LABEL[m] for m in modes]))
    rows, failures = [], 0
    for prob in probs:
        cnf0 = materialise(prob, workdir)
        cb = run_cube(cnf0, prob["symbreak"], prob.get("depth"), workdir, target=cube_target(prob, profile))
        if not cb["ok"]:
            print("== %-12s cube.py FAILED: %s ==\n" % (prob["name"], cb.get("err", "?")[:60]))
            failures += 1; continue
        formula = cb["formula"]
        v, c = cnf_dims(formula)
        gt0, _ = run_dagster(5, _single_dag(formula, workdir, prob["name"]), formula, 2, profile["timeout"])
        if cb["status"].startswith("solved"):
            # march solved the formula outright -- no cubes to conquer. Validate
            # its verdict against ground truth and move on (not a failure).
            direct = "SAT" if cb["status"] == "solved-SAT" else "UNSAT"
            flag = "" if direct == gt0 else "  <-- march verdict != ground truth"
            if flag:
                failures += 1
            print("== %-12s (%s) -- march SOLVED directly: %s (ground truth %s)%s ==\n"
                  % (prob["name"], prob["symbreak"], direct, gt0, flag))
            continue
        cubes, ncubes, gt = cb["cubes"], cb["n_cubes"], gt0
        dag = _single_dag(formula, workdir, prob["name"])   # same DAG used for gt0
        print("== %-12s (%s, %d vars, %d clauses, %d cubes) -- ground truth %s ==" %
              (prob["name"], prob["symbreak"], v, c, ncubes, gt))
        base_t = None
        for mode in modes:
            ranks = ranks_for(profile, mode, ncubes)
            verdict, secs = run_dagster(mode, dag, formula, ranks, profile["timeout"],
                                        cubes=cubes, k=profile["sls_k"])
            if mode == modes[0] and verdict in ("SAT", "UNSAT"):
                base_t = secs
            flag = ""
            if verdict in ("SAT", "UNSAT") and gt in ("SAT", "UNSAT") and verdict != gt:
                flag = "  <-- WRONG (conquer != ground truth)"; failures += 1
            elif verdict.startswith(("ERR", "TIMEOUT")):
                flag = "  <-- %s" % verdict
            sp = "  (%.2fx)" % (base_t / secs) if (base_t and secs > 0 and verdict in ("SAT", "UNSAT")) else ""
            print("   %-12s ranks=%d  %-7s %7.2fs%s%s" % (MODE_LABEL[mode], ranks, verdict, secs, sp, flag))
            rows.append(dict(problem=prob["name"], mode=MODE_LABEL[mode], cubes=ncubes,
                             ranks=ranks, verdict=verdict, ground_truth=gt, seconds=round(secs, 3)))
        print()
    if rows:
        with open(results_csv, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(rows[0].keys())); w.writeheader(); w.writerows(rows)
        print("wrote %s (%d rows)" % (results_csv, len(rows)))
    print("\n%s" % ("ALL CHECKS PASSED" if failures == 0 else "%d FAILURE(S)" % failures))
    return failures


SLURM_TMPL = """#!/bin/bash
#SBATCH --job-name=cube_matrix
#SBATCH --array=0-{last}
#SBATCH --nodes=1
#SBATCH --ntasks={max_ranks}
#SBATCH --time={hms}
#SBATCH --output={outdir}/cell_%a.out
export LD_LIBRARY_PATH=/usr/local/lib:$LD_LIBRARY_PATH
export OMPI_MCA_btl=self,tcp
CELLS=({cells})
# each cell: RANKS CUBES DAG CNF <dagster flags...>  (flags last so they absorb the rest)
read RANKS CUBES DAG CNF FLAGS <<< "${{CELLS[$SLURM_ARRAY_TASK_ID]}}"
SECONDS=0
srun -n $RANKS {bin} $FLAGS -e 0 --cubes "$CUBES" "$DAG" "$CNF" -o {outdir}/sol_${{SLURM_ARRAY_TASK_ID}}.txt
RC=$?
# machine-readable line for collect.py (a missing line => the task hit the SLURM time limit)
echo "CUBEMATRIX task=$SLURM_ARRAY_TASK_ID rc=$RC wall=$SECONDS"
"""


def emit_hpc(profile, modes, outdir):
    outdir = os.path.abspath(outdir)
    pdir = os.path.join(outdir, "problems"); os.makedirs(pdir, exist_ok=True)
    cells, meta = [], []
    for prob in problems(profile["sizes"]):
        cnf0 = materialise(prob, pdir)
        cb = run_cube(cnf0, prob["symbreak"], prob.get("depth"), pdir, target=cube_target(prob, profile))
        if not cb["ok"]:
            print("  (skip %s: cube.py failed)" % prob["name"]); continue
        if cb["status"].startswith("solved"):
            print("  (skip %s: march solved it directly, %s -- no cubes)" % (prob["name"], cb["status"])); continue
        dag = _single_dag(cb["formula"], pdir, prob["name"])
        for mode in modes:
            ranks = ranks_for(profile, mode, cb["n_cubes"])
            flags = " ".join(mode_flags(mode, profile["sls_k"]))
            meta.append((len(cells), prob["name"], MODE_LABEL[mode], ranks, cb["n_cubes"], prob["size"]))
            cells.append("%d %s %s %s %s" % (ranks, cb["cubes"], dag, cb["formula"], flags))
    hms = "%02d:%02d:00" % (profile["timeout"] // 3600, (profile["timeout"] % 3600) // 60)
    job = os.path.join(outdir, "cube_array.slurm")
    with open(job, "w") as f:
        f.write(SLURM_TMPL.format(last=max(0, len(cells) - 1), max_ranks=profile["max_ranks"],
                hms=hms, outdir=outdir, bin=DAGSTER_BIN, cells=" ".join('"%s"' % c for c in cells)))
    # machine-readable index for collect.py (task id -> what was run)
    with open(os.path.join(outdir, "cells.tsv"), "w") as f:
        f.write("task\tproblem\tmode\tranks\tcubes\tsize\n")
        for t, p, m, r, nc, sz in meta:
            f.write("%d\t%s\t%s\t%d\t%d\t%s\n" % (t, p, m, r, nc, sz))
    print("emitted %d cells -> %s\n  (collect results after the run with: python3 collect.py %s)"
          % (len(cells), job, outdir))


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--profile", choices=list(PROFILES), default="local")
    ap.add_argument("--modes", default=None)
    ap.add_argument("--emit-hpc", metavar="DIR")
    ap.add_argument("--results", default=os.path.join(HERE, "results.csv"))
    args = ap.parse_args()
    if not os.path.exists(DAGSTER_BIN):
        sys.exit("dagster not built at %s" % DAGSTER_BIN)
    profile = PROFILES[args.profile]
    modes = [int(x) for x in args.modes.split(",")] if args.modes else profile["modes"]
    if args.profile == "hpc" or args.emit_hpc:
        emit_hpc(PROFILES["hpc"], modes, args.emit_hpc or os.path.join(HERE, "hpc")); sys.exit(0)
    import shutil
    workdir = tempfile.mkdtemp(prefix="cube_matrix_")
    try:
        failures = run_local(profile, modes, workdir, args.results)
    finally:
        shutil.rmtree(workdir, ignore_errors=True)
    sys.exit(1 if failures else 0)


if __name__ == "__main__":
    main()
