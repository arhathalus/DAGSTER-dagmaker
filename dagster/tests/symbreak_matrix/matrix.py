#!/usr/bin/env python3
"""Symmetry-breaking x DAG-generation test + benchmark harness.

For each problem and each --symbreak level (none / light / full / dag), this:
  * generates a DAG with dagmake (so it exercises DAG generation over the
    possibly-augmented CNF),
  * SANITY-CHECKS correctness two ways:
      - ground truth: the (broken) CNF solved single-node must equal the ORIGINAL
        verdict  -> symmetry breaking is verdict-preserving;
      - DAG soundness: the decomposed run must equal that ground truth
        -> the DAG over the (broken) CNF is sound;
  * MEASURES wall-clock time solving the DAG, so we can see whether breaking (and
    which level) actually speeds things up, and what it does to the DAG shape
    (nodes / max_sep / parallel_width / kept-vs-dropped breaking clauses).

Everything is RC-AWARE: a non-zero dagster exit (parse abort, crash, timeout) is
reported as ERR/TIMEOUT, never silently as UNSAT. (Reading "no output" as UNSAT
is exactly the trap that produced false results during development.)

Profiles:
  --profile local  : ~8-core workstation; small/medium instances; run here.
  --profile hpc    : emit a SLURM job array (one task per cell) via --emit-hpc.

Examples:
  python3 matrix.py --profile local
  python3 matrix.py --profile local --modes 0,5      # tinisat + cadical
  python3 matrix.py --profile hpc --emit-hpc ./hpc
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
DAGSTER_DIR = os.path.dirname(os.path.dirname(HERE))       # .../dagster (build dir)
REPO_ROOT = os.path.dirname(DAGSTER_DIR)
DAGSTER_BIN = os.path.join(DAGSTER_DIR, "dagster")
VENV_PY = os.path.join(REPO_ROOT, ".venv", "bin", "python")
DAGMAKE = os.path.join(REPO_ROOT, "utilities", "dag-generator", "dagmake.py")
CORPUS_DIR = os.path.join(REPO_ROOT, "Benchmarks", "corpus")

ENV = dict(os.environ)
ENV["LD_LIBRARY_PATH"] = "/usr/local/lib:" + ENV.get("LD_LIBRARY_PATH", "")
ENV["OMPI_MCA_btl"] = "self,tcp"
ENV["GLOG_logtostderr"] = "true"
ENV["GLOG_v"] = "0"

LEVELS = ("none", "light", "full", "dag")
MODE_LABEL = {0: "tinisat", 4: "minisat", 5: "cadical", 7: "cryptominisat"}


# --------------------------------------------------------------------------
# problems (symmetry-bearing ones are the point)
# --------------------------------------------------------------------------
def write_dimacs(path, clauses, nvars):
    with open(path, "w") as f:
        f.write("p cnf %d %d\n" % (nvars, len(clauses)))
        for cl in clauses:
            f.write(" ".join(str(l) for l in cl) + " 0\n")


def _blocks(k, sz):
    """k identical exactly-one-of-sz blocks: within-block S_sz (local) + block
    interchange (global). SAT. The showcase for DAG-aware breaking."""
    cl = []
    for i in range(k):
        vs = [i * sz + j + 1 for j in range(sz)]
        cl.append(vs[:])
        for a in range(sz):
            for b in range(a + 1, sz):
                cl.append([-vs[a], -vs[b]])
    return cl, k * sz


def problems(sizes):
    """List of dicts {name, clauses, nvars, size}. Symmetric / structured."""
    out = []
    if CORPUS_DIR not in sys.path:
        sys.path.insert(0, CORPUS_DIR)
    G = None
    try:
        import generators as G
    except Exception as e:
        print("  (corpus generators unavailable: %s)" % e, file=sys.stderr)
    specs = [
        ("blocks5x3", lambda: _blocks(5, 3), "small"),
        ("blocks8x4", lambda: _blocks(8, 4), "small"),
    ]
    if G is not None:
        specs += [
            ("pigeonhole6", lambda: G.pigeonhole(holes=5)[:2], "small"),
            ("pigeonhole7", lambda: G.pigeonhole(holes=6)[:2], "medium"),
            ("grid6",       lambda: G.grid_coloring(S=6)[:2], "small"),
            ("modular",     lambda: G.modular(k=3, sz=6)[:2], "small"),
        ]
    for name, fn, size in specs:
        if size not in sizes:
            continue
        try:
            clauses, nvars = fn()
        except Exception as e:
            print("  (skip %s: %s)" % (name, e), file=sys.stderr)
            continue
        out.append(dict(name=name, clauses=clauses, nvars=nvars, size=size))
    return out


# --------------------------------------------------------------------------
# dagmake (DAG generation, per symbreak level)
# --------------------------------------------------------------------------
def run_dagmake(cnf_in, dag_out, level, nodes, max_sep, timeout=180):
    """Returns dict: ok, cnf (the CNF the DAG references), and parsed stats."""
    cmd = [VENV_PY, DAGMAKE, "--symbreak", level, "--nodes", str(nodes),
           "--max-sep", str(max_sep), cnf_in, dag_out]
    try:
        p = subprocess.run(cmd, env=ENV, cwd=os.path.dirname(DAGMAKE),
                           capture_output=True, text=True, timeout=timeout)
    except subprocess.TimeoutExpired:
        return dict(ok=False, err="dagmake timeout")
    if p.returncode != 0 or not os.path.exists(dag_out):
        return dict(ok=False, err=(p.stderr or p.stdout)[-300:])
    out = p.stdout
    cnf = cnf_in if level == "none" else os.path.splitext(dag_out)[0] + ".symbroken.cnf"
    stats = dict(ok=True, cnf=cnf)
    star = next((l for l in out.splitlines() if l.lstrip().startswith("*")), "")
    for key in ("nodes", "max_sep", "parallel_width", "edges"):
        m = re.search(key + r"=(\d+)", star)
        stats[key] = int(m.group(1)) if m else None
    m = re.search(r"(\d+) generators", out)
    stats["generators"] = int(m.group(1)) if m else 0
    m = re.search(r"kept (\d+) node-local / dropped (\d+)", out)
    if m:
        stats["kept"], stats["dropped"] = int(m.group(1)), int(m.group(2))
    return stats


# --------------------------------------------------------------------------
# dagster (rc-aware)
# --------------------------------------------------------------------------
def run_dagster(mode, dag, cnf, ranks, timeout, enumerate_all=False):
    """Return (verdict, seconds). verdict in {SAT, UNSAT, TIMEOUT, ERR(rc)}."""
    out = os.path.join(tempfile.gettempdir(), "_symbreak_sol.txt")
    cmd = ["mpirun", "-n", str(ranks), "--oversubscribe", "-x", "LD_LIBRARY_PATH",
           "-x", "OMPI_MCA_btl", DAGSTER_BIN, "--backend", MODE_LABEL[mode],
           "-e", "1" if enumerate_all else "0", dag, cnf, "-o", out]
    if os.path.exists(out):
        os.remove(out)
    t0 = time.time()
    try:
        p = subprocess.run(cmd, env=ENV, cwd=DAGSTER_DIR, timeout=timeout,
                           stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    except subprocess.TimeoutExpired:
        return ("TIMEOUT", timeout)
    dt = time.time() - t0
    if p.returncode != 0:
        return ("ERR(%d)" % p.returncode, dt)
    has = os.path.exists(out) and os.path.getsize(out) > 0
    if os.path.exists(out):
        os.remove(out)
    return ("SAT" if has else "UNSAT", dt)


def ground_truth(mode, cnf, timeout):
    """Single-node (no decomposition) verdict of a CNF -- the soundness oracle."""
    nvars, nclauses = cnf_dims(cnf)
    dag = os.path.join(tempfile.gettempdir(), "_symbreak_gt.dag")
    with open(dag, "w") as f:
        f.write("DAG-FILE\nNODES:1\nGRAPH:\nCLAUSES:\n0:%s\nREPORTING:\n%s\n"
                % (rng(0, nclauses - 1), rng(1, nvars)))
    v, _ = run_dagster(mode, dag, cnf, 2, timeout)
    return v


def rng(a, b):
    return str(a) if a == b else "%d-%d" % (a, b)


def cnf_dims(path):
    with open(path, errors="replace") as f:
        for line in f:
            if line.startswith("p cnf"):
                p = line.split()
                return int(p[2]), int(p[3])
    return 0, 0


# --------------------------------------------------------------------------
PROFILES = {
    "local": dict(max_ranks=8, max_sep=40, timeout=60, sizes={"small", "medium"},
                  modes=[5], target_nodes=8),
    "hpc":   dict(max_ranks=64, max_sep=60, timeout=3600, sizes={"small", "medium", "large"},
                  modes=[0, 5], target_nodes=16),
}


def ranks_for(profile, dag_nodes):
    return min(profile["max_ranks"], max(2, 1 + (dag_nodes or 1)))


def run_local(profile, modes, workdir, results_csv):
    probs = problems(profile["sizes"])
    print("collected %d problems; levels=%s; modes=%s; max_ranks=%d\n"
          % (len(probs), list(LEVELS), [MODE_LABEL[m] for m in modes], profile["max_ranks"]))
    rows = []
    failures = 0
    for prob in probs:
        cnf0 = os.path.join(workdir, prob["name"] + ".cnf")
        write_dimacs(cnf0, prob["clauses"], prob["nvars"])
        print("== %-12s (%d vars, %d clauses) ==" % (prob["name"], prob["nvars"], len(prob["clauses"])))
        for mode in modes:
            orig_v = ground_truth(mode, cnf0, profile["timeout"])
            print("   [%s] original verdict: %s" % (MODE_LABEL[mode], orig_v))
            print("   %-6s %-7s %-26s %-9s %8s   %s" %
                  ("level", "verdict", "DAG", "gtruth", "wall", "breaking"))
            verdicts = {}
            base_time = None
            for level in LEVELS:
                dag = os.path.join(workdir, "%s_%s.dag" % (prob["name"], level))
                dm = run_dagmake(cnf0, dag, level, profile["target_nodes"], profile["max_sep"])
                if not dm.get("ok"):
                    print("   %-6s dagmake FAILED: %s" % (level, dm.get("err", "?")[:50]))
                    failures += 1
                    continue
                cnf = dm["cnf"]
                gt = ground_truth(mode, cnf, profile["timeout"])         # broken-CNF oracle
                ranks = ranks_for(profile, dm.get("nodes"))
                verdict, secs = run_dagster(mode, dag, cnf, ranks, profile["timeout"])
                verdicts[level] = verdict
                if level == "none":
                    base_time = secs if verdict in ("SAT", "UNSAT") else None
                dagdesc = "n=%s sep=%s pw=%s" % (dm.get("nodes"), dm.get("max_sep"), dm.get("parallel_width"))
                brk = ""
                if "kept" in dm:
                    brk = "kept %d / dropped %d" % (dm["kept"], dm["dropped"])
                elif dm.get("generators"):
                    brk = "%d gens" % dm["generators"]
                flag = ""
                if gt in ("SAT", "UNSAT") and gt != orig_v:
                    flag = " <-SYMBREAK UNSOUND"; failures += 1
                if verdict in ("SAT", "UNSAT") and gt in ("SAT", "UNSAT") and verdict != gt:
                    flag += " <-DAG UNSOUND"; failures += 1
                sp = ""
                if base_time and verdict in ("SAT", "UNSAT") and secs > 0:
                    sp = "  (%.2fx)" % (base_time / secs)
                print("   %-6s %-7s %-26s %-9s %7.2fs%s %s%s" %
                      (level, verdict, dagdesc, gt, secs, sp, brk, flag))
                rows.append(dict(problem=prob["name"], mode=MODE_LABEL[mode], level=level,
                                 verdict=verdict, ground_truth=gt, orig=orig_v,
                                 nodes=dm.get("nodes"), max_sep=dm.get("max_sep"),
                                 parallel_width=dm.get("parallel_width"), ranks=ranks,
                                 kept=dm.get("kept"), dropped=dm.get("dropped"),
                                 generators=dm.get("generators"), seconds=round(secs, 3)))
            # verdict parity across levels (all must agree; symmetry breaking is verdict-preserving)
            real = set(v for v in verdicts.values() if v in ("SAT", "UNSAT"))
            if len(real) > 1:
                print("   PARITY FAIL across levels: %s" % verdicts); failures += 1
            print()

    if rows:
        with open(results_csv, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
            w.writeheader()
            w.writerows(rows)
        print("wrote %s (%d rows)" % (results_csv, len(rows)))
    print("\n%s" % ("ALL SANITY CHECKS PASSED" if failures == 0 else "%d FAILURE(S)" % failures))
    return failures


# --------------------------------------------------------------------------
# HPC: emit a SLURM array (DAGs are generated up front; tasks just solve)
# --------------------------------------------------------------------------
SLURM_TMPL = """#!/bin/bash
#SBATCH --job-name=symbreak_matrix
#SBATCH --array=0-{last}
#SBATCH --nodes=1
#SBATCH --ntasks={max_ranks}
#SBATCH --time={hms}
#SBATCH --output={outdir}/cell_%a.out
export LD_LIBRARY_PATH=/usr/local/lib:$LD_LIBRARY_PATH
export OMPI_MCA_btl=self,tcp
CELLS=({cells})
read BACKEND RANKS DAG CNF <<< "${{CELLS[$SLURM_ARRAY_TASK_ID]}}"
SECONDS=0
srun -n $RANKS {bin} --backend $BACKEND -e 0 "$DAG" "$CNF" -o {outdir}/sol_${{SLURM_ARRAY_TASK_ID}}.txt
RC=$?
# machine-readable line for collect.py (a missing line => the task hit the SLURM time limit)
echo "SYMBREAK task=$SLURM_ARRAY_TASK_ID rc=$RC wall=$SECONDS"
"""


def emit_hpc(profile, modes, outdir):
    # absolute: run_dagmake runs dagmake in a different cwd, so relative CNF/DAG
    # paths (e.g. the default "./hpc") would resolve against the wrong directory.
    outdir = os.path.abspath(outdir)
    os.makedirs(outdir, exist_ok=True)
    pdir = os.path.join(outdir, "problems")
    os.makedirs(pdir, exist_ok=True)
    cells, meta = [], []
    for prob in problems(profile["sizes"]):
        cnf0 = os.path.join(pdir, prob["name"] + ".cnf")
        write_dimacs(cnf0, prob["clauses"], prob["nvars"])
        for level in LEVELS:
            dag = os.path.join(pdir, "%s_%s.dag" % (prob["name"], level))
            dm = run_dagmake(cnf0, dag, level, profile["target_nodes"], profile["max_sep"])
            if not dm.get("ok"):
                print("  (skip %s/%s: dagmake failed)" % (prob["name"], level)); continue
            ranks = ranks_for(profile, dm.get("nodes"))
            for mode in modes:
                meta.append((len(cells), prob["name"], MODE_LABEL[mode], level, ranks,
                             dm.get("nodes"), dm.get("max_sep"), dm.get("parallel_width"),
                             dm.get("kept"), dm.get("dropped"), dm.get("generators")))
                cells.append("%s %d %s %s" % (MODE_LABEL[mode], ranks, dag, dm["cnf"]))
    hms = "%02d:%02d:00" % (profile["timeout"] // 3600, (profile["timeout"] % 3600) // 60)
    script = SLURM_TMPL.format(last=max(0, len(cells) - 1), max_ranks=profile["max_ranks"],
                               hms=hms, outdir=os.path.abspath(outdir), bin=DAGSTER_BIN,
                               cells=" ".join('"%s"' % c for c in cells))
    job = os.path.join(outdir, "symbreak_array.slurm")
    with open(job, "w") as f:
        f.write(script)
    # machine-readable index for collect.py (task id -> what was run, incl. DAG shape)
    cols = ["task", "problem", "backend", "level", "ranks", "nodes", "max_sep",
            "parallel_width", "kept", "dropped", "generators"]
    with open(os.path.join(outdir, "cells.tsv"), "w") as f:
        f.write("\t".join(cols) + "\n")
        for row in meta:
            f.write("\t".join("" if v is None else str(v) for v in row) + "\n")
    print("emitted %d cells -> %s  (sbatch it)" % (len(cells), job))
    print("  collect with: python3 collect.py %s" % outdir)


# --------------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--profile", choices=list(PROFILES), default="local")
    ap.add_argument("--modes", default=None, help="comma list of solver modes (default: profile)")
    ap.add_argument("--emit-hpc", metavar="DIR")
    ap.add_argument("--results", default=os.path.join(HERE, "results.csv"))
    args = ap.parse_args()

    if not os.path.exists(DAGSTER_BIN):
        print("dagster binary not found at %s" % DAGSTER_BIN, file=sys.stderr); sys.exit(1)
    profile = PROFILES[args.profile]
    modes = [int(x) for x in args.modes.split(",")] if args.modes else profile["modes"]

    if args.profile == "hpc" or args.emit_hpc:
        emit_hpc(PROFILES["hpc"], modes, args.emit_hpc or os.path.join(HERE, "hpc"))
        sys.exit(0)

    import tempfile, shutil
    workdir = tempfile.mkdtemp(prefix="symbreak_matrix_")
    try:
        failures = run_local(profile, modes, workdir, args.results)
    finally:
        shutil.rmtree(workdir, ignore_errors=True)
    sys.exit(1 if failures else 0)


if __name__ == "__main__":
    main()
