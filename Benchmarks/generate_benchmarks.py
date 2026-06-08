#!/usr/bin/env python3
"""Generate a curated benchmark corpus from Dagster's problem generators.

Builds the bundled generators, emits DIMACS CNFs (+ the generator's native DAG)
into Benchmarks/generated/, and LABELS each instance with an INDEPENDENT oracle:
standalone CaDiCaL on the raw CNF -- no Dagster, no DAG, no cube/MPI/projection
machinery. Labelling the corpus with a tool *other* than the one under test is the
point: a Dagster bug then shows up as a disagreement with the label rather than
hiding behind it. SAT instances additionally have their full model validated
against every clause.

Each instance is tagged with a TRACK:
  known  settled & oracle-confirmed (SAT/UNSAT)  -> regression test data
  hard   solver-uncertain frontier               -> scaling benchmark (may TIMEOUT)
  open   genuinely open mathematics              -> a Dagster research TARGET (unlabelled)

Families:
  costas N         Costas arrays (SAT for the n shipped); generator emits a 2-node DAG.
  determinant S B  max-determinant #SAT enumeration over an SxS matrix, B bits/entry (SAT).
  ramsey N M [Z]   Ramsey/Monk relation-algebra representability: colour K_N's edges with
                   M colours, no monochromatic triangle, every non-mono triangle everywhere.
                   Representable for all colour counts <=120 EXCEPT the open pair M=8, M=13
                   (cyclic constructions proven absent; general existence open). This is a
                   NON-cyclic SAT encoding, so its minimum representations are smaller than
                   the algebraic (cyclic) ones -- e.g. M=3 is representable on N=7 points.

Open targets (M in {8,13}) are the real prize: a SAT result at some N is a brand-new
non-cyclic representation; UNSAT up to N tightens the lower bound (a non-existence claim
needs sound symmetry breaking + a DRAT proof to be a theorem -- see README).

Other bundled generators that work but need bespoke setup (NOT auto-wired):
  pentomino  (Benchmarks/Pentomino/pentominos.py -- nested create/generate/dag-make)
  gensat     (Benchmarks/gensat_sat/ggensata2.c -- random 3SAT; verdict only known after solving)

Usage:
  python3 generate_benchmarks.py                 # default grid (known + a little hard/open)
  python3 generate_benchmarks.py --timeout 120   # per-instance oracle cap (hard/open hit it)
  python3 generate_benchmarks.py --tracks known  # only (re)generate one track
"""

import argparse
import os
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))            # .../Benchmarks
REPO_ROOT = os.path.dirname(HERE)
GEN_DIR = os.path.join(HERE, "generated")
VENV_PY = os.path.join(REPO_ROOT, ".venv", "bin", "python")
PYTHON = VENV_PY if os.path.exists(VENV_PY) else sys.executable

COSTAS_BIN = os.path.join(HERE, "costas", "generate_costas_N")
RAMSEY_BIN = os.path.join(HERE, "ramsey", "generate_ramsey_NM")
DETERMINANT = os.path.join(HERE, "determinant", "determinant.py")
# Independent verdict oracle: the standalone CaDiCaL binary (NOT Dagster's wrapper).
CADICAL = os.path.join(REPO_ROOT, "dagster", "cadical_solver", "cadical", "build", "cadical")

ENV = dict(os.environ)

# (family, params, track). See module docstring for track meanings. `size` is
# derived from the generated variable count, not hard-coded.
SPECS = [
    # --- known-answer regression data --------------------------------------
    dict(family="costas", params=(5,), track="known"),
    dict(family="costas", params=(6,), track="known"),
    dict(family="costas", params=(7,), track="known"),
    dict(family="costas", params=(8,), track="known"),
    dict(family="costas", params=(9,), track="known"),
    dict(family="determinant", params=(3, 8), track="known"),
    dict(family="determinant", params=(4, 8), track="known"),
    dict(family="determinant", params=(5, 8), track="known"),
    # ramsey known-answer pairs: min representation (SAT) and one below it (UNSAT)
    dict(family="ramsey", params=(3, 2, 0), track="known"),   # 2 colours, below min -> UNSAT
    dict(family="ramsey", params=(4, 2, 0), track="known"),   # 2 colours, min rep   -> SAT
    dict(family="ramsey", params=(6, 3, 0), track="known"),   # 3 colours, below min -> UNSAT
    dict(family="ramsey", params=(7, 3, 0), track="known"),   # 3 colours, min rep   -> SAT
    dict(family="ramsey", params=(6, 8, 0), track="known"),   # 8 colours, no small rep -> UNSAT
    # --- hard scaling frontier (oracle may TIMEOUT under the cap) -----------
    dict(family="ramsey", params=(11, 4, 0), track="hard"),   # 4 colours near its min rep
    dict(family="ramsey", params=(12, 5, 0), track="hard"),
    dict(family="ramsey", params=(13, 6, 0), track="hard"),
    # --- OPEN research targets: representability unknown (cyclic ruled out) --
    dict(family="ramsey", params=(15, 8, 0), track="open"),   # n=8 open case, past the refuted range
    dict(family="ramsey", params=(14, 13, 0), track="open"),  # n=13 open case
]


def size_of(nvars):
    return "small" if nvars < 2000 else ("medium" if nvars < 10000 else "large")


def cnf_dims(path):
    try:
        with open(path, errors="replace") as f:
            for line in f:
                if line.startswith("p cnf"):
                    t = line.split()
                    return int(t[2]), int(t[3])
    except FileNotFoundError:
        pass
    return 0, 0


def build_generators():
    print("[build] costas + ramsey C generators ...")
    for d in ("costas", "ramsey"):
        p = subprocess.run(["make", "-C", os.path.join(HERE, d)], capture_output=True, text=True)
        if p.returncode != 0:
            print(p.stdout[-300:] + p.stderr[-300:])


def generate(family, params):
    """Produce (name, cnf_path, dag_path) in GEN_DIR for one instance."""
    if family == "costas":
        (n,) = params
        name = "costas_%d" % n
        cnf = os.path.join(GEN_DIR, name + ".cnf")
        with open(cnf, "w") as out, open(os.path.join(GEN_DIR, name + ".map"), "w") as mp:
            subprocess.run([COSTAS_BIN, "-N", str(n)], stdout=out, stderr=mp, cwd=GEN_DIR, env=ENV)
        return name, cnf, os.path.join(GEN_DIR, "costas_%d.dag" % n)
    if family == "ramsey":
        n, m, z = params
        name = "ramsey_%d_%d" % (n, m)
        cnf = os.path.join(GEN_DIR, name + ".cnf")
        with open(cnf, "w") as out, open(os.path.join(GEN_DIR, name + ".map"), "w") as mp:
            subprocess.run([RAMSEY_BIN, "-N", str(n), "-M", str(m), "-Z", str(z)],
                           stdout=out, stderr=mp, cwd=GEN_DIR, env=ENV)
        return name, cnf, os.path.join(GEN_DIR, "dag_%d_%d.dag" % (n, m))
    if family == "determinant":
        s, b = params
        name = "determinant_%d_%d" % (s, b)
        cnf = os.path.join(GEN_DIR, name + ".cnf")
        dag = os.path.join(GEN_DIR, name + ".dag")
        subprocess.run([PYTHON, DETERMINANT, str(s), str(b), cnf,
                        os.path.join(GEN_DIR, name + ".map"), dag], env=ENV,
                       capture_output=True, text=True)
        return name, cnf, dag
    raise ValueError(family)


def validate_model(cnf, model):
    """Every clause has a true literal under `model` (a set of signed ints)?"""
    with open(cnf, errors="replace") as f:
        for line in f:
            line = line.strip()
            if not line or line[0] in "pc%":
                continue
            lits = [int(x) for x in line.split() if x not in ("", "0")]
            if lits and not any(l in model for l in lits):
                return False
    return True


def label(cnf, track, timeout):
    """Label with the INDEPENDENT oracle (standalone CaDiCaL on the raw CNF).

    Returns (verdict, seconds, model_check) where model_check is "ok"/"INVALID"/"".
    Open-track instances are NOT solved -- their answer is the open question.
    """
    if track == "open":
        return "OPEN", 0.0, ""
    t0 = time.time()
    try:
        p = subprocess.run([CADICAL, cnf], capture_output=True, text=True, timeout=timeout)
    except subprocess.TimeoutExpired:
        return "TIMEOUT", float(timeout), ""
    dt = time.time() - t0
    if p.returncode == 10:        # SATISFIABLE -- capture + validate the model
        model = set()
        for line in p.stdout.splitlines():
            if line.startswith("v "):
                for tok in line.split()[1:]:
                    if tok != "0":
                        model.add(int(tok))
        return "SAT", dt, ("ok" if validate_model(cnf, model) else "INVALID")
    if p.returncode == 20:        # UNSATISFIABLE
        return "UNSAT", dt, ""
    return "ERR(%d)" % p.returncode, dt, ""


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--timeout", type=int, default=45, help="per-instance oracle cap (s)")
    # Open targets are huge (n=13/N=14 is ~110 MB) and are research inputs for the
    # Dagster workflow, not regression data -- so they are generate-on-demand:
    #   python3 generate_benchmarks.py --tracks open
    ap.add_argument("--tracks", default="known,hard", help="comma list of tracks to generate (known,hard,open)")
    ap.add_argument("--no-build", action="store_true", help="skip building the C generators")
    args = ap.parse_args()

    if not os.path.exists(CADICAL):
        sys.exit("standalone cadical oracle not found at %s\n(build it: make -C dagster/cadical_solver/cadical)" % CADICAL)
    want = set(args.tracks.split(","))
    os.makedirs(GEN_DIR, exist_ok=True)
    if not args.no_build:
        build_generators()

    rows = []
    print("\n%-20s %-11s %-6s %8s %9s  %-8s %-8s %8s" %
          ("name", "family", "track", "vars", "clauses", "verdict", "model", "secs"))
    print("-" * 86)
    for spec in SPECS:
        if spec["track"] not in want:
            continue
        family, params, track = spec["family"], spec["params"], spec["track"]
        try:
            name, cnf, dag = generate(family, params)
        except Exception as e:
            print("  (skip %s %s: %s)" % (family, params, e)); continue
        if not (os.path.exists(cnf) and os.path.exists(dag)):
            print("  (skip %s %s: generator produced no cnf/dag)" % (family, params)); continue
        nv, nc = cnf_dims(cnf)
        verdict, secs, model = label(cnf, track, args.timeout)
        flag = "  <-- MODEL INVALID (encoding bug?)" if model == "INVALID" else ""
        print("%-20s %-11s %-6s %8d %9d  %-8s %-8s %8.2f%s" %
              (name, family, track, nv, nc, verdict, model, secs, flag))
        rows.append(dict(family=family, name=name, params="x".join(map(str, params)),
                         track=track, nvars=nv, nclauses=nc, size=size_of(nv),
                         verdict=verdict, model=model, seconds=round(secs, 3),
                         cnf=os.path.relpath(cnf, REPO_ROOT), dag=os.path.relpath(dag, REPO_ROOT)))

    manifest = os.path.join(GEN_DIR, "manifest.tsv")
    cols = ["family", "name", "params", "track", "nvars", "nclauses", "size",
            "verdict", "model", "seconds", "cnf", "dag"]
    # merge with any rows from tracks we didn't regenerate this run
    existing = {}
    if os.path.exists(manifest):
        import csv
        with open(manifest) as f:
            for r in csv.DictReader(f, delimiter="\t"):
                existing[r["name"]] = r
    for r in rows:
        existing[r["name"]] = {c: str(r[c]) for c in cols}
    with open(manifest, "w") as f:
        f.write("\t".join(cols) + "\n")
        for r in existing.values():
            f.write("\t".join(r.get(c, "") for c in cols) + "\n")
    print("\nwrote %s (%d instances)" % (manifest, len(existing)))


if __name__ == "__main__":
    main()
