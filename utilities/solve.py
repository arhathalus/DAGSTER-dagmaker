#!/usr/bin/env python3
"""solve.py -- turnkey front-end: hand it a CNF, it prepares and dispatches to Dagster.

Given a CNF whose structure we don't know in advance, this runs the full pipeline
and picks the right strategy automatically, while printing exactly what it decided
and why -- so a human can override any stage.

    raw CNF
      │ 1. SANITIZE     strip BOM/CRLF, drop comment lines (real files need it)
      │ 2. PREPROCESS   unit propagation + pure-literal elimination (dagmaker)
      │ 3. SYMMETRY     BreakID (budgeted): detect the symmetry group
      │ 4. ROUTE        dagmaker estimates the best separator width:
      │                   small  → DAG DECOMPOSITION  (structure exists; exploit it)
      │                   large  → CUBE-AND-CONQUER   (expander; march cubes)
      ▼ 5. BUILD + EMIT the chosen artifacts and the ready-to-run dagster command
                       (add --run to execute it here)

WHY a router: a Dagster cutset DAG is cube-and-conquer with a *fixed* separator --
great when a small separator exists, hopeless on expanders (2^width explodes).
march's lookahead cubing handles expanders. The separator width dagmaker finds is
the signal that distinguishes the two regimes.

Every stage is overridable: --no-preprocess, --symbreak {auto,none,light,full,dag},
--route {auto,decompose,cube}, --decompose-sep-threshold N, --march-depth D,
--backend B, --cores N. Run with --verbose to see the numbers behind each decision.

Examples:
  solve.py problem.cnf                       # auto everything, prints the plan
  solve.py problem.cnf --run                 # ... and execute it
  solve.py domatic_8.cnf --route cube --march-depth 10 --cores 64
  solve.py easy.cnf --route decompose
"""

import argparse
import os
import re
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(HERE)
DAGSTER_BIN = os.path.join(REPO_ROOT, "dagster", "dagster")
VENV_PY = sys.executable
DAGMAKE = os.path.join(HERE, "dag-generator", "dagmake.py")
CUBE_PY = os.path.join(HERE, "cube", "cube.py")
sys.path.insert(0, os.path.join(HERE, "dag-generator"))

# Dagster is driven with the orthogonal flag interface (--backend/--sls/--share),
# not the legacy numeric -m selector.
BACKENDS = ["tinisat", "minisat", "cadical", "cryptominisat"]


def sanitize(src, dst):
    """Strip BOM/CR and drop comment lines (strict C parsers reject CP1252 comments)."""
    with open(src, "rb") as f:
        data = f.read()
    if data.startswith(b"\xef\xbb\xbf"):
        data = data[3:]
    with open(dst, "w") as out:
        for raw in data.split(b"\n"):
            line = raw.rstrip(b"\r").strip()
            if not line or line[:1] in (b"c", b"%"):
                continue
            out.write(line.decode("ascii", "ignore") + "\n")


def cnf_dims(path):
    with open(path, errors="replace") as f:
        for line in f:
            if line.startswith("p cnf"):
                p = line.split()
                return int(p[2]), int(p[3])
    return 0, 0


def run(cmd, timeout=None):
    return subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("cnf")
    ap.add_argument("--cores", type=int, default=os.cpu_count(),
                    help="cores available (drives ranks + cube count; default: this box)")
    ap.add_argument("--no-preprocess", action="store_true", help="skip BCP/PLE")
    ap.add_argument("--symbreak", choices=["auto", "none", "light", "full", "dag"], default="auto",
                    help="auto = break if BreakID finds symmetry (dag-level for decompose, full for cube)")
    ap.add_argument("--route", choices=["auto", "decompose", "cube"], default="auto")
    ap.add_argument("--decompose-sep-threshold", type=int, default=24,
                    help="route to DAG decomposition if dagmaker's best separator <= this (default 24)")
    ap.add_argument("--probe-timeout", type=int, default=60,
                    help="seconds for the dagmaker routing probe; on timeout, route to CUBE (default 60)")
    ap.add_argument("--march-depth", type=int, default=None, help="cube cutoff (cube route); overrides auto-tune")
    ap.add_argument("--target-cubes", type=int, default=None,
                    help="cube route: auto-tune depth to ~this many cubes (default 8 x cores)")
    ap.add_argument("--backend", choices=BACKENDS, default="cadical")
    ap.add_argument("--share", action="store_true",
                    help="cube route + cadical only: dedicate one rank as a clause hub that "
                         "relays learned clauses between conquer workers (helps hard UNSAT)")
    ap.add_argument("--breakid-timeout", type=int, default=120)
    ap.add_argument("--workdir", default=None, help="where to put artifacts (default: alongside CNF)")
    ap.add_argument("--run", action="store_true", help="execute the emitted dagster command")
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()

    if not os.path.exists(args.cnf):
        sys.exit("no such CNF: %s" % args.cnf)
    workdir = args.workdir or os.path.dirname(os.path.abspath(args.cnf)) or "."
    base = os.path.join(workdir, os.path.splitext(os.path.basename(args.cnf))[0])
    print("solve.py: %s   (cores=%d, backend=%s)" % (args.cnf, args.cores, args.backend))

    # ---- 1. sanitize ----------------------------------------------------
    clean = base + ".solve.cnf"
    sanitize(args.cnf, clean)
    v, c = cnf_dims(clean)
    print("[1] sanitize        -> %d vars, %d clauses" % (v, c))

    # ---- 2. preprocess (BCP/PLE) ---------------------------------------
    cnf = clean
    if not args.no_preprocess:
        from dagmaker import preprocess as pp
        clauses, mx = pp.read_dimacs(clean)
        s = pp.simplify(clauses, mx)
        if not s.sat:
            print("[2] preprocess      -> UNSAT (conflict during BCP/PLE). Done.")
            sys.exit(0)
        cnf = base + ".pp.cnf"
        pp.write_dimacs(cnf, s.clauses, s.max_var, s.trail)
        v, c = cnf_dims(cnf)
        print("[2] preprocess      -> %d clauses, %d unit(s) fixed  (%s)"
              % (s.n_clauses_after, len(s.trail), cnf))
    else:
        print("[2] preprocess      -> skipped")

    # ---- 3. symmetry detection (BreakID, budgeted) ---------------------
    n_gens = 0
    if args.symbreak != "none":
        from dagmaker import symbreak as sb
        if sb.available():
            tmp = base + ".symprobe.cnf"
            try:
                res = sb.run_symbreak(cnf, tmp, level="full", time_kilo=args.breakid_timeout)
                n_gens = res.num_generators
            except Exception as e:
                print("[3] symmetry        -> BreakID error (%s); proceeding without" % str(e)[:60])
            if os.path.exists(tmp):
                os.remove(tmp)
            print("[3] symmetry        -> %s"
                  % ("%d generators found" % n_gens if n_gens else "none detected (syntactic)"))
        else:
            print("[3] symmetry        -> BreakID not built; skipping (see utilities/symbreak/README.md)")
    else:
        print("[3] symmetry        -> disabled (--symbreak none)")

    # ---- 4. route on structure -----------------------------------------
    route = args.route
    sep, pw, nodes = None, None, None
    if route == "auto":
        # ask dagmaker for the best achievable separator on the (preprocessed) CNF.
        # Budget it: on a large/hard instance the probe is slow, and a slow/failed
        # probe is itself a signal that no quick decomposition exists -> route CUBE.
        probe_dag = base + ".probe.dag"
        try:
            p = run([VENV_PY, DAGMAKE, "--nodes", str(max(2, args.cores)), cnf, probe_dag],
                    timeout=args.probe_timeout)
        except subprocess.TimeoutExpired:
            print("[4] route           -> dagmaker probe exceeded %ds (no quick decomposition) => CUBE"
                  % args.probe_timeout)
            route, p = "cube", None
        if p is not None:
            star = next((l for l in p.stdout.splitlines() if l.lstrip().startswith("*")), "")
            m = re.search(r"max_sep=(\d+)", star); sep = int(m.group(1)) if m else None
            m = re.search(r"parallel_width=(\d+)", star); pw = int(m.group(1)) if m else None
            m = re.search(r"nodes=(\d+)", star); nodes = int(m.group(1)) if m else None
            if sep is None:
                print("[4] route           -> dagmaker probe inconclusive; defaulting to CUBE")
                route = "cube"
            else:
                decompose = (sep <= args.decompose_sep_threshold) and (nodes or 1) > 1
                route = "decompose" if decompose else "cube"
                print("[4] route           -> dagmaker best: separator=%d, parallel_width=%s, nodes=%s"
                      % (sep, pw, nodes))
                print("                       separator %s threshold %d  =>  %s"
                      % ("<=" if decompose else ">", args.decompose_sep_threshold, route.upper()))
    else:
        print("[4] route           -> forced: %s" % route.upper())

    # ---- 5. build the chosen pipeline + emit ---------------------------
    ranks = max(2, args.cores)
    if route == "decompose":
        sbreak = "dag" if (args.symbreak == "auto" and n_gens > 0) else \
                 (args.symbreak if args.symbreak in ("none", "light", "full", "dag") else "none")
        dag = base + ".dag"
        cmd = [VENV_PY, DAGMAKE, "--nodes", str(ranks), "--symbreak", sbreak, cnf, dag]
        print("[5] build DECOMPOSE -> dagmake --symbreak %s ..." % sbreak)
        p = run(cmd, timeout=600)
        if args.verbose:
            print(indent(p.stdout))
        if p.returncode != 0 or not os.path.exists(dag):
            sys.exit("dagmake failed:\n" + p.stdout[-400:] + p.stderr[-400:])
        # the DAG references the (possibly symmetry-broken) CNF dagmake wrote
        final_cnf = base + ".symbroken.cnf" if sbreak != "none" and os.path.exists(base + ".symbroken.cnf") else cnf
        dcmd = "mpirun -n %d %s --backend %s -e 0 %s %s" % (ranks, DAGSTER_BIN, args.backend, dag, final_cnf)
    else:  # cube
        sbreak = "full" if (args.symbreak == "auto" and n_gens > 0) else \
                 (args.symbreak if args.symbreak in ("none", "light", "full") else "none")
        cubes = base + ".icnf"
        formula = base + ".cube.cnf"
        cmd = [VENV_PY, CUBE_PY, cnf, "-o", cubes, "--final-cnf", formula, "--symbreak", sbreak]
        target = args.target_cubes if args.target_cubes else 8 * args.cores  # ~8 cubes/core for load balance
        if args.march_depth is not None:
            cmd += ["--march-depth", str(args.march_depth)]
            tune = "--march-depth %d" % args.march_depth
        else:
            cmd += ["--target-cubes", str(target)]
            tune = "--target-cubes %d (~8/core)" % target
        print("[5] build CUBE      -> cube.py --symbreak %s %s ..." % (sbreak, tune))
        p = run(cmd, timeout=900)
        if args.verbose:
            print(indent(p.stdout))
        m = re.search(r"STATUS (\S+)\s+CUBES (\d+).*?DAG (\S+)", p.stdout)
        if not m:
            sys.exit("cube.py failed:\n" + p.stdout[-400:] + p.stderr[-400:])
        status, ncubes, conquer_dag = m.group(1), int(m.group(2)), m.group(3)
        if status.startswith("solved"):
            print("                       march SOLVED it directly: %s -- nothing to dispatch." % status)
            sys.exit(0)
        if status != "cubes":
            sys.exit("cube.py status %s (try --march-depth for a shallower cutoff)" % status)
        print("                       %d cubes -> %s" % (ncubes, cubes))
        # Clause sharing (--share): one extra rank becomes a hub relaying learned
        # clauses between conquer workers. CaDiCaL-only; needs >= 3 ranks total
        # (master + worker + hub). The hub takes a rank, so worker count is ranks-2.
        if args.share:
            if args.backend != "cadical":
                sys.exit("--share requires --backend cadical")
            share_ranks = max(3, ranks)
            print("                       clause sharing ON -> --share, hub on 1 rank, "
                  "%d conquer workers" % (share_ranks - 2))
            dcmd = "mpirun -n %d %s --backend cadical --share -e 0 --cubes %s %s %s" % (share_ranks, DAGSTER_BIN, cubes, conquer_dag, formula)
        else:
            dcmd = "mpirun -n %d %s --backend %s -e 0 --cubes %s %s %s" % (ranks, DAGSTER_BIN, args.backend, cubes, conquer_dag, formula)

    print("\nPLAN (%s, backend=%s, %d ranks):\n  %s" % (route, args.backend, ranks, dcmd))
    print("\noverride any stage: --no-preprocess  --symbreak {none,light,full,dag}  "
          "--route {decompose,cube}  --march-depth D  --backend B  --cores N  --share")
    if args.share and route != "cube":
        print("NOTE: --share applies only to the cube route; ignored for %s." % route)

    if args.run:
        print("\n[run] executing ...")
        env = dict(os.environ)
        env["LD_LIBRARY_PATH"] = "/usr/local/lib:" + env.get("LD_LIBRARY_PATH", "")
        env["OMPI_MCA_btl"] = "self,tcp"
        out = base + ".sol.txt"
        # local convenience: allow more ranks than physical slots (laptops report
        # logical cores). The printed PLAN stays portable -- on HPC submit it as
        # shown (one rank per allocated core), no --oversubscribe needed.
        run_argv = dcmd.split()
        run_argv = run_argv[:1] + ["--oversubscribe"] + run_argv[1:]
        rc = subprocess.call(run_argv + ["-o", out], env=env)
        verdict = "SAT" if (os.path.exists(out) and os.path.getsize(out) > 0) else \
                  ("UNSAT" if rc == 0 else "ERR(%d)" % rc)
        print("[run] verdict: %s   (solution: %s)" % (verdict, out if verdict == "SAT" else "-"))


def indent(s):
    return "\n".join("    " + l for l in s.splitlines())


if __name__ == "__main__":
    main()
