#!/usr/bin/env python3
"""cube -- the cube-generation stage of cube-and-conquer for Dagster.

Pipeline:  sanitize CNF  ->  (optional) symmetry breaking  ->  march_cu cubes

  1. SANITIZE -- strip a UTF-8 BOM, CR (CRLF), and drop comment lines. The real
     domatic CNFs are Windows-authored with CP1252 bytes in comments, which
     march's and BreakID's strict C parsers reject; sanitising fixes that.
  2. SYMMETRY BREAKING (BreakID, via dagmaker.symbreak) -- default 'full'.
     NOTE: for cube-and-conquer we want 'full' (maximise search-space reduction);
     the 'dag' level is for the DAG-decomposition path (it preserves separator
     parallelism, irrelevant when march does the splitting).
  3. CUBING (march_cu) -- adaptive, lookahead-pruned cubes. Bounded by a wall
     timeout and march's own cutoff knobs.

Outputs the cube file (march .icnf: `a <lits> 0` per cube) and the final CNF the
cubes refer to (the sanitized, possibly symmetry-broken formula) -- the two
inputs the conquer stage (Dagster) needs.

Examples:
  cube.py problem.cnf -o cubes.icnf
  cube.py domatic_8.cnf --symbreak full --breakid-timeout 60 --march-timeout 300
  cube.py problem.cnf --symbreak none --march-depth 12
"""

import argparse
import os
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(os.path.dirname(HERE))            # .../dagster (repo root)
MARCH = os.path.join(HERE, "march_cu", "march_cu")
sys.path.insert(0, os.path.join(REPO_ROOT, "utilities", "dag-generator"))


def sanitize(src, dst):
    """Strip BOM/CR and drop comment lines so strict C parsers accept the CNF.
    Keeps the 'p cnf' header and clause lines (which are pure ASCII)."""
    with open(src, "rb") as f:
        data = f.read()
    if data.startswith(b"\xef\xbb\xbf"):
        data = data[3:]
    kept = 0
    with open(dst, "w") as out:
        for raw in data.split(b"\n"):
            line = raw.rstrip(b"\r").strip()
            if not line:
                continue
            if line[:1] in (b"c", b"%"):          # comment lines (where the non-ASCII lives)
                continue
            out.write(line.decode("ascii", "ignore") + "\n")
            if not line.startswith(b"p cnf"):
                kept += 1
    return kept


def cnf_dims(path):
    with open(path, errors="replace") as f:
        for line in f:
            if line.startswith("p cnf"):
                p = line.split()
                return int(p[2]), int(p[3])
    return 0, 0


def run_march(cnf, cubes_out, timeout, depth=None, free_vars=None, cube_limit=None):
    """Run march_cu -> cubes_out. Returns (status, n_cubes, seconds).
    status in {cubes, solved-SAT, solved-UNSAT, TIMEOUT, ERR}."""
    cmd = [MARCH, cnf, "-o", cubes_out]
    if depth is not None:
        cmd += ["-d", str(depth)]
    if free_vars is not None:
        cmd += ["-n", str(free_vars)]
    if cube_limit is not None:
        cmd += ["-l", str(cube_limit)]
    if os.path.exists(cubes_out):
        os.remove(cubes_out)
    t0 = time.time()
    try:
        p = subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=timeout)
    except subprocess.TimeoutExpired:
        return ("TIMEOUT", 0, timeout)
    dt = time.time() - t0
    # march exit codes: 10 = solved SAT, 20 = solved UNSAT, else cubes emitted
    if p.returncode == 10:
        return ("solved-SAT", 0, dt)
    if p.returncode == 20:
        return ("solved-UNSAT", 0, dt)
    n = 0
    if os.path.exists(cubes_out):
        with open(cubes_out, errors="replace") as f:
            n = sum(1 for line in f if line.startswith("a "))
    if n == 0 and p.returncode != 0:
        return ("ERR(%d)" % p.returncode, 0, dt)
    return ("cubes", n, dt)


def autotune_depth(cnf, target, timeout, log):
    """Pick a march cut-depth so the cube count is about `target`. Cube count grows
    ~geometrically with depth, so probe increasing depths and stop once the count
    first reaches target. Returns (kind, depth): kind 'ok' with a depth, or
    'solved-SAT'/'solved-UNSAT' if march solves the formula during a probe."""
    probe = cnf + ".probe.icnf"
    last_d, last_n = None, 0
    for d in range(4, 41, 2):
        status, n, secs = run_march(cnf, probe, timeout, depth=d)
        if status.startswith("solved"):
            if os.path.exists(probe): os.remove(probe)
            return (status, None)
        if status == "TIMEOUT":
            log("  autotune: -d %d timed out (%.0fs); using -d %s (%d cubes)" % (d, timeout, last_d, last_n))
            break
        if status == "cubes":
            log("  autotune: -d %-2d -> %d cubes (%.0fs)" % (d, n, secs))
            last_d, last_n = d, n
            if n >= target:
                break
            # per-node lookahead is fixed per formula, so the next (deeper) probe
            # costs ~geometrically more. If this probe already ate a big chunk of
            # the budget, a deeper one will just time out -- stop now rather than
            # waste a whole --march-timeout on a doomed probe (the big-CNF trap).
            if secs > timeout / 3.0:
                log("  autotune: -d %d used %.0fs (>1/3 of budget); stopping before a doomed deeper probe" % (d, secs))
                break
    if os.path.exists(probe):
        os.remove(probe)
    return ("ok", last_d)


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("cnf")
    ap.add_argument("-o", "--out", default=None, help="cube file (default: <cnf>.icnf)")
    ap.add_argument("--final-cnf", default=None,
                    help="where to write the sanitized/broken CNF the cubes refer to "
                         "(default: <cnf>.cube.cnf)")
    ap.add_argument("--symbreak", choices=["none", "light", "full"], default="full",
                    help="symmetry breaking before cubing (default: full)")
    ap.add_argument("--breakid-timeout", type=int, default=120,
                    help="BreakID step budget in kilo-steps-ish via -t (default: 120)")
    ap.add_argument("--march-timeout", type=int, default=300,
                    help="wall-clock budget for march in seconds (default: 300)")
    ap.add_argument("--march-depth", type=int, default=None, help="march -d (cut depth)")
    ap.add_argument("--march-free-vars", type=int, default=None, help="march -n (free-var cutoff)")
    ap.add_argument("--cube-limit", type=int, default=None,
                    help="march -l (max cubes). NOTE: -l can hang on some formulas -- prefer --march-depth "
                         "(the --march-timeout still bounds it)")
    ap.add_argument("--target-cubes", type=int, default=None,
                    help="auto-tune the march cut-depth to produce ~this many cubes "
                         "(ignored if --march-depth/--march-free-vars set)")
    ap.add_argument("--quiet", action="store_true")
    args = ap.parse_args()

    if not os.path.exists(MARCH):
        sys.exit("march_cu not built at %s (cd utilities/cube/march_cu && make CFLAGS='-O3 -fcommon -w -DNDEBUG')" % MARCH)
    base = os.path.splitext(args.cnf)[0]
    cubes_out = args.out or base + ".icnf"
    final_cnf = args.final_cnf or base + ".cube.cnf"

    def log(m):
        if not args.quiet:
            print(m)

    # 1. sanitize
    sanitized = base + ".sanitized.cnf"
    kept = sanitize(args.cnf, sanitized)
    v0, c0 = cnf_dims(sanitized)
    log("cube: sanitized %s -> %d vars, %d clauses" % (args.cnf, v0, c0))

    # 2. symmetry breaking (full by default; the right mode for cube-and-conquer)
    cube_input = sanitized
    if args.symbreak != "none":
        from dagmaker import symbreak as sb
        if not sb.available():
            sys.exit("BreakID not built (see utilities/symbreak/README.md)")
        t0 = time.time()
        res = sb.run_symbreak(sanitized, final_cnf, level=args.symbreak,
                              time_kilo=args.breakid_timeout)
        log("cube: symmetry breaking (%s, %.1fs): %s"
            % (args.symbreak, time.time() - t0, res.summary()))
        cube_input = final_cnf
    else:
        # no breaking: the sanitized CNF is what the cubes refer to
        import shutil
        shutil.copyfile(sanitized, final_cnf)

    # 3. cubing (optionally auto-tune the cut depth to hit ~target-cubes)
    depth = args.march_depth
    if args.target_cubes and args.march_depth is None and args.march_free_vars is None:
        log("cube: auto-tuning march depth for ~%d cubes ..." % args.target_cubes)
        kind, depth = autotune_depth(cube_input, args.target_cubes, args.march_timeout, log)
        if kind.startswith("solved"):
            log("cube: march SOLVED the formula directly during auto-tune (%s)" % kind)
            print("STATUS %s  CUBES 0  SECONDS 0.00  FORMULA %s  DAG -" % (kind, final_cnf))
            sys.exit(0)
        log("cube: auto-tuned to --march-depth %s" % depth)
    status, n_cubes, secs = run_march(cube_input, cubes_out, args.march_timeout,
                                      depth=depth, free_vars=args.march_free_vars,
                                      cube_limit=args.cube_limit)
    vf, cf = cnf_dims(final_cnf)
    if status == "cubes":
        log("cube: march produced %d cubes in %.1fs" % (n_cubes, secs))
        log("  cubes -> %s" % cubes_out)
        log("  formula (cubes refer to this) -> %s  (%d vars, %d clauses)" % (final_cnf, vf, cf))
        # emit the single-node conquer DAG (the whole formula, reporting every var).
        # Derive its path from the cube file so cubes/formula/DAG share a base even
        # when -o / --final-cnf point elsewhere than the input CNF.
        conquer_dag = os.path.splitext(cubes_out)[0] + ".conquer.dag"
        rng = lambda a, b: str(a) if a == b else "%d-%d" % (a, b)
        with open(conquer_dag, "w") as f:
            f.write("DAG-FILE\nNODES:1\nGRAPH:\nCLAUSES:\n0:%s\nREPORTING:\n%s\n"
                    % (rng(0, cf - 1), rng(1, vf)))
        log("  conquer DAG -> %s" % conquer_dag)
        log("  conquer with: mpirun -n <ranks> dagster --backend cadical -e 0 --cubes %s %s %s"
            % (cubes_out, conquer_dag, final_cnf))
        log("  (add --share for clause sharing between conquer workers; needs 1 extra rank)")
    elif status.startswith("solved"):
        log("cube: march SOLVED the formula directly (%s) -- no cubing needed" % status)
    elif status == "TIMEOUT":
        log("cube: march timed out after %ds. march's per-node lookahead scales with formula\n"
            "      size, so on a BIG cnf cubing is a slow LINEAR grind (~ cubes x per-node-cost),\n"
            "      not 'almost done' -- a longer timeout buys proportionally more cubes, no more.\n"
            "      Options: raise --march-timeout for more cubes; set a fixed shallow --march-depth\n"
            "      (one fast pass); or for a many-VARIABLE cnf prefer DAG decomposition (dagmake.py)\n"
            "      over cube-and-conquer -- march's lookahead is the wrong tool there." % args.march_timeout)
        print("STATUS TIMEOUT  CUBES 0  SECONDS %.2f  FORMULA %s" % (secs, final_cnf))
        sys.exit(2)
    else:
        log("cube: march failed (%s)" % status)
        print("STATUS %s  CUBES 0  SECONDS %.2f  FORMULA %s" % (status, secs, final_cnf))
        sys.exit(2)
    # machine-readable status line (always printed, even under --quiet). FORMULA is
    # the CNF the cubes (or direct verdict) refer to; DAG the single-node conquer DAG.
    print("STATUS %s  CUBES %d  SECONDS %.2f  FORMULA %s  DAG %s"
          % (status, n_cubes, secs, final_cnf,
             os.path.splitext(cubes_out)[0] + ".conquer.dag" if status == "cubes" else "-"))


if __name__ == "__main__":
    main()
