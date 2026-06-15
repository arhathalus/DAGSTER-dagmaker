# Cube-and-conquer for Dagster

Cube-and-conquer splits a hard formula into many partial assignments ("cubes")
with a lookahead solver, then solves the formula under each cube independently
(the "conquer" phase) in parallel. This is how Pythagorean-triples / Schur-5 /
Keller were proved — it works on expander-like problems where pure decomposition
(a fixed separator) explodes.

In Dagster the two phases are:

```
cube.py CNF                                  →  cubes.icnf  +  formula  +  conquer DAG
  sanitize → symmetry break → march_cu

dagster --cubes cubes.icnf  conquer.dag  formula     (the conquer phase)
  master seeds one message per cube into a single conquer node;
  workers (e.g. cadical, --backend cadical) solve formula+cube in parallel.
```

The conquer phase rides Dagster's existing master/worker machinery, so it gets
**dynamic load balancing** (idle workers pull the next cube — cubes vary wildly
in hardness) and **checkpoint/resume** for free.

## Components

- `march_cu/` — vendored lookahead cuber (from github.com/marijnheule/CnC).
  Build: `cd march_cu && make CFLAGS='-O3 -fcommon -w -DNDEBUG'` (the `-fcommon` is needed
  for the old C globals on modern GCC). Self-contained, no deps.
- `cube.py` — the cube-generation driver: sanitize → symmetry break → march.

## cube.py

```sh
cube.py problem.cnf                                  # full symbreak + march defaults
cube.py problem.cnf --symbreak none --march-depth 10 # hard instance: shallow cutoff
cube.py domatic_8.cnf --symbreak full --breakid-timeout 60 --march-timeout 300
```

It writes `<cnf>.icnf` (cubes), `<cnf>.cube.cnf` (the formula the cubes refer to
— sanitized, possibly symmetry-broken), and `<cnf>.conquer.dag` (a single-node
DAG over that formula), then prints the conquer command.

Key options / behaviours:
- **Sanitize (always)** — strips CR (CRLF) and drops comment lines. The real
  domatic CNFs are Windows-authored with CP1252 bytes in comments, which march's
  and BreakID's strict C parsers reject; this fixes that.
- **`--symbreak {none,light,full}`** (default `full`) — symmetry breaking before
  cubing. NOTE: use `full`, not the DAG-decomposition path's `dag` level (which
  optimises separator parallelism, irrelevant when march does the splitting).
- **`--march-depth D` / `--march-free-vars N`** — cube cutoff. march's *default*
  cutoff splits until subproblems are easy, which **times out on hard instances**;
  a shallow `-d` emits cubes fast and pushes the work into the conquer phase
  (where it belongs). e.g. domatic_8 (9722 vars): `-d 6`→59 cubes, `-d 10`→580,
  `-d 14`→6244.
- **`--breakid-timeout` / `--march-timeout`** — budgets (BreakID `-t` steps; march
  wall-clock). BreakID without a budget can spin a long time on big instances.

## When does which mode help?

- **Locally-symmetric problems** (e.g. pigeonhole): `--symbreak full` collapses the
  cube count by orders of magnitude (pigeonhole10: 142363 → 12 cubes). Often the
  breaking nearly solves it outright.
- **Hard, syntactically-asymmetric problems** (e.g. domatic — its Hamming-cube
  symmetry is *semantic*, not visible to BreakID, which finds 0 generators): use
  `--symbreak none --march-depth D` and let cube-and-conquer + the parallel
  conquer carry it. Cube counts stay HPC-manageable (tens–thousands).

## Conquer (`dagster --cubes`)

```sh
mpirun -n <ranks> dagster --backend cadical -e 0 --cubes cubes.icnf conquer.dag formula.cube.cnf
```

`--backend cadical` is the recommended backend; `-e 0` races to the first SAT cube (drop
for full enumeration). The master seeds one message per cube into node 0 (the
whole formula) and distributes them to the `ranks-1` workers. Verdict: SAT if any
cube is SAT (with a model), UNSAT if all cubes are UNSAT. Verified verdict-correct
on SAT and UNSAT instances.

## Clause sharing (`--share`)

By default each conquer worker learns in isolation. **Clause sharing** dedicates
one extra rank as a *clause hub* that collects the learned (conflict) clauses every
worker discovers, dedupes them, and rebroadcasts them to the other workers — so a
lemma found while solving one cube can prune another worker's search.

```sh
# master + N conquer workers + 1 hub  (needs >= 3 ranks total)
mpirun -n <ranks> dagster --backend cadical --share -e 0 --cubes cubes.icnf conquer.dag formula.cube.cnf
```

- **CaDiCaL-only** (uses CaDiCaL's learned-clause callback). Does not yet compose
  with `--sls`/`--strengthen`.
- **Sound.** CDCL learned clauses are entailed by the formula *independent of the
  cube* (the cube is applied as assumption decisions, never resolved into a learned
  clause), so any worker solving the same formula may use them. Only clauses over
  the original variables, of size 3..K, are shared (`--share-max-size K`, default 8;
  3 is the transport minimum). See `CLAUSE_SHARING_SCOPE.md` for the full argument.
- **When it helps:** hard **UNSAT** on **many cores**, where cubes share structure
  and one worker's lemmas save another's work. On easy/SAT-race instances it is
  roughly neutral (the toy benchmarks finish in <1s, so the hub overhead cancels
  any gain) — measure on the real target, not toys.
- **Turnkey:** `solve.py … --route cube --share` adds `--share` and reserves the hub
  rank for you.
- **Benchmark it:** `dagster/tests/cube_matrix/matrix.py --modes 5,10` runs plain
  vs shared head-to-head (the speedup column reads "share vs plain"); add
  `--profile hpc --emit-hpc DIR` for a SLURM array on big core counts.
