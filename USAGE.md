# Using Dagster — solve a CNF

Dagster is a distributed (MPI) SAT solver. You give it a CNF; it decomposes the
problem into a **DAG** and solves the pieces in parallel across many cores, or
splits it into **cubes** (cube-and-conquer) when there's no good decomposition.

The fast path is the turnkey front-end. The manual pipeline is below it.

## 0. Build (once)

```sh
cd dagster && make            # builds ./dagster (the MPI binary)
```
Optional extras (only if you need them):
```sh
make -C cadical_solver/cadical                 # DRAT checker / proofs use cadical's tools
gcc -O2 -o ../utilities/proof/drat-trim ../utilities/proof/drat-trim.c -lm   # proof checker
bash ipasir_solver/build_glucose.sh            # --backend glucose
bash ipasir_solver/build_lingeling.sh          # --backend lingeling
```
A Python venv at `.venv/` (repo root) is used by the utilities (dagmaker, cube.py,
determinant, the harnesses).

## 1. Turnkey: hand it a CNF

```sh
python3 utilities/solve.py problem.cnf            # decide + print the plan
python3 utilities/solve.py problem.cnf --run      # ... and execute it
python3 utilities/solve.py problem.cnf --run --verbose   # show the reasoning
```

`solve.py` runs the whole pipeline and picks the strategy, printing what it decided
so you can override any stage:

```
raw CNF
  1. SANITIZE    strip BOM/CRLF, drop comment lines
  2. PREPROCESS  unit propagation + pure-literal elimination
  3. SYMMETRY    BreakID (budgeted) — detect the symmetry group
  4. ROUTE       small separator  -> DAG DECOMPOSITION (exploit structure)
                 large separator  -> CUBE-AND-CONQUER  (march cubes)
  5. BUILD+EMIT  the artifacts + the ready-to-run `mpirun … dagster …` command
```

Useful overrides:
```sh
--cores N                      cores to target (default: this box)
--route {auto,decompose,cube}  force a strategy
--backend B                    tinisat|minisat|cadical|cryptominisat|glucose|lingeling  (default cadical)
--symbreak {auto,none,light,full,dag}
--share                        cube route only: clause sharing between workers (helps hard UNSAT)
--march-depth D / --target-cubes N    cube route tuning
```

Result: a `SAT` solution file, or `UNSAT`. (`solve.py` prints the verdict.)

## 2. Manual pipeline (when you want control)

**Decomposition route** — generate a DAG, then solve:
```sh
python3 utilities/dag-generator/dagmake.py --nodes N problem.cnf problem.dag
mpirun -n <N+1> dagster --backend cadical -e 0 problem.dag problem.cnf -o out.sols
```

**Cube-and-conquer route** — cube the formula, then conquer:
```sh
python3 utilities/cube/cube.py problem.cnf -o cubes.icnf --final-cnf formula.cube.cnf --target-cubes 512
mpirun -n <ranks> dagster --backend cadical -e 0 --cubes cubes.icnf <conquer.dag> formula.cube.cnf -o out.sols
# add clause sharing (one extra rank becomes a hub):
mpirun -n <ranks> dagster --backend cadical --share -e 0 --cubes cubes.icnf <conquer.dag> formula.cube.cnf -o out.sols
```

Verdict convention: an empty `-o` file (clean exit) = UNSAT; a non-empty file = SAT
(the solution). A non-zero exit = error/timeout, never "UNSAT".

## 3. Certify an UNSAT result (optional, rigorous)

```sh
python3 utilities/proof/certify.py problem.cnf                 # single-formula: UNSAT VERIFIED | SAT
python3 utilities/proof/cc_certify.py formula.cnf cubes.icnf   # cube-and-conquer: full certificate
```
SAT needs no proof (the model is the certificate). See `utilities/proof/README.md`.

## Key flags (dagster binary)

| flag | meaning |
|---|---|
| `--backend B` | solver: tinisat / minisat / cadical / cryptominisat / glucose / lingeling / ipasir |
| `--ipasir-lib SO` | with `--backend ipasir`: dlopen any IPASIR solver `.so` |
| `--sls` | add gNovelty+ SLS helper processes (needs ≥3 ranks) |
| `--strengthen` | clause-strengthening reducer (tinisat only) |
| `--inprocess L` | backend inprocessing: off / light / default / heavy |
| `--share` | clause sharing between cube-and-conquer workers (cadical) |
| `--cubes FILE` | seed the conquer node with march cubes |
| `--proof FILE` | emit a DRAT UNSAT proof per worker (cadical; single-node) |
| `-e 0/1` | stop at first solution / enumerate all |
| `-v`, `-z`, `-u` | checkpoint frequency / prefix / resume (note: `-v` is NOT log verbosity) |

(The legacy numeric `-m 0..10` selector still works; the flags above are preferred.)

## Gotchas

- **Logging:** dagster's `-v` is *checkpoint frequency*, not verbosity. For solver
  logs use `GLOG_v=N` and pass `-x GLOG_logtostderr=1` to `mpirun` (so non-master
  ranks log to stderr).
- **MPI slots:** MPI counts *physical* cores; pass `--oversubscribe` to `mpirun`
  to use more ranks than that (e.g. on a laptop).
- **Cubes:** the cube file goes via `--cubes ICNF`, with `DAG CNF` as positionals —
  not three positionals.
- **`--backend glucose/lingeling`** default to a `.so` path relative to the working
  dir; run from `dagster/` or pass an absolute `--ipasir-lib`.

For the full design + capability map see `ONBOARDING.md`; for HPC benchmarking see
`HPC_RUN.md`.
