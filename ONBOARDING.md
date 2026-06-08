# Dagster — project handoff & capability map

For a developer or LLM picking this up. Dagster is a **distributed (MPI) hybrid SAT
solver**: a Master hands work to Worker units; a **DAG** decomposes a CNF and the
edges carry partial assignments (vertex separators), so the pieces solve in
parallel. Cost is exponential in separator width, so when no small separator exists
the problem is attacked by **cube-and-conquer** instead. Research goal: solve hard
combinatorial problems on HPC — building toward open cases (the domatic number on
the n-dimensional Hamming cube; the open Ramsey relation algebras n=8, n=13).

- **New user, just solve a CNF →** `USAGE.md`
- **Run the HPC benchmarks →** `HPC_RUN.md`
- **This file →** what exists, where it lives, how to drive it, what's next.

---

## 1. What Dagster can do now

Everything below is built, wired, and validated.

**Solving core.** Master/Worker MPI engine over a DAG decomposition; cube-and-conquer
for expanders; checkpoint/resume (`-v`/`-z`/`-u`).

**Backends** (orthogonal `--backend` flag; legacy `-m 0..10` still maps):
- `tinisat` (the original `SatSolver`), `minisat`, `cadical` (default, top-tier),
  `cryptominisat` (native XOR/Gaussian).
- `glucose`, `lingeling`, and **any IPASIR solver** via a generic **dlopen adapter**
  (`--backend ipasir --ipasir-lib X.so`) — adding a solver is "build a `.so`", no
  recompile.

**Solver augmentations** (compose via flags):
- `--sls` — gNovelty+ stochastic-local-search helper processes guide the CDCL.
- `--strengthen` — clause-strengthening reducer (tinisat only).
- `--inprocess off|light|default|heavy` — backend-native inprocessing.
- `--share` — **clause sharing** between cube-and-conquer workers (a clause hub
  relays learned clauses; CaDiCaL). Sound because CDCL learned clauses are
  formula-entailed independent of the cube.

**Pipeline tools** (`utilities/`):
- `solve.py` — turnkey front-end: sanitize → preprocess → symmetry → route
  (decompose vs cube) → build → run. **The front door.**
- `dag-generator/dagmake.py` (dagmaker) — structure-aware DAG generation.
- `cube/cube.py` + vendored `march_cu` — cube-and-conquer cube generation
  (+ `--symbreak`).
- `symbreak/` — BreakID symmetry breaking; **dag-aware** (`--symbreak dag` keeps
  only node-local breaking clauses to preserve a good DAG).

**UNSAT proofs** (`utilities/proof/` — turns verdicts into checked theorems):
- `--proof` emits per-worker DRAT (CaDiCaL); `drat-trim` (vendored) checks.
- `certify.py` (single-formula), `tautology.py` (cube-split exhaustiveness),
  `cc_certify.py` (full cube-and-conquer certificate). SAT is self-certifying (model).

**Benchmark corpus** (`Benchmarks/`):
- `generate_benchmarks.py` builds CNFs from the bundled generators (costas, ramsey,
  determinant) into `generated/`, **labelled by an independent oracle** (standalone
  CaDiCaL, not Dagster) with SAT-model validation. Three tracks: `known`
  (regression), `hard` (scaling), `open` (the n=8/n=13 Ramsey research targets).

**HPC test harnesses** (`dagster/tests/`), each emits a SLURM array + has a
`collect.py`:
- `backend_matrix` — backend correctness (verdict parity) + a backend scoreboard.
- `cube_matrix` — plain-vs-`--share` speedup (the clause-sharing measurement).
- `symbreak_matrix` — symmetry-breaking soundness across levels + speedup.

---

## 2. Where it lives

```
dagster/                     the C++ MPI solver
  main.cpp                   CLI dispatch + topology (simple/sls/share/strengthen execute)
  Master.cpp Worker.cpp      the engine; Worker picks the backend + augmentations
  SatSolver.*                tinisat backend; SatSolverInterface = the backend contract (~IPASIR)
  cadical_solver/  cryptominisat_solver/  minisat_solver/   backend wrappers (+ vendored solvers)
  ipasir_solver/             generic dlopen IPASIR adapter + glucose/lingeling glue + build scripts
  clause_share/              ClauseChannel + ClauseHub (the --share clause hub)
  SlsChannel.*               reusable gNovelty+ SLS transport
  Arguments.*                CLI flags
  tests/{backend,cube,symbreak}_matrix/   HPC harnesses (matrix.py + collect.py + README)
utilities/
  solve.py                   turnkey front-end (THE entry point)
  dag-generator/             dagmaker (dagmake.py + DAGMAKER.md)
  cube/                      cube.py, march_cu/, README, CLAUSE_SHARING_SCOPE.md, PROOF_SCOPE.md
  proof/                     certify.py, tautology.py, cc_certify.py, drat-trim.c, README
  symbreak/                  BreakID + symbreak.py
Benchmarks/
  generate_benchmarks.py     the corpus driver; generated/ holds the corpus + manifest.tsv
  costas/ ramsey/ determinant/ Pentomino/ gensat_sat/   problem generators
README.md                    original docs (build, options, DAG format, glossary)
USAGE.md  HPC_RUN.md  ONBOARDING.md(this)
```

---

## 3. How to drive it (by task)

| goal | command |
|---|---|
| build | `cd dagster && make` |
| solve a CNF | `python3 utilities/solve.py problem.cnf --run` |
| force cube + sharing | `python3 utilities/solve.py problem.cnf --route cube --share --run` |
| raw solve | `mpirun -n N dagster --backend cadical -e 0 dag cnf -o out` |
| make a DAG | `python3 utilities/dag-generator/dagmake.py --nodes N cnf dag` |
| make cubes | `python3 utilities/cube/cube.py cnf -o c.icnf --final-cnf f.cnf --target-cubes K` |
| certify UNSAT | `python3 utilities/proof/certify.py cnf` / `cc_certify.py formula.cnf cubes.icnf` |
| generate test data | `cd Benchmarks && python3 generate_benchmarks.py` |
| benchmark on HPC | see `HPC_RUN.md` (emit + sbatch + collect.py) |
| add a backend | build `libipasirX.so`, `--backend ipasir --ipasir-lib X.so` (see `dagster/ipasir_solver/README.md`) |

CLI reference + the new-user flow: `USAGE.md`.

---

## 4. State & roadmap

**Done (this project):** the founding goal — HPC-usable hard-SAT solver — is met,
plus: 6 backends + IPASIR adapter; SLS/strengthen/inprocess; cube-and-conquer;
clause sharing (phase 1); dag-aware symmetry breaking; turnkey `solve.py`;
independently-labelled benchmark corpus; 3 HPC harnesses + collectors; UNSAT proofs
(single-formula + full cube-and-conquer); flag-consistent CLI.

**Immediate next:** run the HPC benchmarks (`HPC_RUN.md`) — staged, not yet run on
real cores. The clause-sharing speedup geomean and the backend scoreboard are the
outstanding empirical questions.

**Research frontier (the point):**
- **Open Ramsey n=8 / n=13** — representability is open (cyclic constructions proven
  absent; general existence unknown). Staged: `Benchmarks/generate_benchmarks.py
  --tracks open`, then `solve.py --share`; UNSAT bounds certifiable via `cc_certify.py`.
  A SAT result = a new non-cyclic representation.
- **Domatic n=10** — routes to cube today, but its symmetry is *semantic* (BreakID
  finds none syntactically); needs domatic-specific / semantic symmetry breaking.

**Optional / Phase-B (all scoped, none blocking):** clause-sharing phase 2
(during-solve import via ExternalPropagator, LBD filtering); a single *monolithic*
DRAT (needs CaDiCaL assumption-mode proof); certified symmetry breaking (PR/SR —
until then, prove the *unbroken* formula); DAG-decomposition proofs; pause/resume
with changed parameters; more IPASIR backends (Maple, PicoSAT).

Design docs with full reasoning: `utilities/cube/CLAUSE_SHARING_SCOPE.md`,
`utilities/cube/PROOF_SCOPE.md`.

---

## 5. Gotchas (read before debugging)

- **`-v` is checkpoint frequency, NOT log verbosity.** For solver logs:
  `GLOG_v=N` env + pass `-x GLOG_logtostderr=1` to `mpirun` (non-master ranks log to
  stderr). This burned hours once.
- **MPI slots = physical cores.** Use `mpirun --oversubscribe` for more ranks than
  that (laptops). The harnesses already do.
- **Cubes:** `--cubes ICNF` then `DAG CNF` as positionals — passing the icnf as a
  third positional gives "too many files" + a non-zero exit.
- **Verdict reading:** empty `-o` file + clean exit = UNSAT; non-empty = SAT; non-zero
  exit = ERROR/TIMEOUT (never silently "UNSAT" — that trap caused false results).
- **dagster `-o` is a PROJECTED solution** (reporting-set vars), not a full model;
  don't validate it as a full assignment (use the solver's full model, e.g. via
  `certify.py`).
- **Proof mode** (`--proof`): CaDiCaL only, `-e 0`, no `--share`/`--sls`, and **no
  symmetry breaking** (lex-leader clauses aren't DRAT-justifiable — prove the
  unbroken formula).
- **`--backend glucose/lingeling`** default to a `.so` path relative to the working
  dir; run from `dagster/` or pass an absolute `--ipasir-lib`. Build the `.so` first
  (`ipasir_solver/build_*.sh`).
- **Header/ABI changes** in the C++ need a clean rebuild; `-MMD -MP` tracks deps but
  when in doubt `make clean && make`.

---

## 6. Conventions

- The orthogonal flags (`--backend/--sls/--strengthen/--share/--inprocess`) are the
  interface; the numeric `-m 0..10` is legacy back-compat. All maintained tooling
  emits flags.
- `SatSolverInterface` is essentially IPASIR — that's why the IPASIR adapter is thin.
- Generated artifacts (corpus CNFs, proofs, built `.so`s) are `.gitignore`d and
  regenerable; their generators/manifests are tracked.
- The benchmark corpus is labelled by an **independent** oracle, not Dagster — so it
  can actually catch a Dagster bug.
