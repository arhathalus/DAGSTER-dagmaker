# Session Context — onboarding for a fresh LLM

Read this to pick up where the previous session left off. It captures the project,
the artifacts, the key decisions/findings, how to run things, gotchas, and open work.

## The project in one paragraph

**Dagster** (`/home/tim/dagster/dagster/`) is a C++/MPI **distributed SAT solver**:
a Master process orchestrates Worker solving units (CDCL, optionally SLS-assisted)
that each solve a *subproblem* of a CNF defined by a **DAG file**. The DAG splits
the CNF's clauses across nodes; edges carry variable assignments (a vertex
separator) as constraints. **Dagster's cost is exponential in the separator width;
joins (multi-parent nodes) take a cross-product; depth amplifies work.** The user's
goal: use Dagster on an HPC to solve SAT problems a single solver can't, while
keeping it easy to use. This session built **dagmaker** (a DAG generator), fixed
Dagster bugs, and explored two problem families.

## The DAG file format (authoritative parser: `dagster/Dag.cpp`)

```
DAG-FILE
NODES:<n>
GRAPH:
<from>-><to>:<vars>     # variables passed parent→child (1-based), range notation "a-b,c"
CLAUSES:
<node>:<clause indices> # 0-based clause indices into the CNF
REPORTING:
<vars>                  # 1-based output variables; MUST be non-empty (parser rejects empty)
```
Clauses are indexed in CNF file order (matches `dagster/Cnf.cpp`). A clause range
`a-a` is illegal (use bare `a`). Clause OVERLAP across nodes is allowed.

## Key artifacts created this session

### dagmaker (the DAG generator) — `utilities/dag-generator/`
- `dagmaker/` package: `cnf.py`, `intervals.py`, `dagmodel.py`, `graph.py`,
  `components.py`, `scorer.py`, `assemble.py` (only place edge var-sets computed),
  `validate.py` (overlap-aware running-intersection), `preprocess.py` (BCP+PLE),
  `advisor.py` (recommends dagster flags), `pipeline.py` (multi-backend scoring,
  `DEFAULT_BACKENDS`).
- `dagmaker/decompose/`: `single.py`, `elimination.py` (min-degree + `frontier_chunk`,
  order-agnostic), `ordering.py` (bfs/rcm/spectral), `biconnected.py` (Tarjan),
  `community.py` (Louvain/label-prop), `cutset.py` (backdoor/overlap).
- `dagmaker/structure/`: `metadata.py` (tier A), `autodetect.py` (tier C),
  `plugins/{timeindexed,grid,graph_family}.py` (tier B), `_common.py`; dispatcher
  `__init__.py` with `all_candidates()`.
- `dagmake.py` — CLI. `DAGMAKER.md` — docs. `tests/test_dagmaker.py` — 28 tests.
- Legacy (do NOT use as gates): `dagify.py` (slow), `dag_checker.py` (has a
  variable-gap bug that rejects valid DAGs), `utils.py` (top-imports networkx).

### Y-pentacube packing — `Benchmarks/ypack/`
- `ypack_gen.py` — exact-cover CNF (pack 5×5×5 with 25 Y-pentacubes; 24 orientations,
  960 placements, ~106k clauses). `ypack5.cnf` is the generated instance.
- `ypack_verify.py` — verify a model + ASCII + matplotlib 3D PNG (`--png`),
  per-layer slices (`--slices`), rotating GIF (`--gif`), `--show-shape`.
- `ypack_gen_boundary.py` — **boundary-cell / transfer-matrix encoding** (`ypack5_b.cnf`):
  per-layer carry variables `f[k][cell]`, emits `c NODE k` markers; interface 896→75,
  count-preserving. `ypack_dag.py` — explicit slab-chain DAG builder.

### Corpus — `Benchmarks/corpus/`
- `generators.py` — 8 structured-instance generators.
- `run_corpus.py` — regression harness (`run(verbose,write)`; `--write DIR` dumps
  CNF+`.meta`+`manifest.json`). 8/8 classes matched by their intended backend.
- `README.md`. Also wired into the unit suite as `TestCorpus`.

### Other instances present
- `utilities/dag-generator/domatic_8.cnf` (with lex-leader symmetry breaking; CRLF/CP1252),
  `domatic_8_8_noSymm.cnf` (symmetry removed; the pure Hamming-cube expander),
  `domatic_3_3.cnf`. (`domatic_4_4.cnf` was referenced but never present.)
- `reports/dagster_tutorials_youtube/su.cnf` — a 9×9 Sudoku (729 vars, 3241 clauses);
  the small fast test instance used throughout.

## Key facts / decisions (don't relitigate)

1. **Cost model**: exponential in edge separator width; joins cross-multiply; depth
   amplifies. The scorer encodes this; selection = maximise (parallel_width, nodes)
   subject to `max_sep`, tie-break smaller separator / shallower.
2. **Reporting/separator tension**: outputting a variable requires it to reach a
   terminal → full reporting inflates separators on coupled problems. `--search`
   (minimal reporting) gives small separators. `--search` reports ONE variable
   (not zero) because Dagster rejects an empty REPORTING section.
3. **Encoding dominates decomposition**: ypack placements → no small separator (896);
   boundary-cell encoding → 75. Same problem.
4. **Dagster is for distributed SEARCH, not counting**: it enumerates concrete
   partial solutions, so a thin-interface slab DAG does NOT make it an efficient
   model counter. For counting, use a #SAT counter (d4/ganak/sharpSAT-td). Y-pentacube
   packing count is ≈ 65,000 (user-provided).
5. **Domatic Number on the n-cube** (user's real target, building to the open n=10):
   the graph is a Hamming-distance-≤2 **expander** (treewidth ≈ 203/256 at n=8) → no
   small separator → cutset/backdoor is correct. Reaching n=10 needs symmetry/coset
   reduction + cube-and-conquer on an HPC + DRAT proofs for UNSAT.
6. **dagmaker ≈ c_pro on dense instances** (both → 2-node 32-hub cutset). `c_pro/`
   is Charles Gretton's C++ tool (DIMACS parse + watched-literal BCP/PLE + fixed
   2-node degree-cutset). We ported its two best ideas (preprocessing, overlap cutset).

## How to build & run

- **Python (dagmaker):** use the venv: `/home/tim/dagster/.venv/bin/python`.
  Installed: click, tqdm, numpy, scipy, networkx, matplotlib. (System `python3` lacks click.)
  - `cd utilities/dag-generator && ../../.venv/bin/python -m unittest discover -s tests`
  - `../../.venv/bin/python dagmake.py <cnf> <dag> [--search] [--max-sep N] [--preprocess] [--backends a,b] [--cores N]`
  - Corpus: `cd Benchmarks/corpus && ../../.venv/bin/python run_corpus.py [--write DIR]`
- **Dagster (C++):** `cd dagster && make` (needs g++, mpic++, glog, **cudd in
  /usr/local**). **Run with `LD_LIBRARY_PATH=/usr/local/lib`** (libcudd-3.0.0.so.0
  lives there) and `OMPI_MCA_btl=self,tcp`; pass `mpirun -x LD_LIBRARY_PATH`.
  - `mpirun -n 2 --oversubscribe -x LD_LIBRARY_PATH ./dagster -m 0 -e 0 <dag> <cnf> -o out.sols`
  - `-m 0` CDCL; `-m 1 -k K` CDCL+SLS; `-m 3` CDCL+minimization; `-e 0` first solution,
    `-e 1` all; `-g 1 -c minisat` BDD interface; `-b` breadth/depth; `-n` procs.
  - Standalone solver baseline: `dagster/standalone_tinisat/tinisat <cnf> [out true]`
    (the `true` enables all-solution counting).

## Dagster bugs FIXED this session (rebuilt + regression-passing)

`SatSolver.cpp:639` (SLS prefix-spam — was the `-m 1` hang/abort; store `dLevelIndex`),
`BDDSolutions.cpp:256` (add `Cudd_Ref(tmp)`), `ReversableIntegerMap.cc:110` (`v>=size`),
`Cnf.cpp`×3 + `CnfHolder.cpp`×2 (init `header_vc/header_cc`), `Master.cpp`×3
(remove-before-delete), `Dag.cpp` (iterative cycle check), `DisorderedArray.h`
(`sizeof(T)`), `Message.cpp:84` (explicit cast), `dagmake.py --search`
(non-empty REPORTING). Regression: `-m0` d1=6 sols, `-m1` su=SAT (~4s), `-m0 -g1`
su 2-node BDD=SAT, Python=28 tests OK.

Two review findings were **false positives** (rejected after verifying): `vars[VAR(i)]`
in SatSolver (VAR=abs, so VAR(i)=i — fine) and `resolve_sorted_vectors` conflict
handling in utilities.cpp (the sorted precondition makes it correct).

## Open / deferred work

- **Deferred leaks (ownership risk):** `Cnf` per-load `clauses/cl` (fname ctors don't
  pre-allocate, so a blind `free()` crashes), `BDDSolutions::initial_messages` shutdown
  leak (murky ownership via `readd_message`), strengthener work-item leak
  (`SatSolver.cpp:348`). Need a careful per-call-site ownership pass.
- **Recommended roadmap (priority):** (1) harden Dagster — robustness, a
  best-of-breed node solver via **IPASIR (CaDiCaL; kissat is non-incremental and
  won't fit)**, **DRAT/LRAT proofs**, master scalability; (2) frictionless HPC front
  door (advisor `--cores/--memory` mode, SLURM, container); (3) heavier preprocessing
  (BVE/probing/equivalence) + METIS/KaHyPar separators + cube-and-conquer mode + an
  encoding/symmetry-reduction advisor; (4) grow the corpus.
- **CaDiCaL integration note:** `dagster/SatSolverInterface.h` is already IPASIR-shaped
  (`append_cnf`→add, `run(m)`→assume+solve, `load_into_message`→val,
  `solver_add_conflict_clause`→add blocking, `reset_solver`→re-solve). Add a
  `CadicalSolverInterface` implementation + a solver-select flag.

## Persistent memory
A condensed version of all this is in
`~/.claude/projects/-home-tim-dagster/memory/dagmaker-project.md` (loaded each session).
