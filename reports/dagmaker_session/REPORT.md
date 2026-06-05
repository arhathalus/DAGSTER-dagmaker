# Dagster / dagmaker — Session Work Report

## Overview

This session built a structure-aware DAG generator for the **Dagster** distributed
SAT solver, benchmarked it, hardened the Dagster engine itself, and worked through
two concrete problem families (Domatic Number on the Hamming cube; Y-pentacube
cube packing). The throughline: Dagster's runtime cost is **exponential in the
number of variables passed along a DAG edge** (the vertex separator), joins
multiply, and depth amplifies — so generating a good DAG = finding small
separators while exposing useful parallelism, subject to a memory/separator budget.

## What was built

### `dagmaker` — a new DAG generator (Python, stdlib core)
Location: `utilities/dag-generator/dagmaker/` + CLI `dagmake.py` + `DAGMAKER.md`.
Replaces the slow, lower-quality `dagify.py`. A **candidate-and-score pipeline**:
parse → (optional preprocess) → connected-components pre-pass → every backend
proposes a DAG → validate → score against the cost model → keep the best → write
`.dag` + recommend a tuned `dagster` invocation.

Decomposition backends (all scored together):
- **single** — whole problem, one node (guaranteed-valid floor).
- **elimination** — min-degree variable ordering → frontier chunking (treewidth-style).
- **ordering** — same chunker via BFS / RCM / spectral orderings.
- **biconnected** — articulation/block-cut tree (size-1 separators at cut variables).
- **community** — Louvain (networkx) / stdlib label-propagation modules.
- **gates** — AND/OR gate-definition layering (circuit/Tseitin structure).
- **cutset** — high-degree hub backdoor with clause overlap (for expanders).
- **structure tiers** — metadata (`c`-comment groups / `.meta` sidecar),
  plugins (timeindexed, grid, graph), autodetect.

Supporting modules: `cnf.py`, `intervals.py`, `dagmodel.py`, `graph.py`,
`assemble.py` (the only place edge variable-sets are computed; enforces soundness),
`scorer.py`, `validate.py` (overlap-aware running-intersection check),
`components.py`, `preprocess.py` (BCP+PLE), `advisor.py`, `pipeline.py`.
Tests: `tests/test_dagmaker.py` (**28 passing**).

### Preprocessing + overlap (ported from `c_pro`)
- **BCP + PLE** (`preprocess.py`, `--preprocess`): unit propagation + pure-literal
  elimination to a fixpoint; matches `c_pro`'s reductions exactly (e.g. 8-64:
  1,274,350 → 1,143,890 clauses). Often exposes more components.
- **Overlap / cutset**: clauses may appear in multiple nodes (Dagster allows it).
  Validator uses the running-intersection property (relaxed so a variable fully
  contained in one node is resolved authoritatively). `--strict-partition` opts out.

### Structured-problem corpus
`Benchmarks/corpus/`: 8 parameterized generators (`generators.py`) emitting
CNF + `.meta`, a regression harness (`run_corpus.py`, **8/8 classes pass**), a
README, and a manifest. Classes: chain_bmc, grid_coloring, tree_constraints,
modular, components, expander, pigeonhole, banded_xor.

### Y-pentacube packing toolkit
`Benchmarks/ypack/`: `ypack_gen.py` (exact-cover CNF, 960 placements/106k clauses
for 5×5×5), `ypack_verify.py` (verify + ASCII + 3D matplotlib render + per-layer
slices + rotating GIF), `ypack_dag.py` (slab DAG), `ypack_gen_boundary.py`
(boundary-cell "transfer-matrix" encoding: interface 896 → 75, count-preserving).

### Dagster (C++) bug fixes — see below.

## Key findings

- **Cost model** (verified in source): exponential in separator width; joins take a
  cross-product; work multiplies along paths; depth amplifies.
- **Reporting/separator tension**: to *output* a variable it must reach a terminal,
  so full-variable reporting forces wide separators on coupled problems; search-style
  (minimal) reporting yields small ones. `dagmake --search` supports this.
- **Encoding dominates**: Y-pentacube placement encoding has no small separator
  (896); the boundary-cell encoding exposes a 75-wide layer chain — same problem.
- **Dagster is a distributed SEARCH engine, not a counter**: it enumerates concrete
  partial solutions (not profile-equivalence-classes), so even a thin-interface slab
  DAG won't make it count efficiently. For counting (~65,000 packings), use a #SAT
  counter (d4/ganak); the DAG/cube-and-conquer route is for distributed search/UNSAT.
- **Domatic Number on the Hamming cube** is an *expander* (Hamming distance ≤ 2,
  36-regular at n=8, treewidth ≈ 203/256) — no small separator exists, so the
  cutset/backdoor is the correct decomposition. Removing symmetry breaking didn't
  add exploitable structure. Reaching n=10 needs **symmetry/coset reduction +
  cube-and-conquer on an HPC**, not just a better DAG.
- **dagmaker vs c_pro**: converge on dense instances (both → 2-node 32-cut);
  dagmaker wins where structure exists (component-awareness on 8-64; biconnected
  found a sep-6 cut on lex-leader domatic; metadata gives ypack5_b's 75-wide chain).

## Dagster bug fixes (all applied, rebuilt, regression-passing)

| location | bug → fix |
|---|---|
| `SatSolver.cpp:639` | SLS guard stored `dLevel`, compared `dLevelIndex` → prefix-spam livelock/abort in `-m 1`. Store `dLevelIndex`. **Root cause of the SLS failure.** |
| `BDDSolutions.cpp:256` | `readd_message` missing `Cudd_Ref(tmp)` → CUDD use-after-free. Added. |
| `ReversableIntegerMap.cc:110` | `v>size` allowed OOB read → `v>=size`. |
| `Cnf.cpp` ×3, `CnfHolder.cpp` ×2 | uninitialized `header_vc/header_cc` (UB on headerless CNF) → init to 0. |
| `Master.cpp` ×3 | delete-before-remove used a freed pointer → remove-before-delete. |
| `Dag.cpp` | exponential recursive cycle check → O(V+E) iterative white/grey/black DFS. |
| `DisorderedArray.h` ×2 | `sizeof(T*)` → `sizeof(T)` (latent). |
| `Message.cpp:84` | implicit `size_t→int` narrowing → explicit, documented. |
| `dagmake.py --search` | emitted empty REPORTING (dagster rejects) → reports one var. |

Regression: `-m 0` (d1)=6 solutions; `-m 1 -k 1` (su)=SAT in ~4s (was a hang);
`-m 0 -g 1` (su 2-node BDD)=SAT; Python suite=28 tests OK.

**Deferred** (ownership risk, benign): `Cnf` per-load `clauses/cl` leak,
`BDDSolutions::initial_messages` shutdown leak, strengthener work-item leak.

## Recommended next steps (priority order)

1. **Harden Dagster (Tier 1, the gate to solving the unsolvable):** robustness, then
   a best-of-breed node solver via **IPASIR** (CaDiCaL — the `SatSolverInterface` is
   already IPASIR-shaped; kissat is non-incremental and won't fit), then **DRAT/LRAT
   proof logging** for certifiable UNSAT, then master scalability. Finish the
   deferred ownership/leak review here.
2. **Frictionless HPC front door (cheap, high adoption ROI):** `--cores/--memory`
   HPC mode in the advisor, SLURM script generation, container fix for the
   `LD_LIBRARY_PATH`/MPI setup.
3. **Feed it well:** heavier preprocessing (BVE/probing/equivalence), high-quality
   separators (METIS/KaHyPar/FlowCutter), a cube-and-conquer mode, and an
   *encoding/symmetry-reduction* advisor (the real lever for open problems).
4. **Grow the corpus** as the regression/playbook harness.
