# dagmaker — structure-aware DAG generator for Dagster

`dagmaker` generates a Dagster `.dag` decomposition from a DIMACS CNF. It is a
faster, structure-aware successor to `dagify.py`.

## Why

Dagster's runtime cost is **exponential in the number of variables passed along a
DAG edge** (the vertex separator between subproblems), joins multiply incoming
partial-solution sets, and depth amplifies cost (verified in `dagster/Dag.cpp`,
`SolutionsInterface.h`, `TableSolutions.cpp`). So a good DAG **minimises the
maximum separator width** while exposing useful parallelism.

`dagify.py` recovers structure from the flat primal graph with min-fill
elimination — slow at scale, and it discards the high-level structure the CNF
encoder knew. `dagmaker` is **structure-aware first, generic second**, and runs
a stdlib-only core (no networkx/numpy required).

## Install

Core needs only the stdlib + `click` (already in the project `.venv`). Optional
accelerators (used automatically if present):

    pip install numpy scipy        # faster components + (future) RCM ordering
    pip install pymetis kahypar    # (future) high-quality native partitioning

## Usage

    ../../.venv/bin/python dagmake.py problem.cnf problem.dag [options]

Key options:

| option | meaning |
|---|---|
| `--nodes/-k N` | target node count (an outcome, not a hard cap) |
| `--cores N` | HPC core budget; sizes the recommended `-n` |
| `--max-sep N` | separator-width cap; over-budget cuts are merged, not emitted |
| `--reporting "1-9,20"` | variables to output (default: all used). A smaller set enables smaller separators |
| `--prune/--pass-all-data` | carry only downstream-needed vars (smaller separators) vs. forward the full neighborhood (legacy-compatible) |
| `--family NAME` | force a structure family: `timeindexed`, `grid`, `graph` |
| `--meta FILE.meta` | JSON sidecar describing structure |
| `--preprocess` | simplify with unit propagation + pure-literal elimination first; writes the simplified CNF the DAG references |
| `--simplified-cnf PATH` | where to write the simplified CNF (default: alongside the DAG) |
| `--symbreak {none,light,full,dag}` | add BreakID symmetry-breaking clauses before decomposing (DAG references the augmented CNF). `light` = local symmetries only; `full` = break everything (smaller space, more coupling); **`dag` = DAG-aware** — break everything then keep only the breaking clauses internal to a single DAG node (reduction *and* parallelism). See `../symbreak/README.md`. |
| `--symbroken-cnf PATH` | where to write the symmetry-broken CNF (default: alongside the DAG) |
| `--strict-partition` | require each clause in exactly one node (disables the cutset backend) |
| `--cutset-hubs N` | hub count for the overlap/cutset backend (capped at `--max-sep`) |

It prints the candidate DAGs with their costs, writes the best one, and
recommends a tuned `dagster` invocation (mode, `-k`, BDD, breadth/depth, `-n`).

## How it works

0. **Optional preprocessing** (`--preprocess`) — unit propagation (BCP) + pure-
   literal elimination to a fixpoint (a port of `c_pro/up.cpp`): drops satisfied
   clauses, removes falsified literals, fixes forced/pure variables, detects
   UNSAT. The simplified CNF is written out and the DAG references it (fixed
   variables are recorded as `c FIXED` comments for solution reconstruction).
   Shrinking the CNF also tends to expose *more* connected components.
1. **Connected-components pre-pass** — independent components become parallel
   branches with zero communication (free parallelism).
2. **Candidate decompositions**, scored against the cost model and the best kept:
   - **Structure tiers** (when the family is known/detected):
     - *(A) metadata* — `c <label>` comment groups (as `Benchmarks/sudoku/dag_gen.py`
       emits) or a `.meta` JSON sidecar.
     - *(B) plugins* — `timeindexed` (BMC/planning → chain on state-variable
       separators), `grid` (Sudoku/CSP row bands), `graph`.
     - *(C) autodetect* — variable-stride/period (time-indexed) and `N×N×N`
       grid detection.
   - **Generic** — min-degree elimination ordering + frontier chunking
     (clean partition, small separators where structure exists).
   - **Ordering** (`ordering`) — the same frontier chunker driven by BFS / RCM /
     spectral variable orderings; finds better cut points than min-degree on some
     instances.
   - **Biconnected** (`biconnected`) — articulation/block-cut decomposition; where
     the variable graph has cut variables it yields a tree DAG with size‑1
     separators. (Declines when the graph is one block.)
   - **Community** (`community`) — partition variables into modules (Louvain via
     networkx, else stdlib label propagation); strong on modular/industrial CNFs.
   - **Gates** (`gates`) — detect AND/OR gate definitions and order variables by
     definitional depth; isolates circuit/Tseitin layers (e.g. BMC). Needs the
     signed clauses.
   - **Cutset / overlap** — pick a small set of high-degree hub variables;
     node 0 constrains them, the terminal solves *all* clauses with the hubs
     fixed, and only the hubs are passed. Clauses overlap across nodes, so the
     separator is just the hub count *regardless of the reporting set* — this is
     what lets dagmaker decompose densely-coupled, full-reporting problems
     (inspired by `c_pro`). Disabled by `--strict-partition`.
   - **Single-node** — guaranteed-valid floor.
3. **Validation** — coverage, acyclicity, reporting reachability, and soundness.
   By default clause **overlap is allowed** (Dagster permits it); the soundness
   check is the running-intersection property, relaxed so a variable fully
   contained in one node (e.g. the cutset terminal) is resolved authoritatively.
   `--strict-partition` re-imposes exactly-once partitioning. (`validate.py`; the
   legacy `dag_checker.py` has a variable-gap bug and is not used as a gate.)

## The reporting / separator tradeoff

To output a variable it must reach a terminal node. Reporting *all* variables
therefore forces them all forward → large separators on densely-coupled problems
(e.g. Sudoku). Small separators require either genuine structure or a
**restricted `--reporting` set**. `dagmaker` is honest about this: if no
decomposition fits `--max-sep`, it falls back to single-node.

## Tests

    ../../.venv/bin/python -m unittest discover -s tests -v

## Status / follow-ups

Implemented: stdlib core, components, generic elimination backend, ordering
(BFS/RCM/spectral), biconnected/articulation, community detection, gate/XOR,
cutset/overlap, BCP+PLE preprocessing, structure tiers (metadata +
timeindexed/grid plugins + autodetect), scorer, parameter advisor, overlap-aware
validator, CLI (`--backends` to restrict), multi-backend scoring, test suite (28
tests). Optional accelerators: numpy/scipy (spectral), networkx (Louvain,
biconnected).

A labelled benchmark corpus of structured instances lives in
`Benchmarks/corpus/` (see its README); `run_corpus.py` is a regression harness
that checks each structure class is matched by its intended strategy.

Not yet implemented (the framework supports adding them as further candidates in
`pipeline.generate` / `structure.all_candidates`): `decompose/rcm.py` (SciPy
RCM), `decompose/partition.py` (stdlib FM-lite bisection), `decompose/external.py`
(pymetis/kahypar). These are quality/speed accelerators; the tool is fully
functional without them.
