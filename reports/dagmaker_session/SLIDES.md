---
marp: true
title: Dagster / dagmaker — Session Summary
paginate: true
---

# Dagster / dagmaker
### Session summary

Structure-aware DAG generation + engine hardening for the Dagster distributed SAT solver

---

## The core idea

Dagster solves a CNF by following a **DAG** that splits it into subproblems.

**Cost is exponential in the variables passed on an edge** (the vertex separator).
Joins multiply; depth amplifies.

> Generating a good DAG = find small separators, expose useful parallelism,
> subject to a separator/memory budget.

---

## What we built: `dagmaker`

A candidate-and-score pipeline (replaces `dagify.py`):

```
CNF → [preprocess] → components → {backends propose DAGs}
    → validate → score → keep best → write .dag + advise dagster flags
```

Stdlib core; numpy/scipy/networkx optional. **28 tests**, CLI `dagmake.py`.

---

## Six decomposition lenses (+ structure tiers)

| backend | finds |
|---|---|
| elimination | min-degree treewidth-style chain |
| ordering (BFS/RCM/spectral) | better low-frontier cuts |
| **biconnected** | size-1 separators at cut variables |
| **community** | modular clusters (Louvain) |
| **gates** | circuit/Tseitin definition layers |
| cutset | high-degree backdoor (for expanders) |

Structure tiers: metadata (`c`-comments / `.meta`), plugins (timeindexed/grid/graph), autodetect.

---

## Preprocessing + overlap (from `c_pro`)

- **BCP + PLE** (`--preprocess`): unit propagation + pure-literal elimination.
  Matches c_pro exactly (8-64: 1.27M → 1.14M clauses); exposes more components.
- **Overlap / cutset**: clauses may live in multiple nodes (Dagster allows it);
  validator uses the **running-intersection** property.

---

## Labelled corpus + regression

`Benchmarks/corpus/` — 8 structure classes, each → the right strategy:

```
chain_bmc → timeindexed   tree → biconnected (sep 1)
grid → elimination        modular → biconnected (sep 1)
components → parallel×4    banded_xor → ordering
expander / pigeonhole → no small split (cutset)   ✓ 8/8
```

---

## Case study 1: Domatic Number (Hamming cube)

- Graph = Hamming distance ≤ 2 → a **36-regular expander**, treewidth ≈ 203/256.
- **No small separator exists** → cutset/backdoor is the *correct* answer.
- Removing symmetry breaking didn't help.
- Path to n=10 (open): **symmetry/coset reduction + cube-and-conquer on an HPC**,
  not a cleverer DAG.

---

## Case study 2: Y-pentacube packing (5×5×5)

- Easy to *find* one packing (3 s); the goal is to **count** (~65,000).
- Placement encoding → no small separator (interface **896**).
- **Boundary-cell ("transfer-matrix") encoding** → interface **75**, count-preserving.
- **But:** Dagster enumerates concrete partial solutions, *not* profile classes →
  it is **not a model counter**. Use a #SAT counter (d4/ganak); the DAG is for search.

---

## Encoding > decomposition

Same problem, two encodings:

| ypack5 (placements) | ypack5_b (boundary cells) |
|---|---|
| no sub-896 split → cutset | clean 5-layer chain, sep **75** |

dagmaker auto-builds the layer chain from `c NODE` markers (metadata tier).

---

## We fixed real Dagster bugs

- **`-m 1` (SLS) livelock/abort** → one-word fix (`SatSolver.cpp:639`): the prefix
  guard stored the wrong value and re-sent forever. **Now solves in 4 s.**
- CUDD use-after-free, OOB read, uninitialized header, freed-pointer use,
  exponential cycle check, `--search` empty-REPORTING — **all fixed & regression-tested.**

---

## Roadmap to "solve the unsolvable, easily"

1. **Harden Dagster (the gate):** robustness · best-of-breed node solver via
   **IPASIR/CaDiCaL** · **DRAT proofs** · master scalability.
2. **Frictionless HPC front door:** `--cores/--memory` mode, SLURM, containers.
3. **Feed it:** heavier preprocessing · METIS/KaHyPar separators · cube-and-conquer
   · an **encoding/symmetry** advisor.
4. **Corpus** = the regression + playbook harness.

> Trap to avoid: polishing dagmaker/corpus while Dagster's robustness, node-solver
> strength, and proofs go unaddressed.

---

## CaDiCaL backend — already feasible

`SatSolverInterface` is **already IPASIR-shaped** (add clauses, assume, solve,
read model, blocking-clause re-solve). Write one `CadicalSolverInterface`:

- **CaDiCaL** ✓ incremental + assumptions + DRAT proofs (and likely faster).
- **kissat** ✗ non-incremental by design — wrong shape.

---

# Status

- dagmaker: 6 backends + structure tiers + preprocessing + 28 tests
- corpus: 8 classes, 8/8 matched
- Dagster: 9 bugs fixed, rebuilt, regression-passing; SLS works again
- Clear, prioritized roadmap

**Next:** CaDiCaL/IPASIR backend + the deferred ownership/leak review.
