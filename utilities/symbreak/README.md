# Symmetry breaking (BreakID)

Vendored copy of [BreakID](https://github.com/meelgroup/breakid) — a standalone
symmetry-breaking preprocessor for DIMACS CNF. It detects the formula's symmetry
group via graph automorphism (bundled `bliss`) and adds lex-leader
symmetry-breaking clauses. `dagmaker` uses it as a **pre-decomposition** step (see
`dagmaker/symbreak.py` and `dagmake --symbreak`).

## Build

bliss is bundled (`breakid/src/bliss`); the only requirement is CMake + a C++
compiler. No network needed after the initial clone.

```sh
cd breakid
mkdir -p build && cd build
cmake -DCMAKE_BUILD_TYPE=Release ..
make -j breakid-bin        # -> build/breakid
```

`dagmaker/symbreak.py` looks for the binary at `breakid/build/breakid`.

## Why a preprocessor (not the solver's internal breaking)

Symmetry-breaking clauses are lex-leader chains that **couple variables**, raising
the formula's treewidth / separator width — which is exactly what Dagster's
per-node cost is exponential in. So breaking has to happen at the **CNF level,
before** `dagmaker` decomposes, so the DAG generator sees (and can trade off
against) the added coupling. A solver's *internal* symmetry breaking runs after
the DAG is fixed and can't inform decomposition.

`dagmake --symbreak` exposes a spectrum:

| level   | how | effect |
|---------|-----|--------|
| `none`  | (not run)               | best DAG / parallelism, largest search space |
| `light` | `breakid -s 20 --row false` | local point symmetries only; skips global matrix/row interchange → minimal added coupling, stays decomposable |
| `full`  | `breakid -s 50 --row true`  | break everything BreakID finds → smallest space, but couples variables and can widen DAG separators |
| `dag`   | **DAG-aware** (see below)   | break everything, then keep only the breaking clauses that fit within a single DAG node; drop cross-node ones → reduction *and* parallelism |

### `dag` — DAG-aware breaking (the recommended mode for the HPC)

`full` minimises the search space but its breaking chains can couple variables
across the decomposition and **serialise** an otherwise-parallel problem. `dag`
gets the best of both:

1. decompose the **original** formula → node variable sets;
2. run `full` BreakID;
3. **keep** each breaking clause only if all its variables fit within a single
   node (adds no cross-node coupling); **drop** the clauses that span nodes.

Dropping a subset of sound breaking clauses is itself sound (it only keeps more
assignments). Worked example — 5 identical "exactly-one-of-3" blocks:

| level | best DAG | breaking |
|---|---|---|
| `none` | `max_sep=0 parallel_width=5` | — |
| `full` | `max_sep=19 parallel_width=1` (serialised!) | breaks all |
| `dag`  | `max_sep=0 parallel_width=5` | kept 21 within-block, dropped 48 cross-block |

`dag` keeps all 5 parallel nodes (spreads across the HPC) while still breaking the
within-block symmetry. Because the trade-off is instance-dependent, you can also
just **measure**: compare `--symbreak none/full/dag` and read off each DAG's
`max_sep` / `parallel_width` (dagmake prints them).

## Important: output normalisation

BreakID's breaking clauses can contain **duplicate literals** (`-a -a b`), which
is logically harmless but dagster's CNF parser rejects ("duplicate literals in
clause"). `dagmaker/symbreak.py` normalises BreakID's output (dedupe literals,
drop tautologies) before the CNF reaches dagster. Symmetry breaking is verdict
preserving (verified: original SAT/UNSAT == broken SAT/UNSAT across the corpus).
