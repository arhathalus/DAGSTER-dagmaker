# Structured-problem corpus

A set of SAT instances with **known structure**, used to develop and regression-
test `dagmaker`'s decomposition strategies. Each class is a parameterized
generator (`generators.py`) that emits signed clauses + a `meta` dict naming the
structure and the dagmaker backend expected to exploit it.

| class | structure | expected backend | what it tests |
|---|---|---|---|
| `chain_bmc` | sequential / BMC unfolding | timeindexed / ordering | thin state-variable chain |
| `grid_coloring` | grid graph (2-colour) | ordering / elimination | row/column separators |
| `tree_constraints` | tree-structured constraints | biconnected | every edge a bridge → sep 1 |
| `modular` | planted communities | community / biconnected | dense clusters, sparse bridges |
| `components` | disjoint subproblems | elimination | free parallelism (width) |
| `expander` | random 3-SAT (threshold) | cutset / none | no exploitable structure |
| `pigeonhole` | PHP (symmetric, UNSAT) | cutset / none | dense symmetric, no separator |
| `banded_xor` | windowed parity | ordering / elimination | banded (chain-like) |

## Usage

```
# run the regression (prints a table; exits non-zero on a regression)
python run_corpus.py

# also write <DIR>/<class>.cnf + .meta + manifest.json
python run_corpus.py --write ./instances
```

The harness runs `dagmaker.pipeline.generate` on each class with search-style
reporting (so separators reflect structure, not full-output carrying) and checks
that the intended strategy finds a small separator — and that the negative
controls (`expander`, `pigeonhole`) have **no** small multi-node split. It is
also invoked from the unit suite (`TestCorpus`).

To match a *new* real instance to a custom DAG: identify which class it resembles
(or give dagmaker a `.meta` sidecar), then use the corresponding backend via
`dagmake.py --backends <name>` or let multi-backend scoring pick it automatically.
