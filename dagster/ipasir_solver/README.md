# IPASIR backend — drop in any incremental SAT solver

`IpasirSolver` is a `SatSolverInterface` backend over the standard incremental SAT
API, **IPASIR**. Dagster's solver contract already *is* IPASIR (run = assume+solve,
`load_into_message` = val, conflict/append = add), so this is a thin adapter. The
IPASIR solver is loaded **at run time from a shared library via `dlopen`**, so any
IPASIR-compliant solver drops in by building it as a `.so` — **no Dagster recompile,
no symbol collisions** (each solver is a separate `.so`).

## Use

```sh
# generic: point at any libipasirSOLVER.so
mpirun -n N dagster --backend ipasir --ipasir-lib /path/to/libipasirX.so  DAG CNF

# convenience: vendored Glucose (defaults --ipasir-lib to ipasir_solver/libipasirglucose.so,
# resolved relative to the working dir -- run from dagster/, or pass an absolute --ipasir-lib)
mpirun -n N dagster --backend glucose  DAG CNF
```

## Glucose (the bundled drop-in)

Glucose 4.2.1 is vendored under `glucose/` (core/mtl/utils/simp). Build its `.so`:

```sh
bash ipasir_solver/build_glucose.sh        # -> ipasir_solver/libipasirglucose.so
```

`glucose_glue.cc` implements the IPASIR entry points over Glucose's core `Solver`
(not `SimpSolver`, so no variable elimination — fully incremental, no freezing).
Validated against the other backends: identical verdicts (UNSAT/SAT) and identical
solution *counts* under enumeration (`-e 1`).

## Lingeling (a second drop-in — proves the genericity)

Lingeling (Biere) is a completely different engine from the MiniSat family, added
with **zero Dagster code changes** — just another `.so`. Its source is **vendored**
under `lingeling/`, so the build is standalone (no fetch):

```sh
bash ipasir_solver/build_lingeling.sh                 # -> libipasirlingeling.so
# (or point at a checkout elsewhere: build_lingeling.sh /path/to/lingeling)
mpirun -n N dagster --backend lingeling  DAG CNF
```
`build_lingeling.sh` runs lingeling's `configure.sh` + `make liblgl.a` from the
vendored source, then links `lingeling_glue.cpp` (which freezes variables for
incremental use) into the `.so`. Only lingeling's build artifacts (`makefile`,
`lglcfg.h`, `lglcflags.h`, `*.o`, `liblgl.a`) are git-ignored — the source is
tracked. Validated to match the other backends on verdicts and enumeration counts.

## MapleSAT (a third drop-in — an engine Painless uses)

MapleSAT (LRB branching; the Maple family Painless bundles) is a MiniSat/Glucose
descendant, so `maple_glue.cc` is `glucose_glue.cc` with the `Minisat` namespace.
Its source is **vendored** under `maplesat/` (standalone, no fetch):

```sh
bash ipasir_solver/build_maple.sh                     # -> libipasirmaplecomsps.so
mpirun -n N dagster --backend maple  DAG CNF
```

The vendored `maplesat/` is the **`assumptions-incremental` branch** of bitbucket
`JLiangWaterloo/maplesat`, **NOT `maplecomsps`** — this matters. Dagster drives the
node solver **incrementally with assumptions** (the DAG interface assignment /
cubes), and the competition `maplecomsps` branch mishandles the model under
assumptions, so Dagster would reject it (`ipasir backend returned false solution`)
on any multi-node DAG — it only appears to work on single-node/whole-formula
solves. This tree is clean (no MathCheck programmatic hooks) and **validated to
match CaDiCaL** on multi-node enumeration (costas5/6/7 → 6/17/30 solutions; UNSAT
parity on 4unsat). `build_maple.sh` needs `-fpermissive` (a 2017 `friend mkLit()`
default-arg that modern g++ rejects).

> Note: this is the natural fit for a *whole-formula / portfolio* role (how
> Painless uses Maple). It also works as a Dagster node backend for DAG
> decomposition, but Maple is tuned for one-shot competition solving, so on a
> given instance CaDiCaL is often the stronger node engine — benchmark per family.

## Adding another IPASIR solver (Maple, CaDiCaL, …)

1. Get its source + an IPASIR glue (most ship one; the glue is ~70 lines, mirror
   `glucose_glue.cc` / `lingeling_glue.cpp`).
2. Build it `-fPIC -shared` into `libipasir<solver>.so` (mirror the build scripts).
3. Run `--backend ipasir --ipasir-lib libipasir<solver>.so`. No Dagster rebuild.
   (A `--backend <name>` convenience alias is a one-liner in main.cpp if wanted.)

## Notes / limits

- Wraps the **core** solver for safe incrementality; preprocessing solvers
  (SimpSolver) would need variable freezing — a later refinement.
- No `--sls` variant (SLS helpers attach via a solver-specific ctor); guarded off.
- The `glucose/` source is tracked; the built `.so` and `.o` files are git-ignored
  (regenerate with `build_glucose.sh`).
