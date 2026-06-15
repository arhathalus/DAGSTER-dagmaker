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
with **zero Dagster code changes** — just another `.so`. Provide the source
yourself from GitHub (the build script does **not** fetch), then build:

```sh
git clone https://github.com/arminbiere/lingeling ipasir_solver/lingeling
bash ipasir_solver/build_lingeling.sh                 # -> libipasirlingeling.so
# (or point at a checkout elsewhere: build_lingeling.sh /path/to/lingeling)
mpirun -n N dagster --backend lingeling  DAG CNF
```
`lingeling_glue.cpp` (vendored) freezes variables for incremental use. The
`lingeling/` source dir is git-ignored (you supply it); validated to match the
other backends on verdicts and enumeration counts.

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
