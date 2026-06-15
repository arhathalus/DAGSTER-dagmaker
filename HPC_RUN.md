# HPC benchmark run plan

Three benchmark harnesses, each emits a SLURM job array and has a `collect.py` that
turns the finished run into a report. This is the plan to validate, on real cores,
that Dagster works **and** that its improvements (clause sharing especially) help.

Run them from the HPC login node (the emit step generates problems/DAGs/cubes
locally, then writes the array script you `sbatch`).

## Prerequisites (once, on the HPC)

```sh
cd dagster && make                          # build the solver
make -C cadical_solver/cadical              # (only if using --proof)
cd ../Benchmarks && python3 generate_benchmarks.py   # materialise the corpus (gitignored)
# ensure the repo .venv exists with deps (dagmaker, cube.py, determinant use it)
# optional extra backends:
#   bash ../dagster/ipasir_solver/build_glucose.sh
#   bash ../dagster/ipasir_solver/build_lingeling.sh
```

Edit each emitted `*.slurm` for your partition/account if needed (the templates set
`--nodes`, `--ntasks`, `--time`, `--output`).

## 1. Clause-sharing speedup — the headline

Plain CaDiCaL (`--backend cadical`) vs clause sharing (`--share`) on the hard
pigeonhole ladder (exponential UNSAT — where sharing pays off), at 64 ranks.

```sh
cd dagster/tests/cube_matrix
python3 matrix.py --profile hpc --emit-hpc ./hpc --modes 5,10
sbatch ./hpc/cube_array.slurm
#   when finished:
python3 collect.py ./hpc                    # plain-vs-share speedup table (+ --csv out.csv)
```
Read: per-problem plain vs shared wall time + the **speedup factor**, a geomean
summary, and a **verdict-mismatch flag** (sharing must never change a verdict). The
small toy problems are the correctness/no-deadlock check; the `pigeonhole11..15`
ladder is the speedup measurement.

## 2. Backend correctness across the known-answer corpus

Every backend vs the independent-oracle-labelled corpus (costas/determinant/ramsey
+ sudoku). This is "does everything still agree on the right answer at scale."

```sh
cd dagster/tests/backend_matrix
python3 matrix.py --profile hpc --emit-hpc ./hpc
sbatch ./hpc/matrix_array.slurm
#   when finished:
python3 collect.py ./hpc                    # per-problem backend agreement + backend scoreboard
```
Read: a **CORRECTNESS** section (all backends/DAGs must agree per problem; ground
truth = single-node DAG verdict; exit 1 on any disagreement) and a **BACKEND
SCOREBOARD** (solved / timeout / error + median time per backend). Add Glucose /
Lingeling with `--modes ...,11,12` once their `.so`s are built.

## 3. Symmetry breaking — soundness + speedup

```sh
cd dagster/tests/symbreak_matrix
python3 matrix.py --profile hpc --emit-hpc ./hpc
sbatch ./hpc/symbreak_array.slurm
#   when finished:
python3 collect.py ./hpc                    # per (problem,backend): soundness across levels + speedup
```
Read: **SOUNDNESS** (none/light/full/dag must all agree on SAT/UNSAT; a level
disagreeing with `none` = symmetry breaking changed the answer = UNSOUND, exit 1)
and **SPEEDUP** (each level vs `none`, with DAG shape + kept/dropped breaking clauses).

## Interpreting / collecting

- Every array task writes `cell_<id>.out` (with a `<TAG> task= rc= wall=` timing
  line) and `sol_<id>.txt` (non-empty = SAT). `cells.tsv` is the task→what index.
- A **missing timing line** = the task hit the SLURM `--time` limit (TIMEOUT).
- Each `collect.py` **exits non-zero** on a correctness/soundness failure, so it is
  CI-friendly.

## After the run

- Send the `collect.py` output (or `--csv`) back for interpretation / tuning
  (cube counts, difficulty band, `--share-max-size`, ranks).
- The clause-sharing geomean is the number that says whether `--share` earns its
  hub rank on real cores.

## Notes

- `mpirun`/`srun` flags, `LD_LIBRARY_PATH=/usr/local/lib`, and `OMPI_MCA_btl` are set
  in the templates; adjust for your MPI/site.
- Profiles live in each `matrix.py` (`PROFILES`): `max_ranks`, `timeout`, `sizes`,
  `modes`. Tune there for your allocation.
- For the research targets (open ramsey n=8/n=13, domatic), see `ONBOARDING.md` §
  "Research frontier" — those are attacked with `solve.py --share` + `cc_certify.py`,
  not these correctness/scaling harnesses.
