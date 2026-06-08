# Symmetry-breaking × DAG-generation test + benchmark harness

`matrix.py` validates and benchmarks symmetry breaking together with DAG
generation. For each problem and each `--symbreak` level (`none`/`light`/`full`/
`dag`) it generates a DAG with `dagmake`, then:

**Sanity checks (correctness):**
1. *symmetry-breaking soundness* — the broken CNF solved single-node must give the
   **same verdict** as the original (breaking is verdict-preserving);
2. *DAG soundness* — the decomposed run must equal that single-node ground truth;
3. *cross-level parity* — all levels agree SAT/UNSAT.

**Benchmark (speedup):** wall-clock time solving the DAG at each level, plus the
DAG shape (nodes / `max_sep` / `parallel_width`) and how many breaking clauses the
`dag` level kept vs dropped.

Everything is **rc-aware**: a non-zero dagster exit (parse abort / crash / timeout)
is reported as `ERR`/`TIMEOUT`, never silently as `UNSAT`.

## Usage

```sh
python3 matrix.py --profile local                 # run here (~8-core box), cadical
python3 matrix.py --profile local --modes 0,5      # tinisat + cadical
python3 matrix.py --profile hpc --emit-hpc ./hpc   # emit a SLURM job array
sbatch ./hpc/symbreak_array.slurm                  # on the cluster
```

`--modes` is a comma list of dagster solver modes (0 tinisat, 4 minisat, 5 cadical,
7 cryptominisat). Local writes `results.csv`; HPC writes `symbreak_array.slurm` +
`cells.tsv` + materialised problems/DAGs.

## What the local run showed (4-core laptop, cadical)

- **Sanity: all passed** — every level verdict-preserving and DAG-sound.
- **Speedup on symmetric UNSAT:** pigeonhole 5.4× (small) and 2.2× (larger). This
  is where symmetry breaking pays off.
- `none`/`light`/`full`/`dag` are close in *wall time* on the laptop because the
  `dag` level's advantage is **parallel width**, and with only 4 cores an N-node
  DAG (N>4) is oversubscribed — the parallelism can't translate to wall-clock.
  **The `dag` level's payoff needs the HPC** (enough cores for `parallel_width`
  workers), which is exactly what the `hpc` profile is for.

## Caveat on cores

SLS configurations (`--sls`) need ≥3 ranks (master + worker + ≥1 gnovelty helper).
Non-SLS DAG runs launch `1 + #DAG-nodes` ranks; on a small box that oversubscribes
for wide DAGs, so wall-time comparisons of the parallel levels are only meaningful
once `#ranks ≤ #cores` (i.e. on the HPC). Raise `max_ranks`/`target_nodes` in the
`hpc` profile for real scaling studies.
