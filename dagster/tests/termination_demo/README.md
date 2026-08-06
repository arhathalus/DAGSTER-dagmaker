# Minimal reproducer: non-interruptible-solve termination bug

A tiny demonstration of the bug that the `--yield-seconds` work fixes: **a worker
stuck in a non-interruptible node solve blocks the whole run from terminating, even
after another worker has already found the answer.**

This is *plain DAG decomposition* — it reproduces on the **original, pre-fix
Dagster** (it does not need cube-and-conquer, which the original didn't have).

## The instance (`demo.cnf` + `demo.dag`)

Two **disjoint** single-node subgraphs, handed to two workers concurrently:

| node | contents | difficulty |
|---|---|---|
| 0 | a trivially-SAT formula over vars 1,2 | solved in ~microseconds (terminal → triggers `-e 0`) |
| 1 | pigeonhole 12 (13 pigeons, 12 holes) over disjoint vars | UNSAT, exponentially hard for CDCL — a worker grinds on it for minutes |

Regenerate / make it harder with `python3 gen_demo.py [H]` (default `H=12`; larger
`H` → longer hang).

## Reproduce (original / pre-fix Dagster, legacy `-m` flags)

```sh
export OMPI_MCA_btl=self,tcp
# tinisat (interruptible: it yields every sat_reporting_time decisions)
mpirun -n 3 --oversubscribe ./dagster -m 0 -g 1            -e 0 demo.dag demo.cnf -o out.sols
# minisat (NON-interruptible: run() solves to completion, returns only 0/1)
mpirun -n 3 --oversubscribe ./dagster -m 4 -k 1 -g 1 -q 0  -e 0 demo.dag demo.cnf -o out.sols
```

Measured on the pristine original (commit `1248ff8`), 150 s cap:

```
  tinisat (-m 0)   wall=  3.3s   rc=0     -> terminates, solution written
  minisat (-m 4)   wall=151.1s   rc=124   -> NEVER terminated (hit the timeout)
```

`-g` (master sub-mode: 1=BDD/0=Table) and `-q` (minisat clause carryover) are
orthogonal to the bug — they just mirror the original mode-4 test template. The
hang is purely minisat's non-yielding `solve()`.

## Why

- **tinisat** returns code 2 ("paused") every `sat_reporting_time` decisions, so the
  worker on node 1 checks in, the master sees the node-0 solution is already found,
  and the run exits — abandoning the pigeonhole mid-solve.
- **minisat** (`MinisatSolver::run`) runs `solve()` to completion and returns only
  0/1 — never yields. Its worker never checks in, and the master's *poll-all-workers-
  before-exit* loop blocks until the pigeonhole finishes. The node-0 answer was found
  in milliseconds, but the process can't finish (and never writes its output, since
  that happens only after the master loop returns). The same was true of cadical and
  the IPASIR backends before the fix.

## The fix

`--backend <b> --yield-seconds S` (default 30; 0 = old run-to-completion behaviour)
makes every backend yield a long solve back to the worker after `S` wall-clock
seconds, so the worker can poll the master and be reassigned or killed:

```sh
mpirun -n 3 --oversubscribe ./dagster --backend minisat            -e 0 demo.dag demo.cnf -o out.sols
mpirun -n 3 --oversubscribe ./dagster --backend minisat --yield-seconds 0 -e 0 demo.dag demo.cnf -o out.sols
```

Measured on the fixed build (same instance, 90 s cap):

```
  minisat --yield-seconds 0  (old behaviour)   wall= 91.4s   rc=124   HANG
  minisat  (default yield 30s)                 wall= 32.9s   rc=0     terminates
  minisat --yield-seconds 3  (snappier)        wall=  4.7s   rc=0     terminates
```

So the same instance reproduces the bug (`--yield-seconds 0`) and confirms the fix
(any positive yield) in one place. Termination latency ≈ the yield interval.

Every backend now yields: tinisat (native `-j`), cadical (`Terminator`),
minisat/glucose/maple (conflict-budget chunking), lingeling (`lglseterm`),
cryptominisat (`set_max_confl`). See `utilities/cube/README.md` and the commits
`8f45b4a`, `985d771`, `ff621ad`.
