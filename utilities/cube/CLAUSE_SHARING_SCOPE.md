# Detailed scope: clause sharing between conquer workers

> **STATUS — phase 1 BUILT & VERIFIED.** `-m 10` / `--share` (CaDiCaL only) is
> implemented and merged: `clause_share/{ClauseChannel,ClauseHub}`, a
> `CaDiCaL::Learner` exporter + between-cube import in `CadicalSolver`, the
> persistent-solver lifecycle in `Worker`, and the `share_execute` star topology
> in `main.cpp` (master + N workers + 1 hub). Exposed via `solve.py --share` and
> benchmarked by `tests/cube_matrix/matrix.py --modes 5,10`.
> Verification: hub relay confirmed (pigeonhole7, 70 cubes — received 102, 100
> unique, relayed 200; workers imported 55×); **UNSAT verdict parity** (`-m 5` vs
> `-m 10`); **SAT parity** (sudoku, 64 cubes — byte-identical solution, validated
> against all 3241 clauses); edge cases (n=3 min topology, n=2 guard) and 3×
> stability all pass. Phase 2 (during-solve import via `ExternalPropagator`,
> LBD/adaptive filtering) remains future work; §5 below is the original plan.



**Goal.** In cube-and-conquer, N workers each solve `formula ∧ cube_i` independently.
A clause learned by one worker that is implied by `formula` helps *every* worker.
Sharing such clauses is the standard way parallel SAT / cube-and-conquer scales
beyond embarrassing parallelism; today Dagster's workers learn in isolation.

---

## 1. Soundness — settled (and simpler than first thought)

**Claim: CDCL learned clauses are entailed by the formula, independent of the cube,
so sharing them among workers solving the *same* formula is sound.**

Why: conflict analysis derives the learned clause by resolving **clauses** (the
input formula + previously-learned clauses, which are themselves formula-entailed
by induction). The cube is applied as **assumption decision literals on the trail**
— *not* as clauses in the database — so it is never an antecedent of a resolution
step. Hence every `learn()`-callback clause is a logical consequence of `formula`
alone. (This is the very invariant that lets incremental SAT keep learned clauses
across `assume`/`solve` calls.)

The one assumption-specific artifact — the *failed-assumptions* clause `¬(⋀ failed
assumptions)` — is **not** emitted via the `Learner` callback; it is surfaced
through the solve result / `failed()`. So we never see it on the sharing path.

Consequences for the design:
- **No cube-guard needed.** (An earlier draft proposed sharing `(¬cube ∨ L)`; that
  is both unnecessary *and* useless here — cubes partition the same split
  variables, so a cube-guarded clause is trivially satisfied by every *other*
  cube and never fires. Share `L` directly instead.)
- **Applies to one shared formula.** Cube-and-conquer is the clean case: all
  workers solve the single conquer node's formula. In a *multi-node* DAG each node
  has a *different* sub-CNF, so sharing is only sound **between workers on the same
  node**, never across different nodes. ⇒ scope this to the conquer phase (and,
  later, same-node sharing in decomposition).

Residual risks (engineering, not logic), mitigated by gating on the
`tests/cube_matrix` verdict-parity harness:
- Inprocessing-derived clauses involving *eliminated/extension* variables (BVE,
  etc.) are equisatisfiable, not always entailed by the original formula. The
  `Learner` is documented to emit *learned (conflict)* clauses, but to be safe:
  share only clauses over the **original variables** (drop any clause containing a
  variable id beyond the formula's `max_var`), and verify empirically.

---

## 2. Architecture — a clause hub (star), reusing the strengthener substrate

Mirror the existing strengthener wiring, but as a **star** (all workers ↔ one hub)
rather than the strengthener's per-worker pairs, so clauses propagate *between*
workers.

```
            master (rank 0)  --cubes/assignments-->  workers           (existing master comm)
                                                        │
  worker_i (CaDiCaL + Learner) --push learned (≤K, orig-vars)-->  CLAUSE HUB
                               <--import shared (deduped)--------       (new clause comm, MpiBuffer)
```

Three communicators (as the SLS/strengthener modes already do via `MPI_Comm_split`):
1. **master ↔ workers** — assignments/cubes (unchanged).
2. **workers ↔ hub** — clause exchange. Each worker has one `MpiBuffer` to the hub;
   the hub holds one `MpiBuffer` per worker (or `getClause` via `MPI_ANY_SOURCE`).
   The hub is *not* in the master comm; it only relays clauses.

World layout: `master(1) + N workers + 1 hub`. (A worker is in both the master comm
and the clause comm; the hub only in the clause comm — exactly how SLS helpers sit
only in the SLS comm.)

### Components

**(a) `ClauseChannel`** — reusable CaDiCaL-side wrapper (analogous to `SlsChannel`),
owning an `MpiBuffer` to the hub. Methods:
- `export_clause(int* lits, int n)` — push a learned clause to the hub (size-filtered, original-vars-only).
- `import_clauses(std::function<void(int*,int)> add)` — drain `getClause()` from the hub, calling `add` for each (the solver's `add`).
Implemented over the existing `MpiBuffer` (push/getClause/readyToSend) — no new transport.

**(b) `CadicalSolver` hooks.**
- *Export*: implement `CaDiCaL::Learner`:
  - `bool learning(int size)` → `return size <= K && channel != NULL;` (K≈8; the size filter is the main quality/volume knob).
  - `void learn(int lit)` → accumulate into a buffer; on `lit==0`, if all vars ≤ formula `max_var`, `channel->export_clause(buf, len)`.
  - install with `solver->connect_learner(this)` in the SLS-style ctor variant.
- *Import* (two options):
  - **MVP — between cubes**: in `run()`, before `solve()`, call
    `channel->import_clauses([&](int* c,int n){ for(i)solver->add(c[i]); solver->add(0); })`.
    Sound and simple; the conquer solver is incremental so imported clauses persist
    and compound. Latency = one cube; fine when cubes are many/short.
  - **Full — during solve**: a `CaDiCaL::ExternalPropagator` whose
    `cb_add_external_clause_lit` feeds shared clauses mid-solve. Needed when cubes
    are few/long. More complex (IPASIR-UP); a follow-up.

**(c) Clause hub process** — like `strengthener_surrogate_main`/`SolRed`, but it
*relays* instead of *reduces*:
```
receive node CNF from master (for max_var bound)         // like the reducer
seen = hash set of clause signatures
loop until kill:
    for each worker w: while (cw = buffers[w].getClause()): 
        sig = signature(cw->clause)
        if sig not in seen: seen.add(sig); for each other worker w': buffers[w'].pushClause(cw->clause)
```
Dedup keeps volume down; an optional cap on `|seen|` / LRU bounds memory.

**(d) Topology + flag.** A new mode (e.g. `-m 10` "cadical + clause sharing") or a
`--share` flag composable with `--cubes`, that does the 3-way `MPI_Comm_split`
(master / workers / hub) and constructs `CadicalSolver` with a `ClauseChannel`.
Mirror `sls_execute`'s split; the hub is the analogue of the gnovelty helper.

---

## 3. Backend coverage
- **CaDiCaL** — clean (`connect_learner` + `add`/`ExternalPropagator`). The target.
- **tinisat** — already has `push_to_reducer`/`get_from_reducer`; could join the
  same hub, but lower priority (tinisat is the weak backend).
- **CryptoMiniSat** — no public learned-clause export (same gap as SLS) → out.
- **MiniSat** — would need a source hook → out.

---

## 4. Engineering knobs & risks (the real work)
- **Size filter K** (≈ 6–10): the dominant quality/volume lever; share only short,
  high-value clauses (LBD/size). Start with size ≤ 8.
- **Volume / dedup**: hub-side dedup by clause signature; rate-limit pushes
  (`MpiBuffer` is already batched + bounded). Risk: flooding workers with low-value
  clauses → slowdown. Mitigate with K and dedup.
- **Deadlock**: clause exchange must be non-blocking (the `MpiBuffer` already uses
  Isend/Irecv + double buffering — same pattern as the working strengthener and
  `SlsChannel`). Workers poll the hub between cubes (MVP) — no collective op, so
  no lockstep requirement (unlike the SLS window). Lower deadlock risk than SLS.
- **Per-cube solver lifetime**: under `--cubes` the conquer `CadicalSolver` is kept
  incremental across cubes, so the `Learner` + imported clauses accumulate. Confirm
  the Worker keeps it (it does for non-SLS incremental backends).
- **Checkpoint interaction**: shared clauses are just learned clauses in each
  worker's local DB — not master state — so checkpoint/resume is unaffected
  (workers rebuild their DB on resume; sharing simply restarts). No new soundness
  issue.

---

## 5. Effort & phasing

| phase | piece | effort | risk |
|---|---|---|---|
| 1 | `ClauseChannel` (MpiBuffer wrapper) | low | low (reuses transport) |
| 1 | `CadicalSolver` `Learner` export + between-cube `add` import | low–med | low |
| 1 | clause hub process (relay + dedup) | med | low–med (relay loop, like the reducer) |
| 1 | topology/mode wiring (3-way split) | med | med (MPI split correctness — mirror SLS) |
| 1 | gate on `cube_matrix` verdict-parity + measure on hard UNSAT | — | — |
| 2 | during-solve import via `ExternalPropagator` | med–high | med (IPASIR-UP) |
| 2 | LBD-based filtering, adaptive K, memory caps | med | low |

**MVP (phase 1)** is a coherent, sound, testable unit: CaDiCaL workers export
size-filtered learned clauses to a dedup hub and import them between cubes.

**Expected benefit:** large on hard UNSAT (cubes that share structure benefit from
each other's lemmas); modest/neutral on easy or SAT-race instances. Measure with a
`--share`-axis added to `tests/cube_matrix` (UNSAT instances, with vs without
sharing, on enough cores that workers actually run concurrently — i.e. the HPC).

---

## 6. Recommendation
Build **phase 1, CaDiCaL-only**, composed with `--cubes`. It's sound (§1), reuses
the `MpiBuffer` transport and the SLS-style topology split, and the deadlock
surface is smaller than SLS (no collective window). Gate every step on
`tests/cube_matrix`. Defer during-solve import and fancy filtering to phase 2 once
phase 1 shows a measurable win on the HPC.
