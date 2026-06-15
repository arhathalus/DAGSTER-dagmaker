# Scope: machine-checkable UNSAT proofs (DRAT/LRAT) for Dagster

> **STATUS — Milestone 0 DONE (2026-06).** `dagster --backend cadical --proof FILE`
> emits a per-worker DRAT proof (CaDiCaL `trace_proof`, opened in CONFIGURING;
> closed on teardown). A DRAT checker (`drat-trim`, vendored from CaDiCaL's test
> tree) is built under `utilities/proof/`, and `utilities/proof/certify.py` is the
> turnkey single-node certifier (solve → emit DRAT → drat-trim). VERIFIED end-to-end
> on `ramsey_6_3` and a clean PHP (UNSAT → `s VERIFIED`); SAT short-circuits (model
> is the certificate). `--proof` is guarded CaDiCaL-only and rejects `--share`/`--sls`.
> This certifies the corpus's UNSAT labels independently of Dagster's machinery.
>
> **STATUS — Milestone 1 DONE (2026-06).** The cube-split **tautology proof** is
> `utilities/proof/tautology.py`. KEY FINDING that resolves the "march gap" below:
> march's cubes are **exhaustive** (verified across php/ramsey at many depths — the
> sub-2^d counts are adaptive-depth cutoff, not refutation-dropping), so we don't
> need march to emit a tautology proof — the cubes are exhaustive **iff the
> negated-cubes CNF `⋀_i ¬Cᵢ` is UNSAT**, and CaDiCaL refutes that and emits the
> proof (checked by drat-trim, via `certify.py`). EXHAUSTIVE → tautology proof
> VERIFIED (exit 0); a non-exhaustive set → drat-trim returns a model = an uncovered
> assignment (exit 1). So §3b's "trickiest missing piece" is solved without touching
> march.
>
> **STATUS — Milestone 2 DONE (2026-06).** Full cube-and-conquer certification is
> `utilities/proof/cc_certify.py formula.cnf cubes.icnf`: (1) certify every
> `formula ∧ Cᵢ` UNSAT with a drat-trim-checked proof (certify.py, cube literals as
> unit clauses) and (2) certify the cube set exhaustive (tautology.py) ⟹ formula
> UNSAT. Every clause-level step is drat-trim-checked; the only thing outside the
> checker is the one-line C&C meta-inference (1)+(2)⟹UNSAT (this per-cube *bundle* is
> what large-scale C&C certification, e.g. Pythagorean, actually uses, and fits
> Dagster's distributed conquer). VERIFIED on php8 (8 cubes refuted + exhaustive →
> "UNSAT CERTIFIED"); SAT-witness path (a cube extending to a model ⟹ formula SAT)
> and `--max-cubes` cap tested. cc_certify re-solves each cube offline (solve fast,
> certify after) — inherently expensive at scale (proof IO), like any C&C proof.
>
> **REMAINING refinement (optional):** a *single monolithic* DRAT inlining even the
> meta-inference (concat per-cube `formula ⊢ ¬Cᵢ` segments + tautology, one drat-trim
> check vs the formula). Needs CaDiCaL **assumption-mode** proof emission (solve under
> cube *assumptions* so lemmas stay formula-entailed and the segment concludes `¬Cᵢ`);
> the unit-clause bundle can't be concatenated soundly because lemmas learned with the
> cube as units aren't formula-entailed. Plumbing `assume`+`conclude` proof output into
> `CadicalSolver` is the work. The bundle is a complete, rigorous certificate today.



**Goal.** Turn Dagster's UNSAT verdicts into *theorems*. Today an UNSAT result is
solver-trust: it relies on the backend, Dagster's DAG/cube machinery, and (when
used) symmetry breaking all being correct. For the research targets — lower bounds
on the open ramsey relation algebras (n=8, n=13) and the domatic n=10 case — and to
*validate the symmetry breaking* the generators emit, an UNSAT answer is only
worth something if it ships a proof an independent checker accepts.

SAT is already certified: the model is the certificate, and the corpus already
validates models clause-by-clause. **This document is entirely about UNSAT.**

---

## 1. Background — what a proof is

- **DRAT**: a trace of clause additions (RUP/RAT steps) and deletions. A checker
  (`drat-trim`) replays it and confirms it derives the empty clause from the
  formula ⇒ UNSAT. The de-facto standard (SAT Competition since 2013).
- **LRAT**: DRAT augmented with antecedent hints, so a *formally verified* checker
  (`cake_lpr`) checks it in near-linear time. Produced from DRAT by `gratgen`, or
  directly by CaDiCaL.
- **What we have**: CaDiCaL (our `-m 5` backend) can emit both —
  `trace_proof(path)` for DRAT, `connect_proof_tracer(tracer, antecedents,…)` for
  LRAT. Both must be enabled in CaDiCaL's CONFIGURING state (before any clause),
  exactly like the inprocessing/`factor` options — so a ctor flag, as we already
  do for `--inprocess`.
- **What we lack**: a checker (none on PATH — vendor `drat-trim`, ~one C file), and
  the *composition* logic that turns many distributed sub-proofs into one global
  proof.

---

## 2. The hard part — Dagster proofs are *composed*, not flat

A single solver emits a flat DRAT. Dagster's UNSAT is not one solver's run; it is a
*composition*. Two regimes, very different difficulty:

### Cube-and-conquer (the tractable, target-relevant case)
This is the well-trodden path (Heule, Kullmann, Marek — Boolean Pythagorean Triples).
The global proof is a **concatenation**, in this order:

1. **transformation proof** — justifies any preprocessing that changed the formula
   (BCP / pure-literal elim / symmetry breaking) before cubing;
2. **cube proofs** — for each cube *i*, a proof that `formula ⊢ ¬cube_i` (i.e. the
   conquer solver's UNSAT-under-assumptions run);
3. **tautology proof** — that the cube set is *exhaustive* (the negated cubes plus
   the formula are jointly unsatisfiable; the split covers the whole space).

`drat-trim` checks the concatenation. (For scale, the Pythagorean proof was ~200 TB
DRAT / 68 GB compressed — proof size is a first-class concern; see §6.)

### DAG decomposition (research; defer)
A general Dagster DAG solves a *different* sub-CNF per node and combines partial
assignments across vertex separators. Composing node proofs into a global proof is
**not** the standard cube-and-conquer pattern and has no off-the-shelf tooling — it
needs a custom meta-proof. **Out of scope for the first cut.**

---

## 3. Architecture — Phase A (cube-and-conquer, CaDiCaL)

A `--proof DIR` mode, composable with `--cubes`:

**(a) Per-cube proofs.** `CadicalSolver` gains a proof-trace ctor flag (enable
`trace_proof` in CONFIGURING). Under `--proof`, each cube's conquer run writes a
self-contained DRAT segment establishing `¬cube_i`, to a per-cube file on the
worker's local disk (named by cube id, recorded in a manifest — mirror the
`cells.tsv`/collector pattern we built for the benchmark harnesses).

- **Proof mode is restrictive on purpose** (first cut):
  - *fresh CaDiCaL per cube* (non-incremental) so each segment is self-contained —
    the incremental path's proof is cumulative and hard to segment;
  - *clause sharing OFF* — an imported clause a worker didn't derive breaks its
    segment's self-containment;
  - *symmetry breaking OFF* — see §5.
  Proof mode trades Dagster's speed levers for certifiability; that's the deal.

**(b) Tautology proof.** The march cube tree is a complete DPLL-style split, so a
tautology proof is reconstructible from it — **but our stripped `march_cu` does not
emit one** (confirmed: its "tautology" code is equivalence-clause cleanup, not the
cube-split proof). This is the **trickiest missing piece**: either teach `march_cu`
to emit the split proof, vendor the original CnC proof tool, or reconstruct it from
the `.icnf` (the cubes + the split variables determine it).

**(c) Transformation proof.** If `cube.py` sanitised/BCP/PLE'd the formula, that
step must be proof-justified (BCP/PLE are RUP-easy). Symmetry breaking is *not*
(§5) → proof mode runs without it for now.

**(d) Collect + check.** A `proof_collect` step concatenates `transformation ++
cubes ++ tautology` in order and runs the checker. Vendor `drat-trim` first (simple
DRAT); upgrade to `gratgen → cake_lpr` (verified LRAT) once the pipeline works.

---

## 4. Milestones (each independently checkable)

0. **Single-formula DRAT** — no DAG, no cubes: run CaDiCaL on a whole UNSAT CNF with
   `trace_proof`, check with `drat-trim`. Validates the CaDiCaL plumbing + the
   vendored checker on a tiny instance (e.g. `ramsey_6_3`, PHP). *Smallest useful
   step; do this first.*
1. **Cube-split tautology proof** — emit/reconstruct the exhaustiveness proof for a
   march `.icnf`; check that `formula ++ tautology` is itself refuted.
2. **Full cube-and-conquer composition** — per-cube DRAT from the conquer workers +
   tautology + transformation, concatenated, `drat-trim` PASS, on a small
   known-UNSAT (`ramsey_6_3`, a small PHP). End-to-end Phase A.
3. **LRAT upgrade** — verified checking via `cake_lpr` (near-linear, trustworthy).
4. **Scale** — run on a real target; plan proof IO (per-worker local disk, parallel
   / LRAT checking).

---

## 5. Caveats (the real risks, in priority order)

- **Symmetry breaking ≠ DRAT.** Lex-leader / BreakID clauses are
  *satisfiability-preserving*, not *entailment-preserving* — they are **not**
  RUP/RAT-derivable, so plain DRAT cannot justify them. Certifying them needs **PR
  or SR proofs** and a checker that supports them (an active research area). ⇒ proof
  mode **disables symmetry breaking** initially. This directly answers the open
  "is the generator's symmetry breaking sound?" question: until we have certified
  SB, a rigorous lower bound must be proved on the *unbroken* formula (bigger
  search, but sound). Certified SB is **Phase B**.
- **Clause sharing** breaks per-worker proof self-containment ⇒ OFF in proof mode.
- **Incrementality**: fresh solver per cube for clean segments ⇒ slower; the cost of
  certifiability. (Keeping incremental speed *and* proofs is Phase B.)
- **Proof size/IO** is the dominant practical constraint at scale (10s of TB
  possible). Mitigate with per-worker local-disk segments, LRAT, and parallel
  checking — but it bounds which targets are certifiable in practice.
- **The tautology proof** for the march cube set is the main missing component in
  the current toolchain (§3b).

---

## 6. Effort & recommendation

| component | effort | risk |
|---|---|---|
| CadicalSolver `trace_proof` ctor flag | low | low |
| `--proof` mode wiring (fresh/cube, no share, no SB) | low–med | low |
| per-cube proof files + manifest + `proof_collect` | med | low (mirrors collect.py) |
| cube-split **tautology proof** (march gap) | med–high | med (the crux) |
| transformation proof (BCP/PLE) | med | low–med |
| vendor + wire `drat-trim`; later `gratgen`/`cake_lpr` | low | low |

**Recommendation.** Build **Milestone 0 first** (single-formula CaDiCaL DRAT +
vendored `drat-trim`, on `ramsey_6_3`) — it's a day of work, proves the backend
plumbing + checker, and immediately lets us *certify the corpus's UNSAT labels*
(closing the loop on trust). Then tackle the **tautology proof** (the crux) to
unlock full cube-and-conquer certification (Milestone 2). Keep symmetry breaking and
clause sharing **off** in proof mode for the first cut; certified SB and
proof-carrying sharing are Phase B. The general DAG-decomposition proof is a
separate research effort — not on this path.

This makes UNSAT results theorems exactly where it matters: the open ramsey/domatic
lower bounds, and validating that the symmetry breaking we rely on is sound.
