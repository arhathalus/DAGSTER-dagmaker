# Machine-checked result: no small Cayley representation of the 8-colour Ramsey algebra

## Theorem (machine-checked)

**The 8-colour Ramsey (Monk) relation algebra has no vertex-transitive (Cayley)
representation on any group of order ≤ 48.**

Established by exhaustively certifying, for **every one of the 223 groups** of each
order 15–48 (GAP SmallGroups library — the complete enumeration), that the
Comer/Cayley representation CNF is **UNSAT**, with each UNSAT backed by a **DRAT
proof checked by `drat-trim`**.

Reproduce: `python3 certify.py -M 8 --range 15 48`  (add `--airtight` for the
symmetry-break-free variant; see below).

## What "Cayley representation" means here

Vertices = elements of a finite group G (order n); edge {x,y} gets the colour of
the difference x⁻¹y; the colour map c: G\{e} → {1..8} is symmetric (c(g)=c(g⁻¹)).
A valid such colouring (no monochromatic triangle + every non-mono colour-triangle
realised over every edge) is a *vertex-transitive* representation of the algebra on
n points. This is the generalised-Comer-scheme search that is the state of the art
for these algebras (Alm/Andrews/Levet 2024). See `comer.py` / `STRATEGY.md`.

## The two things the theorem rests on (besides the checked proofs)

1. **Encoder faithfulness** — that the CNF really encodes "a Cayley representation
   on G exists". Established independently of the solver: `comer.py`'s `verify()`
   re-checks any model against the definition without reusing the encoder, and the
   encoder **reproduces the known cyclic representations** (M=3 → Z₁₃, M=4 → Z₄₁,
   M=5 → Z₇₁), each verified. Group inputs are checked to satisfy the group axioms
   (`check_group`) so an encoding bug can't masquerade as a maths result.

2. **Value-precedence lemma** (default mode only) — the CNF is solved with colour
   value-precedence symmetry breaking, so the certified UNSAT means "no
   *precedence-ordered* colouring exists". Value precedence is **verdict-preserving**
   for the 8 fully-interchangeable colours: any colour permutation maps a
   representation to a representation, so every orbit that contains a representation
   contains its lex-least (precedence-ordered) member; hence the broken CNF is SAT
   iff the unbroken one is (Crawford et al.; Law & Lee, *value precedence*). So
   UNSAT of the broken CNF ⇒ no representation.

   **`--airtight` removes this dependency** by solving the *unbroken* CNF, so the
   UNSAT rests on nothing but the checked proof. It is verified feasible for the
   vast majority of the 223 groups (proofs up to ~60 MB), but a few highly-symmetric
   groups (e.g. the order-32 modular group) don't solve in reasonable time without
   the break — which is exactly why the default uses it.

## Scope / what this does NOT claim

- **Vertex-transitive only.** The Cayley search is sound but *incomplete*: a
  representation with no group symmetry would be invisible to it at any order. The
  known representations for the solved M are all Cayley, and the SOTA method is
  vertex-transitive, so this is the right search — but the result is "no *Cayley*
  representation ≤ 48", not "no representation".
- **Order ≤ 48 only.** A Cayley representation could still exist at a larger order
  (the cyclic sizes 13/41/71 for M=3/4/5 suggest the relevant order for M=8, if any,
  may be well above 48). Pushing higher needs a stronger solver (Kissat) + HPC.
- It says nothing about **M=13** (run `certify.py -M 13 ...` for that).

## Relation to the literature

Previously only **cyclic** (abelian, finite-field) representations were known to be
ruled out for M=8 (no prime ≡ 1 mod 16 below 8⁴+5). This extends that to **all
group-based (every non-abelian included) representations up to order 48**,
exhaustively and with checked proofs — a genuine, if partial, machine-checked
contribution. See `STRATEGY.md` for references and the full plan.

## Artifacts

- `cert_m8_15-48.log` — the per-group verification log (223 × `VERIFIED`).
- `proofs_m8/*.drat` — the individual DRAT proofs (git-ignored; regenerable with
  `certify.py --proofs proofs_m8`). Re-check any one with
  `drat-trim <regenerated cnf> proofs_m8/<group>.drat`.
