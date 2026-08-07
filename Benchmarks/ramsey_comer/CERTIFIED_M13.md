# Machine-checked result: no small Cayley representation of the 13-colour Ramsey algebra

## Theorem (machine-checked)

**The 13-colour Ramsey (Monk) relation algebra has no vertex-transitive (Cayley)
representation on any group of order ≤ 48.**

Established by exhaustively certifying, for **every one of the 223 groups** of each
order 15–48 (GAP SmallGroups library — the complete enumeration), that the
Comer/Cayley representation CNF is **UNSAT**, with each UNSAT backed by a **DRAT
proof checked by `drat-trim`**. Run: 223 groups, 223 certified UNSAT, 223 proofs
`VERIFIED`, 0 anomalies (`cert_m13_15-48.log`, ~17 min wall).

Reproduce: `python3 certify.py -M 13 --range 15 48`  (add `--airtight` for the
symmetry-break-free variant; see the M=8 note for the trade-off).

## Relationship to the M=8 result

This is the exact companion of `CERTIFIED.md` (the M=8 theorem) — same encoder
(`comer.py`), same certifier (`certify.py`), same exhaustive GAP catalogue, same
two soundness dependencies. Everything in `CERTIFIED.md` under *"What 'Cayley
representation' means"*, *"The two things the theorem rests on"*, and *"Scope /
what this does NOT claim"* applies verbatim with 8 replaced by 13; only the colour
count and the numbers below differ. Read `CERTIFIED.md` for the full argument.

## Why M=13 is *cheaper* than M=8 here despite bigger CNFs

The M=13 instances are ~2.6× larger than M=8 (e.g. order 48: 197k vars / 403k
clauses vs 78k / 160k) yet the full sweep ran **faster** (1037 s vs 2249 s). With
13 colours but only ≤ 48 elements, the flexibility requirement (every non-mono
colour-triangle realised over every edge) is wildly over-constrained — there are
nowhere near enough elements to host 13 mutually-flexible colour classes — so UNSAT
falls out almost immediately (sub-second solves even at order 48). Note also that a
representation needs ≥ 13 symmetric colour classes, i.e. (n−1)/2 ≥ 13, so every
order < 27 is trivially UNSAT (too few classes to use all 13 colours); those orders
are included for a clean "order ≤ 48" statement.

## Scope / what this does NOT claim

Identical to the M=8 case: **vertex-transitive (Cayley) only** (a representation
with no group symmetry would be invisible at any order), and **order ≤ 48 only** (a
Cayley representation could exist at larger order; the cyclic sizes 13/41/71 for
M=3/4/5 suggest the relevant order for an open M, if any, is likely well above 48 —
pushing higher needs Kissat + HPC). SAT can only ever *find* a representation, never
rule one out; this result is the ruling-out direction, made rigorous.

## Artifacts

- `cert_m13_15-48.log` — the per-group verification log (223 × `VERIFIED`).
- `proofs_m13/*.drat` — the individual DRAT proofs (git-ignored; regenerable with
  `certify.py --proofs proofs_m13`). Re-check any one with
  `drat-trim <regenerated cnf> proofs_m13/<group>.drat`.
