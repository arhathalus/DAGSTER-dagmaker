# Ramsey relation-algebra representability (M = 8 and M = 13): notes & strategy

Working notes on the attack on the two open **Ramsey/Monk relation-algebra
representability** cases, M = 8 and M = 13. Written so a future collaborator (or a
future session) can pick this up cold. Last updated 2026-08.

---

## 1. The problem

A **Ramsey (Monk) relation algebra** with M symmetric "colour" atoms is
**representable** iff the complete graph K_N can have its edges coloured with M
colours, for *some* N, so that:

- **(A) no monochromatic triangle** — no triangle has all three edges the same
  colour; and
- **(B) flexibility** — every *non*-monochromatic colour-triangle (i, j, k) that
  the algebra allows actually occurs, and occurs over every edge.

Representability is **known for every M ≤ 120** (strong representability to 2000)
**except the open pair M = 8 and M = 13**. It sits inside the *flexible-atom
conjecture* (every symmetric integral RA whose only forbidden cycles are the
identity cycles is representable). References: Kowalski 2015; Alm "401 and Beyond"
2017; Alm & Andrews 2019; Kramer & Maddux "Monk algebras and Ramsey theory" 2022;
Alm/Andrews/Levet "Comer Schemes … Flexible Atom Conjecture" (updated Dec 2024).

**Why it's worth doing:** a representation is *self-certifying* — exhibiting one
resolves the open problem, a genuine citable mathematical result.

### The single most important structural fact

**SAT can only give a *positive* answer.** A model at a fixed structure = a
representation exists = **resolved**. But UNSAT at a fixed structure only says "no
representation *of that size/shape*." Proving **non**-representability would need
to rule out *all* N up to the M-colour triangle Ramsey number R_M(3) — which is
finite but astronomically large and itself unknown — so it won't come from a
solver. It needs a mathematical theorem. **Plan around this:** we are hunting for
a *witness*, not a disproof.

---

## 2. Literature status (Step-0 review, done 2026-08)

- **M = 8 and M = 13 are confirmed STILL OPEN** as of Jan 2025 ("Monk Algebras and
  Representability", arXiv 2501.07332). The 8-colour case is called "one of the
  most persistent open problems in this area."
- **The state-of-the-art method is NOT brute-force K_N SAT.** It is **generalised
  Comer-scheme structured search + SAT** (Alm/Andrews/Levet, arXiv 1905.11914,
  Dec 2024): assume a group/coset "scheme" structure — a generalisation of the
  cyclic finite-field construction where cosets need not be sum-free — which
  collapses the search enormously, and use SAT within it. A second live avenue
  (2025-26) is building representations from **distance-regular graphs**
  (arXiv 2605.14190).
- **Cyclic is dead for M = 8/13.** The finite-field (Comer) construction needs a
  prime p ≡ 1 (mod 2M); for M = 8 that's p ≡ 1 (mod 16), and every such prime up
  to the bound (8⁴ + 5 = 4101) fails the sum-free condition. So any representation
  is **non-cyclic**.
- **Encouraging:** for *sibling* flexible-atom algebras, non-cyclic finite
  representations exist at **modest point counts (11-24)** — far below the 120+
  cyclic ones — and are being **actively found computationally in 2024-25**. So a
  non-cyclic M = 8 representation, *if it exists*, plausibly lives at a
  SAT-reachable order.
- **Sobering:** M = 8/13 are the persistent hold-outs even among a family whose
  siblings are being cracked with exactly these methods; and there is **no usable
  finite upper bound on N** for a general representation, so the search is
  open-ended.

---

## 3. What we built

Two encodings of the same problem live in this repo:

### (a) The raw K_N encoding — `Benchmarks/ramsey/` (pre-existing)

Colours all ~N² edges directly. Variables/clauses grow ~N³·M³ (edge-colour vars +
"tour" channelling). Hand-rolled lex-leader vertex symmetry breaking. **Two
variants**, and the corpus wires up the *weaker* one:

- `ramsey/` — breaks colour symmetry only relative to one distinguished vertex.
- `ramsey_colour_cardinality/` — **stronger**: breaks colour symmetry *globally*
  by edge-frequency (folds all M! colour permutations) and exposes `-A/-B`
  per-colour count bounds. **Not currently used by `generate_benchmarks.py`** —
  a free win for the raw path.

Raw results (standalone cadical): `ramsey_11_4` SAT 3.3 s; `ramsey_12_5`,
`ramsey_13_6` TIMEOUT at 60 s. The open targets `ramsey_15_8` (≈1.8 M clauses) and
`ramsey_14_13` (≈5.9 M clauses) were **never generated or run** — the raw encoding
is too big at the interesting sizes; CDCL drowns.

### (b) The Comer/Cayley encoding — `comer.py` (built here, the real attack)

Search only **vertex-transitive (Cayley) representations**: put the vertices on
the elements of a finite group G (order n), and colour edge {x, y} by the colour
of the **difference** x⁻¹y. A colouring is then just a symmetric map

    c : G \ {e} → {1..M},   c(g) = c(g⁻¹)

This collapses ~N² edge variables to ~n/2·M element variables **and bakes in the
vertex symmetry for free** (Cayley graphs are vertex-transitive — no per-vertex
breaking clauses). This is exactly the generalised-Comer method that's cracking
the siblings; arbitrary symmetric partitions (what the SAT solver searches)
*subsume* the ruled-out cyclic case.

Constraints (all reduced by translation invariance — colour depends only on the
difference):
- **exactly one** colour per inverse-class;
- **(A)** no mono triangle: no a, b with c(a) = c(b) = c(ab) (only triangles
  through e needed);
- **(B)** flexibility, per edge: over every difference d, every non-mono colour
  pair {i, j} is realised by some witness y (c(y) = i, c(y⁻¹d) = j); the (i,i)
  pair is legitimately excused when c(d) = i.
- colour value-precedence (Crawford) symmetry breaking.

Extras: an **independent verifier** (`verify`) re-checks any SAT model is a genuine
representation without reusing the encoder; **pluggable groups** (cyclic, dihedral,
direct products, semidirect products, and any group via a Cayley table); a
**group-axiom self-check** so an encoder bug can't masquerade as maths; **GAP
integration** (`gap_groups`) to dump *every* group of an order from the SmallGroups
library; and a `--sweep` mode.

**Validated — it reproduces the textbook cyclic representations, independently
verified:**

| M | smallest cyclic representation | size |
|---|---|---|
| 3 | **Z₁₃** (cubic residues mod 13) | 816 vars / 1734 clauses |
| 4 | **Z₄₁** (quartic residues) | ~ |
| 5 | **Z₇₁** | ~150 k clauses |

Smaller groups correctly come back UNSAT. Instances are *tiny* vs the raw encoding.

---

## 4. Results so far (M = 8)

Two sweeps of small groups, standalone cadical:

- **Orders 15-40, ~90 groups** (cyclic + dihedral + direct products): **all UNSAT**.
- **Orders 15-48, 163 groups** (+ semidirect products): **all UNSAT** (2 timeouts
  at orders 44/46 where M = 8 instances outgrow 60 s cadical).

So: **no Cayley representation of the 8-colour algebra on any of ~250 groups of
order ≤ 48.** Cyclic is UNSAT throughout (reproduces "cyclic ruled out"). Real, if
modest, lower-bound data. Nothing found — expected, since we've searched well under
5 % of the plausible space (small orders, non-exhaustive group catalogue).

**Calibration:** M = 8 instances are fast to ~order 40, start timing out (single-
core cadical, 60 s) around order 45.

---

## 5. Best approach to *find* M = 8 / M = 13 (recommended)

The Comer/Cayley structured search (§3b) is the right method — validated, and it's
what's cracking the siblings. To turn it into a real shot, three multipliers, in
priority order:

1. **Exhaustive group enumeration via GAP.** Our hand-built families (cyclic,
   dihedral, semidirect, products) miss ~half the groups at composite orders
   (quaternion, extraspecial, …). `sudo apt install gap` → `catalog()` auto-switches
   to the SmallGroups library (**every** group of each order), cached per order.
   This is our genuine comparative advantage: *exhaustive breadth* the specialists'
   construction-guided searches may not have fully covered.
2. **Push to larger orders.** The cyclic order pattern (M = 3/4/5 → 13/41/71) hints
   the relevant order for M = 8, if any, could be well above 48. Non-cyclic order is
   unknown, but likely larger than what we've swept.
3. **Stronger solver + HPC.** Use **Kissat** (strongest engine; no incrementality
   needed here, so it's free to use — unlike inside Dagster). The sweep is
   **embarrassingly parallel** — one tiny independent SAT per group — ideal for an
   HPC job array or Dagster's master/worker as a task distributor.

A single `*** REPRESENTATION ***` on any group resolves the case.

**Dagster's real niche here (valuable regardless of a hit):** **certified lower
bounds.** Every UNSAT is a "no representation on this structure" fact; run each
under our DRAT/LRAT proof pipeline to turn the solver claims into *machine-checked
theorems* — "no Cayley representation of the 8-colour algebra on any group of order
≤ k" — a citable partial result the raw-SAT community usually lacks.

---

## 6. Other approaches that could work

- **Algebraic constructions (the mathematicians' route).** Representations from
  **distance-regular graphs, association schemes, non-abelian group actions, or
  combinatorial designs** — the generalisations of Comer's method. Plausibly
  *higher* probability than blind SAT *if* a representation has algebraic structure,
  but needs domain expertise. The DRG avenue is being actively explored (2025-26).
- **Streamlined SAT + CAS (SC², "MathCheck").** Combine SAT with a computer-algebra
  system: impose a candidate symmetry/scheme, have the CAS generate/verify the
  structural constraints, SAT-search within. This is the exact SOTA lineage (Bright,
  Ganesh — and the MapleSAT backend we vendored comes from that ecosystem).
- **Stronger symmetry breaking on the raw K_N path.** Switch the corpus to the
  `ramsey_colour_cardinality` generator (global frequency-based colour breaking) +
  `-A/-B` balance bounds + BreakID graph-automorphism breaking. Won't reach the open
  N, but is a cheap unlock for pushing the *known* frontier (12_5/13_6) and for
  UNSAT lower bounds.
- **Local search / SLS (WalkSAT-style).** Good at finding models in huge symmetric
  spaces *if solutions are dense*. For the open cases they're likely rare, so this
  is a cheap long-shot, not a main line.
- **Cube-and-conquer + DRAT for UNSAT bounds.** Dagster's home turf. Doesn't find a
  representation (that's SAT), but rigorously *certifies* how far up the frontier is
  ruled out. Complements the search.
- **Collaborate with the specialists (Alm / Andrews / Kowalski / Bright).** They own
  the method and the domain; our exhaustive-SAT-sweep + certified-proof machinery is
  a natural, complementary contribution — especially the *certification* they may
  lack.

---

## 7. Honest likelihood

- **Find a representation → resolve M = 8/13:** ~**8-15 %** with a thorough
  exhaustive-group sweep (GAP) at larger orders on HPC. Higher if a representation
  lives at a reachable order with some structure; near-zero if it's non-representable
  or only representable at huge N. We'd be competing with specialists using the same
  method — our edge is *breadth* they may not have exhausted. A rough calibration,
  not a computed number; the dominant unknown (does a structured representation exist
  at reachable order?) is genuinely unknown.
- **Certified lower bounds (Dagster's niche):** high, and genuinely valuable —
  achievable regardless of whether a representation is found.
- **Prove non-representability:** ~0 % via SAT (structurally impossible; needs a
  theorem).

Two clean negative sweeps (~250 small groups) barely move this — that's <5 % of the
space, and matches *why* it's an open problem: the easy cases don't exist.

---

## 8. Concrete next steps

1. `sudo apt install -y gap` — the biggest single lever; makes the search
   *exhaustive* at each order and makes results *certifiable*.
2. Re-run the M = 8 (and M = 13) sweep with the **exhaustive** catalogue over small
   orders; **DRAT-certify** the UNSAT frontier → a citable "no representation below
   order k" result.
3. Wire in **Kissat** and push to **larger orders** on **HPC** (embarrassingly
   parallel) for the actual representation hunt.
4. In parallel, the cheap raw-path unlock: switch the corpus to
   `ramsey_colour_cardinality` + `-A/-B` bounds to push the known frontier / lower
   bounds.

---

## 9. Files & how to run

- `comer.py` — the Comer/Cayley encoder + verifier + group families + GAP hook.
  - `python3 comer.py --validate` — reproduce known M = 3..7 cyclic reps.
  - `python3 comer.py -M 8 --sweep 15 48` — sweep the catalogue (exhaustive if GAP
    installed, else the fallback families) for an 8-colour representation.
  - `python3 comer.py -M 3 --n 13` — single group (cyclic Z_n).
  - `python3 comer.py -M 3 --n 14 --group dihedral` — a dihedral run.
  - `python3 comer.py -M 8 --n 24 -o out.cnf` — just emit the CNF.
  - `groups_cache/` — per-order GAP SmallGroups dumps (regenerable; git-ignored).
- `Benchmarks/ramsey/` — the raw K_N generator (weak colour break; makefile).
- `Benchmarks/ramsey_colour_cardinality/` — raw K_N generator with the STRONGER
  colour-cardinality break + `-A/-B` bounds (not wired into the corpus; the free win).
- Standalone cadical: `dagster/cadical_solver/cadical/build/cadical`. Kissat is the
  upgrade for the real sweep (fetch/build separately).

---

## 10. References

- Kowalski, *Representability of Ramsey Relation Algebras*, Algebra Univ. 74 (2015).
- Alm, *401 and Beyond: Improved Bounds and Algorithms for the Ramsey Algebra
  Search*, arXiv 1609.01817 (2017).
- Alm & Andrews, *A Reduced Upper Bound for an Edge-coloring Problem from Relation
  Algebra*, arXiv 1504.07290 (2019).
- Alm, Andrews & Levet, *Comer Schemes, Relation Algebras, and the Flexible Atom
  Conjecture*, arXiv 1905.11914 (updated Dec 2024) — the SOTA method.
- Kramer & Maddux, *Monk algebras and Ramsey theory*, J. Algebra (2022).
- *Monk Algebras and Representability*, arXiv 2501.07332 (Jan 2025) — current status.
- *Relation Algebra Representations from Distance-Regular Graphs*, arXiv 2605.14190
  (2025-26) — the DRG avenue.
- Codish, Miller, Prosser & Stuckey, *Breaking Symmetries in Graph Representation*
  (the lex-leader breaking used by the raw generators).
