# Proof tooling — machine-checked UNSAT certificates

Milestone 0 of the proof work (design in `../cube/PROOF_SCOPE.md`): turn a Dagster
UNSAT *verdict* into a *checked theorem* via a DRAT proof.

## Contents
- `drat-trim.c` — the standard DRAT checker (vendored from CaDiCaL's test tree). Build it:
  ```sh
  gcc -O2 -o drat-trim drat-trim.c -lm
  ```
- `certify.py` — turnkey: solve a CNF as a single Dagster node with CaDiCaL emitting
  a DRAT proof (`dagster --backend cadical --proof`), then verify it with drat-trim.
  Exposes `certify(cnf, ...)` for reuse.
- `tautology.py` — Milestone 1: certify that a march `.icnf`'s cubes are EXHAUSTIVE
  (the cube-split tautology proof). Builds the negated-cubes CNF `⋀_i ¬Cᵢ` and
  certifies it UNSAT — that refutation *is* the tautology proof. A SAT result means
  the cubes have a gap (drat-trim hands back the uncovered assignment).
  ```sh
  python3 tautology.py cubes.icnf          # EXHAUSTIVE (verified) | GAP
  ```
- `cc_certify.py` — Milestone 2: full cube-and-conquer UNSAT certificate. Certifies
  every `formula ∧ Cᵢ` UNSAT (drat-trim) **and** the cubes exhaustive (tautology),
  which together prove `formula` UNSAT. A SAT cube ⟹ the formula is SAT (witness reported).
  ```sh
  python3 cc_certify.py formula.cnf cubes.icnf            # UNSAT CERTIFIED | SAT | gap
  python3 cc_certify.py formula.cnf cubes.icnf --max-cubes 8   # smoke test
  ```

## Use
```sh
python3 certify.py problem.cnf            # -> UNSAT VERIFIED | SAT | (NOT verified)
python3 certify.py problem.cnf --keep     # keep the .drat proof + DAG
```
- **UNSAT** → emits + checks a DRAT proof; "VERIFIED" means the verdict is a theorem
  (independent of Dagster's DAG/cube machinery — the single-node run is just the
  backend + proof). Exit 0 iff verified.
- **SAT** → no proof needed; the model is the certificate.

Certify a corpus UNSAT label, e.g.:
```sh
python3 certify.py ../../Benchmarks/generated/ramsey_6_3.cnf
```

## Scope / next
This is the single-solver certifier. It scales only as far as one CaDiCaL can refute
the whole formula. Genuinely hard UNSAT (the open ramsey/domatic targets) needs
**cube-and-conquer proofs** — per-cube proofs + a cube-split tautology proof,
concatenated and checked — which is the next milestone (`../cube/PROOF_SCOPE.md` §3-4).
The march cube-split tautology proof is the main missing piece there.

Caveats (enforced by `dagster`): `--proof` is CaDiCaL-only and rejects `--share`/
`--sls` (a proof must be a self-contained single solve); use `-e 0` (enumeration
adds non-entailed blocking clauses). Symmetry breaking is **not** DRAT-justifiable
(needs PR/SR proofs), so certify the *unbroken* formula.
