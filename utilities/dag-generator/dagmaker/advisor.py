"""Recommend a tuned dagster invocation for a generated DAG.

Maps structural features of the DAG (and the CNF) onto dagster's command-line
options.  Flag semantics are taken from the project README (the source of truth):

  -n   mpirun process count            --backend/--sls/--strengthen  solving unit
  -k   #SLS helpers (with --sls)        -g/-c  BDD solution compilation
  -b   breadth(1)/depth(0)-first       -e   enumerate all solutions

Heuristics (each is reported with a one-line rationale so the user can override):
  * parallel width -> number of concurrent solving units -> -n.
  * a large, loosely-constrained (low clause/var ratio) node -> SLS helps
    (--sls -k); a tightly-constrained / likely-UNSAT node -> clean CDCL +
    minimisation (--strengthen).
  * an unavoidably wide separator (> bdd_threshold) -> BDD mode (-g 1 -c minisat)
    compresses the 2^k partial-solution set; the offending edge is named.
  * deep chain -> depth-first (-b 0); wide/shallow -> breadth-first (-b 1).
"""

from __future__ import annotations

from typing import List, Optional

from .dagmodel import DagModel
from .scorer import Score

# Above this separator width an explicit 2^k solution table is unwieldy and BDD
# compression is advisable.
BDD_THRESHOLD = 24
# Random-3SAT phase transition; a rough "tightly constrained" marker.
TIGHT_RATIO = 4.0


class Recommendation:
    def __init__(self) -> None:
        self.n: int = 2
        self.mode: int = 0
        self.k: int = 0
        self.breadth: int = 0
        self.bdd: bool = False
        self.bdd_compile: str = "minisat"
        self.enumerate_all: bool = False
        self.rationale: List[str] = []

    # advisor's internal mode (0..3) -> dagster's orthogonal flag interface.
    # 0 plain tinisat, 1 +SLS, 2 +SLS+strengthen, 3 +strengthen.
    _MODE_FLAGS = {0: ["--backend", "tinisat"],
                   1: ["--backend", "tinisat", "--sls"],
                   2: ["--backend", "tinisat", "--sls", "--strengthen"],
                   3: ["--backend", "tinisat", "--strengthen"]}

    def command(self, dag_path: str, cnf_path: str, binary: str = "./dagster") -> str:
        parts = ["mpirun", "-n", str(self.n), binary] + self._MODE_FLAGS[self.mode]
        if self.mode in (1, 2) and self.k > 0:
            parts += ["-k", str(self.k)]
        parts += ["-b", str(self.breadth)]
        if self.bdd:
            parts += ["-g", "1", "-c", self.bdd_compile]
        if self.enumerate_all:
            parts += ["-e", "1"]
        parts += [dag_path, cnf_path]
        return " ".join(parts)

    def __str__(self) -> str:
        return "\n".join(["  " + r for r in self.rationale])


def advise(model: DagModel, score: Score, cnf=None,
           cores: Optional[int] = None, enumerate_all: bool = False) -> Recommendation:
    rec = Recommendation()
    rec.enumerate_all = enumerate_all

    # --- process count from parallel width ------------------------------
    units = max(1, score.parallel_width)
    if cores is not None:
        units = min(units, max(1, cores - 1))  # leave one rank for the master

    # --- mode from per-node tightness -----------------------------------
    # use the largest node as the representative subproblem
    big = _largest_node(score)
    ratio = None
    if cnf is not None and score.node_var_counts and big is not None:
        vc = score.node_var_counts[big]
        cc = score.node_clause_counts[big]
        if vc > 0:
            ratio = cc / vc

    big_vars = score.node_var_counts[big] if (big is not None and score.node_var_counts) else 0
    if ratio is None:
        rec.mode = 2
        rec.k = 1
        rec.rationale.append("mode 2 (--backend tinisat --sls --strengthen -k 1): no var stats; SLS+minimisation is a safe default")
    elif ratio >= TIGHT_RATIO:
        rec.mode = 3
        rec.rationale.append(
            "mode 3 (--backend tinisat --strengthen): tightly constrained (clause/var={:.1f}); clean CDCL "
            "with clause minimisation".format(ratio))
    elif big_vars >= 100:
        rec.mode = 1
        rec.k = 2 if big_vars >= 1000 else 1
        rec.rationale.append(
            "mode 1 (--backend tinisat --sls -k {}): largest node is big ({} vars) & loosely "
            "constrained (clause/var={:.1f}); SLS should find models fast"
            .format(rec.k, big_vars, ratio))
    else:
        rec.mode = 0
        rec.rationale.append(
            "mode 0 (--backend tinisat): small, loosely-constrained nodes ({} vars, "
            "clause/var={:.1f}); plain CDCL, SLS overhead not worth it"
            .format(big_vars, ratio))

    # processes per unit: 1 (CDCL) + k SLS helpers in modes 1/2
    per_unit = 1 + (rec.k if rec.mode in (1, 2) else 0)
    rec.n = 1 + units * per_unit
    if cores is not None:
        rec.n = min(rec.n, cores)
    rec.rationale.append(
        "-n {}: 1 master + {} solving unit(s) x {} process(es){}".format(
            rec.n, units, per_unit,
            " (capped at --cores)" if cores is not None else ""))

    # --- BDD escalation for wide separators -----------------------------
    if score.max_sep_width > BDD_THRESHOLD:
        rec.bdd = True
        edge = _widest_edge(model)
        rec.rationale.append(
            "-g 1 -c {}: widest separator is {} vars (edge {}); BDD compresses the "
            "2^{} partial-solution set".format(rec.bdd_compile, score.max_sep_width,
                                               edge, score.max_sep_width))

    # --- breadth vs depth -----------------------------------------------
    if score.parallel_width > 1:
        rec.breadth = 1
        rec.rationale.append("-b 1: breadth-first (DAG has parallel width {})".format(score.parallel_width))
    else:
        rec.breadth = 0
        rec.rationale.append("-b 0: depth-first (linear/chain DAG)")

    if enumerate_all:
        rec.rationale.append("-e 1: enumerate all solutions (requested)")

    return rec


def _largest_node(score: Score) -> Optional[int]:
    if not score.node_clause_counts:
        return None
    return max(range(len(score.node_clause_counts)),
               key=lambda i: score.node_clause_counts[i])


def _widest_edge(model: DagModel):
    best = None
    best_w = -1
    for (u, v), vars_ in model.edges.items():
        if len(vars_) > best_w:
            best_w, best = len(vars_), (u, v)
    return "{}->{}".format(*best) if best else "n/a"
