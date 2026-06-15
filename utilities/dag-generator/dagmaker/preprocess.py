"""BCP + PLE preprocessing (a Python port of c_pro's `up.cpp`).

Runs unit propagation (Boolean Constraint Propagation) and pure-literal
elimination to a fixpoint, simplifying the CNF before decomposition:

  * drops satisfied clauses,
  * removes falsified literals from surviving clauses,
  * fixes forced/pure variables (recorded in the ``trail``),
  * detects UNSAT (empty clause / conflict).

Variable numbering is preserved (no renaming), matching c_pro, so the simplified
CNF's variable ids still mean the same thing; fixed variables simply no longer
appear in any clause and their values are reported via the trail.

BCP is counter-based (per-clause active-literal counts + per-variable
positive/negative occurrence lists) rather than watched-literals -- simpler in
Python and fast enough for a one-off preprocessing step.

Because preprocessing changes the clause set, the DAG that decomposes the result
references the *simplified* CNF; callers must emit that CNF (see ``write_dimacs``)
and hand it to dagster, not the original.
"""

from __future__ import annotations

from collections import deque
from typing import List, NamedTuple, Tuple


class Simplified(NamedTuple):
    sat: bool                 # False if proven UNSAT
    clauses: List[List[int]]  # simplified signed clauses
    max_var: int              # unchanged from input
    trail: List[int]          # forced/pure literals (positive => var true)
    n_clauses_before: int
    n_clauses_after: int


def read_dimacs(path: str) -> Tuple[List[List[int]], int]:
    """Read signed clauses from a DIMACS file, using the same line/skip rules and
    0-terminated clause indexing as :class:`CnfIndex` (so clause indices agree)."""
    clauses: List[List[int]] = []
    max_var = 0
    cur: List[int] = []
    header_seen = False
    header_cc = None
    with open(path, "r", errors="replace") as f:
        for raw in f:
            line = raw.strip()
            if not line:
                continue
            first = line[0]
            if first == "p" and not header_seen:
                parts = line.split()
                if len(parts) >= 4 and parts[1] == "cnf":
                    max_var = int(parts[2])
                    header_cc = int(parts[3])
                    header_seen = True
                continue
            if not (first.isdigit() or first == "-"):
                continue  # comment / marker
            for tok in line.split():
                lit = int(tok)
                if lit == 0:
                    clauses.append(cur)
                    cur = []
                    if header_cc is not None and len(clauses) == header_cc:
                        break
                else:
                    cur.append(lit)
                    av = lit if lit > 0 else -lit
                    if av > max_var:
                        max_var = av
            if header_cc is not None and len(clauses) == header_cc:
                break
    if cur:
        clauses.append(cur)
    return clauses, max_var


def write_dimacs(path: str, clauses: List[List[int]], max_var: int,
                 trail: List[int] = ()) -> None:
    """Write a simplified CNF. The fixed assignments are preserved as ``c FIXED``
    comment lines so the full solution can be reconstructed from dagster's output
    over the surviving variables."""
    with open(path, "w") as f:
        if trail:
            f.write("c simplified by dagmaker BCP+PLE; {} variable(s) fixed below\n"
                    .format(len(trail)))
            # chunk the fixed literals across comment lines
            for i in range(0, len(trail), 20):
                f.write("c FIXED " + " ".join(map(str, trail[i:i + 20])) + "\n")
        f.write("p cnf {} {}\n".format(max_var, len(clauses)))
        for c in clauses:
            f.write(" ".join(map(str, c)) + " 0\n")


def simplify(clauses: List[List[int]], max_var: int) -> Simplified:
    n_before = len(clauses)
    value = [0] * (max_var + 1)            # 0 unknown, +1 true, -1 false
    satisfied = [False] * len(clauses)
    active = [len(c) for c in clauses]     # # non-falsified literals
    pos_cl: List[List[int]] = [[] for _ in range(max_var + 1)]
    neg_cl: List[List[int]] = [[] for _ in range(max_var + 1)]
    for ci, c in enumerate(clauses):
        for lit in c:
            (pos_cl if lit > 0 else neg_cl)[lit if lit > 0 else -lit].append(ci)

    trail: List[int] = []
    q: deque = deque()

    def assign(lit: int) -> bool:
        v = lit if lit > 0 else -lit
        s = 1 if lit > 0 else -1
        if value[v] == s:
            return True
        if value[v] == -s:
            return False  # conflict
        value[v] = s
        trail.append(lit)
        q.append(lit)
        return True

    def propagate() -> bool:
        while q:
            lit = q.popleft()
            v = lit if lit > 0 else -lit
            s = value[v]
            sat_list = pos_cl[v] if s > 0 else neg_cl[v]
            fal_list = neg_cl[v] if s > 0 else pos_cl[v]
            for ci in sat_list:
                satisfied[ci] = True
            for ci in fal_list:
                if satisfied[ci]:
                    continue
                active[ci] -= 1
                if active[ci] == 0:
                    return False  # empty clause -> conflict
                if active[ci] == 1:
                    unit = _remaining_literal(clauses[ci], value)
                    if unit is not None and not assign(unit):
                        return False
        return True

    # initial units / empty clauses
    for ci, c in enumerate(clauses):
        if len(c) == 0:
            return Simplified(False, [], max_var, trail, n_before, 0)
        if len(c) == 1 and not assign(c[0]):
            return Simplified(False, [], max_var, trail, n_before, 0)
    if not propagate():
        return Simplified(False, [], max_var, trail, n_before, 0)

    # PLE + BCP to fixpoint
    while True:
        before = len(trail)
        if not _pure_literal_round(clauses, value, satisfied, assign):
            return Simplified(False, [], max_var, trail, n_before, 0)
        if not propagate():
            return Simplified(False, [], max_var, trail, n_before, 0)
        if len(trail) == before:
            break

    out = _rebuild(clauses, value, satisfied)
    return Simplified(True, out, max_var, trail, n_before, len(out))


def _remaining_literal(clause, value):
    for lit in clause:
        if value[lit if lit > 0 else -lit] == 0:
            return lit
    return None


def _pure_literal_round(clauses, value, satisfied, assign) -> bool:
    max_var = len(value) - 1
    has_pos = bytearray(max_var + 1)
    has_neg = bytearray(max_var + 1)
    for ci, c in enumerate(clauses):
        if satisfied[ci]:
            continue
        sat = False
        for lit in c:
            v = lit if lit > 0 else -lit
            a = value[v]
            if a != 0 and (a > 0) == (lit > 0):
                sat = True
                break
        if sat:
            satisfied[ci] = True
            continue
        for lit in c:
            v = lit if lit > 0 else -lit
            if value[v] == 0:
                if lit > 0:
                    has_pos[v] = 1
                else:
                    has_neg[v] = 1
    for v in range(1, max_var + 1):
        if value[v] != 0:
            continue
        if has_pos[v] and not has_neg[v]:
            if not assign(v):
                return False
        elif has_neg[v] and not has_pos[v]:
            if not assign(-v):
                return False
    return True


def _rebuild(clauses, value, satisfied) -> List[List[int]]:
    out: List[List[int]] = []
    for ci, c in enumerate(clauses):
        if satisfied[ci]:
            continue
        keep = []
        sat = False
        for lit in c:
            v = lit if lit > 0 else -lit
            a = value[v]
            if a == 0:
                keep.append(lit)
            elif (a > 0) == (lit > 0):
                sat = True
                break
        if sat:
            continue
        out.append(keep)
    return out
