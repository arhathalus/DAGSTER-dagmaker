"""Streaming DIMACS CNF reader producing a compact, query-friendly index.

Clause indexing matches Dagster's own loader (``dagster/Cnf.cpp::load_DIMACS_Cnf``)
exactly, which is what the ``.dag`` ``CLAUSES:`` indices must line up with:

  * The header ``p cnf V C`` is located first; ``V`` is the variable count.
  * After the header, the body is a continuous stream of integers terminated by
    ``0`` per clause.  Clause index = order of appearance, 0-based.
  * A line whose first non-whitespace character is not a digit or ``-`` (i.e. a
    ``c`` comment, ``%`` marker, or stray text) is skipped wholesale -- its
    contents are NOT tokenised.  This matters because comment lines such as
    ``c MAPPING -- r=1 c=1 v=1: -- 1`` contain digits that must not be read as
    literals.

The index stores the primal-graph adjacency in CSR form using ``array('i')`` so
that instances with ~10^6 clauses stay memory-lean.  Comment markers are
captured (with the clause index in effect when they appear) so the structure
tiers can recover clause-group labels and variable maps.
"""

from __future__ import annotations

from array import array
from typing import List, Sequence, Tuple


class CnfIndex:
    """Indexed view of a DIMACS CNF.

    Attributes:
        n_clauses: number of clauses (clause indices are ``0 .. n_clauses-1``).
        max_var: largest variable number used (variables are ``1 .. max_var``).
        comment_markers: list of ``(clause_index, text)`` -- ``text`` is the raw
            comment line (without the leading ``c``); ``clause_index`` is the
            number of clauses already read when the comment was encountered, so
            a marker labels the clauses in ``[clause_index, next_marker_index)``.
    """

    def __init__(self) -> None:
        self.n_clauses: int = 0
        self.max_var: int = 0
        # CSR: clause c -> variables  is  _c2v[_c2v_off[c]:_c2v_off[c+1]]
        self._c2v_off: array = array("i", [0])
        self._c2v: array = array("i")
        # CSR: var v -> clauses  is  _v2c[_v2c_off[v]:_v2c_off[v+1]]   (1-based v)
        self._v2c_off: array = array("i")
        self._v2c: array = array("i")
        self.comment_markers: List[Tuple[int, str]] = []

    # ----- construction -------------------------------------------------

    @classmethod
    def from_file(cls, path: str) -> "CnfIndex":
        # CNFs in the wild may be CRLF / CP1252 in comments; clause data is ASCII,
        # so decode leniently rather than crash on a stray byte.
        with open(path, "r", errors="replace") as f:
            return cls.from_lines(f)

    @classmethod
    def from_clauses(cls, clauses, max_var: int) -> "CnfIndex":
        """Build an index from a list of signed clauses (e.g. the output of
        :func:`preprocess.simplify`).  Variable numbering is taken as given;
        ``max_var`` should match the original problem so reporting ids line up."""
        self = cls()
        self.max_var = max_var
        clause_vars = []
        for c in clauses:
            vs = [(l if l > 0 else -l) for l in c]
            clause_vars.append(vs)
            self._c2v.extend(vs)
            self._c2v_off.append(len(self._c2v))
            self.n_clauses += 1
            for v in vs:
                if v > self.max_var:
                    self.max_var = v
        self._build_var_index(clause_vars)
        return self

    @classmethod
    def from_lines(cls, lines) -> "CnfIndex":
        self = cls()
        header_cc = None
        # variables of the clause currently being assembled (may span lines)
        cur: List[int] = []
        clause_vars: List[List[int]] = []
        header_seen = False

        for raw in lines:
            line = raw.strip()
            if not line:
                continue
            first = line[0]
            if first == "p" and not header_seen:
                parts = line.split()
                # "p cnf V C"
                if len(parts) >= 4 and parts[1] == "cnf":
                    self.max_var = int(parts[2])
                    header_cc = int(parts[3])
                    header_seen = True
                continue
            if not (first.isdigit() or first == "-"):
                # comment / marker line: record it, do not tokenise
                text = line[1:].strip() if first == "c" else line
                self.comment_markers.append((self.n_clauses, text))
                continue
            # clause data line: feed its integer tokens into the 0-delimited stream
            for tok in line.split():
                lit = int(tok)
                if lit == 0:
                    self._close_clause(cur, clause_vars)
                    cur = []
                    if header_cc is not None and self.n_clauses == header_cc:
                        break
                else:
                    v = lit if lit > 0 else -lit
                    if v > self.max_var:
                        self.max_var = v
                    cur.append(v)
            if header_cc is not None and self.n_clauses == header_cc:
                break

        # tolerate a final clause that was not 0-terminated
        if cur:
            self._close_clause(cur, clause_vars)

        self._build_var_index(clause_vars)
        return self

    def _close_clause(self, cur: List[int], clause_vars: List[List[int]]) -> None:
        # Dagster rejects duplicate / contradicting literals in a clause
        # (Cnf.cpp), so each variable already appears at most once -- no need to
        # set()/sort(), which is a large saving at ~10^6 clauses.
        clause_vars.append(cur)
        self._c2v.extend(cur)
        self._c2v_off.append(len(self._c2v))
        self.n_clauses += 1

    def _build_var_index(self, clause_vars: List[List[int]]) -> None:
        # counting sort: var -> list of clause indices
        counts = array("i", [0]) * (self.max_var + 2)
        for vs in clause_vars:
            for v in vs:
                counts[v + 1] += 1
        for v in range(1, self.max_var + 2):
            counts[v] += counts[v - 1]
        self._v2c_off = array("i", counts)  # offsets (will be consumed below)
        self._v2c = (array("i", [0]) * len(self._c2v)) if len(self._c2v) else array("i")
        cursor = array("i", self._v2c_off)
        for ci, vs in enumerate(clause_vars):
            for v in vs:
                self._v2c[cursor[v]] = ci
                cursor[v] += 1

    # ----- queries ------------------------------------------------------

    def clause_vars(self, c: int) -> Sequence[int]:
        return self._c2v[self._c2v_off[c]:self._c2v_off[c + 1]]

    def var_clauses(self, v: int) -> Sequence[int]:
        return self._v2c[self._v2c_off[v]:self._v2c_off[v + 1]]

    def var_degree(self, v: int) -> int:
        return self._v2c_off[v + 1] - self._v2c_off[v]

    def used_vars(self):
        """Set of variables that actually appear in at least one clause.

        The header's variable count can exceed this (some variable numbers may
        never occur); such free variables are unconstrained and are not a
        sensible default for REPORTING.
        """
        return {v for v in range(1, self.max_var + 1) if self.var_degree(v) > 0}

    def __repr__(self) -> str:
        return "CnfIndex(n_clauses={}, max_var={}, markers={})".format(
            self.n_clauses, self.max_var, len(self.comment_markers))
