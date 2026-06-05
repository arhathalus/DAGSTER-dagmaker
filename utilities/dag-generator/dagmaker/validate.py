"""Stdlib validity gate for generated DAGs.

This checks the invariants that Dagster's C++ actually enforces
(``dagster/Dag.cpp``, ``SolutionsInterface.h``), and deliberately does NOT
replicate the buggy variable-count assertions in the legacy
``utilities/dag-generator/dag_checker.py`` (lines 148-154), which assume a
gap-free ``1..N`` variable numbering and reject perfectly valid subproblem DAGs.

Use :func:`validate` as the primary gate.  It returns a :class:`Report`; treat
``report.ok`` as the pass/fail signal and surface ``report.problems`` to users.
"""

from __future__ import annotations

from typing import List, Optional

from .dagmodel import DagModel


class Report:
    def __init__(self) -> None:
        self.problems: List[str] = []
        self.warnings: List[str] = []

    @property
    def ok(self) -> bool:
        return not self.problems

    def err(self, msg: str) -> None:
        self.problems.append(msg)

    def warn(self, msg: str) -> None:
        self.warnings.append(msg)

    def __str__(self) -> str:
        lines = []
        if self.ok:
            lines.append("VALID")
        else:
            lines.append("INVALID ({} problem(s))".format(len(self.problems)))
            lines += ["  - " + p for p in self.problems]
        if self.warnings:
            lines += ["  ! " + w for w in self.warnings]
        return "\n".join(lines)


def validate(model: DagModel, cnf=None, strict: bool = False) -> Report:
    """Validate a DAG.

    ``strict=False`` (default) allows a clause to appear in more than one node --
    Dagster permits this and it is what cutset/overlap decompositions rely on
    (the README's own ``d1.txt`` overlaps clauses).  ``strict=True`` enforces the
    clean exactly-once partition used by the elimination/structure backends.
    """
    r = Report()
    n = model.num_nodes

    if n < 1:
        r.err("DAG has no nodes")
        return r

    # --- edges reference valid nodes, no self loops ---------------------
    for (u, v) in model.edges:
        if not (0 <= u < n and 0 <= v < n):
            r.err("edge {}->{} references a non-existent node".format(u, v))
        if u == v:
            r.err("self-loop edge on node {}".format(u))

    # --- acyclic --------------------------------------------------------
    order = model.topo_order()
    if len(order) != n:
        r.err("DAG is cyclic (topological sort covered {}/{} nodes)".format(len(order), n))

    # --- every node has clauses -----------------------------------------
    for i, clauses in enumerate(model.nodes):
        if not clauses:
            r.err("node {} has no clauses".format(i))

    # --- clause coverage ------------------------------------------------
    # Every clause must appear in at least one node.  Duplicates (a clause in
    # several nodes) are an error only in strict-partition mode.
    if model.n_clauses:
        seen = bytearray(model.n_clauses)
        out_of_range = 0
        duplicated = 0
        for clauses in model.nodes:
            for c in clauses:
                if not (0 <= c < model.n_clauses):
                    out_of_range += 1
                    continue
                if seen[c]:
                    duplicated += 1
                else:
                    seen[c] = 1
        missing = model.n_clauses - sum(seen)
        if out_of_range:
            r.err("{} clause index/indices out of range [0,{})".format(out_of_range, model.n_clauses))
        if missing:
            r.err("{} clause(s) not covered by any node".format(missing))
        if duplicated:
            if strict:
                r.err("{} clause(s) assigned to more than one node (strict partition)".format(duplicated))
            else:
                r.warn("{} clause occurrence(s) overlap across nodes (allowed)".format(duplicated))

    # --- reporting basic checks -----------------------------------------
    for v in model.reporting:
        if v < 1:
            r.err("REPORTING contains non-positive variable {}".format(v))
        if model.max_var and v > model.max_var:
            r.err("REPORTING variable {} exceeds max_var {}".format(v, model.max_var))
    if model.max_var and not model.reporting:
        r.warn("REPORTING is empty -- no solution variables will be output")

    # --- variable-aware checks (need the CNF) ---------------------------
    if cnf is not None:
        _validate_with_cnf(model, cnf, r, strict)

    # --- format round-trip ----------------------------------------------
    _validate_roundtrip(model, r)

    return r


def _validate_with_cnf(model: DagModel, cnf, r: Report, strict: bool = False) -> None:
    # available[n] = local clause vars + everything inherited on actual edges
    available = model.neighborhoods(cnf)
    local = model.node_local_vars(cnf)

    # an edge may only pass variables the parent actually possesses
    for (u, v), vars_ in model.edges.items():
        if 0 <= u < model.num_nodes:
            missing = vars_ - available[u]
            if missing:
                ex = sorted(missing)[:5]
                r.err("edge {}->{} passes {} variable(s) the parent lacks (e.g. {})"
                      .format(u, v, len(missing), ex))

    # --- coherence (running-intersection property) ----------------------
    # This is the genuine dagster soundness condition: for each variable, the
    # nodes whose clauses use it must be connected by edges that carry it, so a
    # consistent value is propagated.  (We do NOT require the legacy
    # dag_checker subset invariant, which pass-all-data satisfies but pruned
    # DAGs intentionally do not.)
    _check_coherence(model, cnf, r, strict)

    # informational: note if the legacy subset invariant is broken (pruned DAG)
    for (u, v) in model.edges:
        if 0 <= u < model.num_nodes and 0 <= v < model.num_nodes:
            if not available[u].issubset(available[v]):
                r.warn("edge {}->{} breaks the legacy subset invariant "
                       "(pruned DAG; fine for dagster, fails dag_checker.py)".format(u, v))
                break

    # Every reporting variable that actually occurs in the CNF must reach a
    # terminal node.  Free variables (declared but never used in a clause) are
    # unconstrained and cannot appear in any neighborhood -- they are not a
    # reachability error, just flagged as a warning.
    present = set().union(*local) if local else set()
    free_reported = model.reporting - present
    if free_reported:
        r.warn("{} REPORTING variable(s) do not occur in any clause (free vars)"
               .format(len(free_reported)))
    terms = model.terminals()
    if terms:
        term_vars = set().union(*(available[t] for t in terms))
        miss = (model.reporting & present) - term_vars
        if miss:
            r.err("{} REPORTING variable(s) never reach a terminal node (e.g. {})"
                  .format(len(miss), sorted(miss)[:5]))


def _check_coherence(model: DagModel, cnf, r: Report, strict: bool = False) -> None:
    """Soundness check for variable coordination.

    Strict (clean-partition) DAGs must satisfy the running-intersection property:
    for each variable, the nodes whose clauses use it are connected via edges
    that carry it, so a consistent value is propagated.

    With overlap allowed, a variable is ALSO coherent if some single node
    contains *all* of that variable's clauses -- that node resolves the variable
    authoritatively (this is exactly how a cutset DAG works: the terminal holds
    every clause, so upstream copies don't need coordination).  We therefore only
    flag a variable when running-intersection fails AND no node fully contains
    its clauses.
    """
    # variable -> nodes whose own clauses reference it
    clause_nodes = {}
    for n, clauses in enumerate(model.nodes):
        for c in clauses:
            for x in cnf.clause_vars(c):
                clause_nodes.setdefault(x, set()).add(n)
    # variable -> edges that carry it
    carrying = {}
    for (u, v), vars_ in model.edges.items():
        for x in vars_:
            carrying.setdefault(x, []).append((u, v))
    # clause -> nodes containing it (for the "fully resolved in one node" test)
    clause_to_nodes = None  # built lazily, only if needed

    violations = 0
    for x, nodes in clause_nodes.items():
        if len(nodes) <= 1:
            continue  # trivially coherent
        parent = {}

        def find(a):
            while parent[a] != a:
                parent[a] = parent[parent[a]]
                a = parent[a]
            return a

        def add(a):
            if a not in parent:
                parent[a] = a

        for n in nodes:
            add(n)
        for (u, v) in carrying.get(x, ()):
            add(u)
            add(v)
            parent[find(u)] = find(v)
        roots = {find(n) for n in nodes}
        if len(roots) <= 1:
            continue  # running-intersection holds

        if not strict:
            if clause_to_nodes is None:
                clause_to_nodes = _build_clause_to_nodes(model)
            if _fully_resolved_in_one_node(cnf, x, clause_to_nodes):
                continue  # authoritative node resolves x; overlap is sound

        violations += 1
        if violations <= 5:
            r.err("variable {} is used in disconnected nodes {} not joined by "
                  "carrying edges (running-intersection violated)"
                  .format(x, sorted(nodes)[:6]))
    if violations > 5:
        r.err("... and {} more variable(s) with running-intersection violations"
              .format(violations - 5))


def _build_clause_to_nodes(model: DagModel):
    c2n = {}
    for n, clauses in enumerate(model.nodes):
        for c in clauses:
            c2n.setdefault(c, set()).add(n)
    return c2n


def _fully_resolved_in_one_node(cnf, x, clause_to_nodes) -> bool:
    """True if some single node contains every clause that mentions x."""
    common = None
    for c in cnf.var_clauses(x):
        nodes_c = clause_to_nodes.get(c, set())
        common = nodes_c if common is None else (common & nodes_c)
        if not common:
            return False
    return bool(common)


def _validate_roundtrip(model: DagModel, r: Report) -> None:
    try:
        text = model.to_string()
        reparsed = DagModel.from_string(text, model.n_clauses, model.max_var)
    except Exception as e:  # noqa: BLE001 - surface any serialisation failure
        r.err("serialisation/round-trip failed: {}".format(e))
        return
    if reparsed.num_nodes != model.num_nodes:
        r.err("round-trip node count mismatch")
    if [reparsed.nodes[i] for i in range(reparsed.num_nodes)] != \
            [model.nodes[i] for i in range(model.num_nodes)]:
        r.err("round-trip clause assignment mismatch")
    if {k: set(v) for k, v in reparsed.edges.items()} != \
            {k: set(v) for k, v in model.edges.items()}:
        r.err("round-trip edge mismatch")
    if reparsed.reporting != model.reporting:
        r.err("round-trip REPORTING mismatch")
