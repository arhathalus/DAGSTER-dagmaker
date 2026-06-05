#!/usr/bin/env python3
"""dagmake -- generate a Dagster DAG decomposition from a DIMACS CNF.

A structure-aware successor to ``dagify.py``: it runs a connected-components
pre-pass, decomposes with the available backends (structure tiers when the
problem family is known, generic min-degree elimination otherwise), scores the
candidates against Dagster's cost model, validates the winner, writes the
``.dag`` file, and prints a recommended ``dagster`` invocation tuned to the
decomposition it found.

Minimal use:

    python dagmake.py problem.cnf problem.dag

See ``--help`` for the knobs (node budget, core budget, separator cap, reporting
set, structure family / metadata, pruning).
"""

from __future__ import annotations

import sys

import os

import click

from dagmaker import advisor, intervals, pipeline
from dagmaker import preprocess as preprocess_mod
from dagmaker.cnf import CnfIndex


@click.command()
@click.argument("cnf", type=click.Path(exists=True, dir_okay=False))
@click.argument("dag", type=click.Path(dir_okay=False))
@click.option("--nodes", "-k", "target_nodes", type=int, default=8, show_default=True,
              help="Target number of DAG nodes (an outcome, not a hard cap; the "
                   "separator budget can reduce it).")
@click.option("--cores", type=int, default=None,
              help="HPC core/solving-unit budget hint; sizes the recommended -n.")
@click.option("--max-sep", type=int, default=30, show_default=True,
              help="Separator-width cap. Cuts whose interface exceeds this are "
                   "merged rather than emitted (avoids 2^k blow-up).")
@click.option("--reporting", default=None,
              help="Variables to output, in range notation (e.g. '1-9,20'). "
                   "Default: all variables that occur in the CNF. A smaller set "
                   "enables smaller separators.")
@click.option("--search", is_flag=True,
              help="Search/decision mode: report no variables. Yields the "
                   "smallest separators (for SAT/UNSAT or counting, where full "
                   "per-variable output isn't needed).")
@click.option("--prune/--pass-all-data", default=True, show_default=True,
              help="Pass only downstream-needed variables on each edge (smaller "
                   "separators) vs. forward the full neighborhood (legacy-compatible).")
@click.option("--family", default=None,
              help="Force a structure family (e.g. timeindexed, grid, graph). "
                   "Default: auto-detect / generic.")
@click.option("--meta", type=click.Path(exists=True, dir_okay=False), default=None,
              help="Path to a .meta sidecar describing problem structure.")
@click.option("--binary", default="./dagster", show_default=True,
              help="dagster binary path used in the recommended command.")
@click.option("--enumerate", "enumerate_all", is_flag=True,
              help="Recommend enumerating all solutions (-e 1).")
@click.option("--preprocess", is_flag=True,
              help="Simplify the CNF with unit propagation + pure-literal "
                   "elimination first, and decompose the result. Writes the "
                   "simplified CNF (which the DAG references).")
@click.option("--simplified-cnf", "simplified_cnf", type=click.Path(dir_okay=False),
              default=None, help="Where to write the simplified CNF when "
                   "--preprocess is set (default: alongside the DAG).")
@click.option("--strict-partition", is_flag=True,
              help="Require each clause in exactly one node (disables the "
                   "overlapping cutset backend). Default: overlap allowed.")
@click.option("--cutset-hubs", type=int, default=32, show_default=True,
              help="Hub-variable count for the overlap/cutset backend (capped at "
                   "--max-sep).")
@click.option("--backends", default=None,
              help="Comma-separated subset of backends to try (default: all). "
                   "Options: structure,elimination,biconnected,community,gates,"
                   "ordering,cutset,single.")
@click.option("--quiet", is_flag=True, help="Only write the DAG; minimal output.")
def main(cnf, dag, target_nodes, cores, max_sep, reporting, search, prune, family, meta,
         binary, enumerate_all, preprocess, simplified_cnf, strict_partition,
         cutset_hubs, backends, quiet):
    """Generate DAG file <DAG> from DIMACS CNF <CNF>."""
    # the DAG's clause indices reference this CNF (the original, or the simplified one)
    cnf_for_dagster = cnf

    if preprocess:
        if not quiet:
            click.echo("dagmake: preprocessing {} (BCP + PLE) ...".format(cnf))
        clauses, max_var = preprocess_mod.read_dimacs(cnf)
        s = preprocess_mod.simplify(clauses, max_var)
        if not s.sat:
            click.echo("UNSAT: preprocessing derived a conflict; no DAG needed.", err=True)
            sys.exit(3)
        if simplified_cnf is None:
            base, _ = os.path.splitext(dag)
            simplified_cnf = base + ".simplified.cnf"
        preprocess_mod.write_dimacs(simplified_cnf, s.clauses, s.max_var, s.trail)
        cnf_for_dagster = simplified_cnf
        index = CnfIndex.from_clauses(s.clauses, s.max_var)
        signed_clauses = s.clauses
        if not quiet:
            click.echo("  simplified {} -> {} clauses, {} variable(s) fixed".format(
                s.n_clauses_before, s.n_clauses_after, len(s.trail)))
            click.echo("  wrote simplified CNF: {} (DAG references THIS file)".format(simplified_cnf))
    else:
        if not quiet:
            click.echo("dagmake: parsing {} ...".format(cnf))
        index = CnfIndex.from_file(cnf)
        signed_clauses = preprocess_mod.read_dimacs(cnf)[0]  # for gate detection
    if not quiet:
        click.echo("  {} clauses, {} variables, {} comment marker(s)".format(
            index.n_clauses, index.max_var, len(index.comment_markers)))

    if search:
        # decision/search mode: report a single variable, not none -- dagster's
        # DAG parser rejects an empty REPORTING section. One reported variable
        # keeps separators essentially minimal while staying loadable.
        used = index.used_vars()
        report_set = {min(used)} if used else {1}
    elif reporting:
        report_set = set(intervals.expand(reporting))
    else:
        report_set = None

    gen_kwargs = dict(
        target_nodes=target_nodes, max_sep=max_sep, reporting=report_set,
        prune=prune, cores=cores, family=family, meta=meta,
        strict=strict_partition, cutset_hubs=cutset_hubs, signed_clauses=signed_clauses)
    if backends:
        gen_kwargs["backends"] = tuple(b.strip() for b in backends.split(","))
    result = pipeline.generate(index, **gen_kwargs)

    best = result.best
    if not quiet:
        click.echo("\ncandidates:")
        for c in result.candidates:
            tag = "*" if c is best else " "
            status = "valid" if c.report.ok else "INVALID"
            click.echo("  {} {:<20} {} | {}".format(tag, c.name, status, c.score))

    if not best.report.ok:
        click.echo("\nERROR: best candidate failed validation:", err=True)
        click.echo(str(best.report), err=True)
        sys.exit(1)

    best.model.write(dag)
    if not quiet:
        click.echo("\nwrote {}  ({})".format(dag, best.name))
        for w in best.report.warnings:
            click.echo("  ! " + w)

    rec = advisor.advise(best.model, best.score, index, cores=cores,
                         enumerate_all=enumerate_all)
    if quiet:
        click.echo(rec.command(dag, cnf_for_dagster, binary))
    else:
        click.echo("\nrecommended dagster invocation:")
        click.echo("  " + rec.command(dag, cnf_for_dagster, binary))
        click.echo("rationale:")
        click.echo(str(rec))


if __name__ == "__main__":
    main()
