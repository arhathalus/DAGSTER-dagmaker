"""End-to-end orchestration: CNF -> candidate DAGs -> score -> pick best.

The selection embodies the project objective -- *maximise useful parallelism
subject to a separator-width budget*:

  1. Build candidate DAGs from the available backends (and, later, the
     structure tiers).
  2. Validate each; discard invalid ones.
  3. Keep only candidates whose actual max separator is within ``max_sep``
     (the single-node fallback always qualifies, with separator 0).
  4. Among survivors, prefer more parallelism (parallel width, then node count),
     breaking ties toward smaller separators and shallower DAGs.

This is also where the structure tiers (metadata / plugins / autodetect) and the
optional external backends plug in -- they simply contribute more candidates.
"""

from __future__ import annotations

from typing import List, NamedTuple, Optional, Sequence

from . import scorer, validate
from .dagmodel import DagModel
from .decompose import single, elimination, cutset, biconnected, community, gates, ordering

try:  # structure tiers (added incrementally); absent-safe
    from . import structure as _structure
except Exception:  # noqa: BLE001
    _structure = None


class Candidate(NamedTuple):
    name: str
    model: DagModel
    score: scorer.Score
    report: validate.Report


class Result(NamedTuple):
    best: Candidate
    candidates: List[Candidate]


DEFAULT_BACKENDS = ("structure", "elimination", "biconnected", "community",
                    "gates", "ordering", "cutset", "single")


def generate(cnf, *, target_nodes: int = 8, max_sep: int = 30,
             reporting=None, prune: bool = True, cores: Optional[int] = None,
             family: Optional[str] = None, meta=None, strict: bool = False,
             cutset_hubs: int = 32, signed_clauses=None,
             backends: Sequence[str] = DEFAULT_BACKENDS) -> Result:
    candidates: List[Candidate] = []

    def consider(name: str, model: DagModel) -> None:
        if model is None:
            return
        rep = validate.validate(model, cnf, strict=strict)
        sc = scorer.score(model, cnf)
        candidates.append(Candidate(name, model, sc, rep))

    if "structure" in backends and _structure is not None:
        for name, model in _structure.all_candidates(
                cnf, family=family, meta=meta, target_nodes=target_nodes,
                max_sep=max_sep, reporting=reporting, prune=prune):
            consider("structure:" + name, model)

    if "elimination" in backends:
        consider("elimination",
                 elimination.build(cnf, target_nodes=target_nodes, max_sep=max_sep,
                                   prune=prune, reporting=reporting))

    if "biconnected" in backends:
        consider("biconnected",
                 biconnected.build(cnf, max_sep=max_sep, prune=prune, reporting=reporting))

    if "community" in backends:
        consider("community",
                 community.build(cnf, max_sep=max_sep, prune=prune, reporting=reporting))

    if "gates" in backends and signed_clauses is not None:
        consider("gates",
                 gates.build(cnf, signed_clauses, target_nodes=target_nodes,
                             max_sep=max_sep, prune=prune, reporting=reporting))

    if "ordering" in backends:
        for method in ("rcm", "spectral"):
            consider("ordering:" + method,
                     ordering.build(cnf, method=method, target_nodes=target_nodes,
                                    max_sep=max_sep, prune=prune, reporting=reporting))

    if "cutset" in backends and not strict:
        # overlap decomposition: only meaningful when overlap is permitted
        consider("cutset",
                 cutset.build(cnf, hubs=cutset_hubs, max_sep=max_sep, reporting=reporting))

    if "single" in backends:
        consider("single", single.build(cnf, reporting))

    best = _select(candidates, max_sep)
    return Result(best=best, candidates=candidates)


def _select(candidates: List[Candidate], max_sep: int) -> Candidate:
    valid = [c for c in candidates if c.report.ok]
    if not valid:
        # nothing validated -- return the first candidate so the caller can
        # surface its problems
        return candidates[0]
    within = [c for c in valid if c.score.max_sep_width <= max_sep]
    pool = within if within else valid
    # prefer parallelism; tie-break toward cheaper communication / shallower
    return max(pool, key=lambda c: (c.score.parallel_width, c.score.num_nodes,
                                    -c.score.max_sep_width, -c.score.depth))
