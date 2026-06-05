"""Compact / expand integer lists in Dagster's range notation.

Dagster ``.dag`` files encode lists of integers as comma-separated runs, e.g.
``[1,2,3,5,6,7,9]`` -> ``"1-3,5-7,9"``.  This mirrors ``compact_list_of_integers``
in ``utilities/dag-generator/utils.py`` but lives here so the stdlib-only core
never has to import that module (which top-imports networkx).

Important format constraint (verified in ``dagster/Dag.cpp``):
  * A CLAUSES range ``a-b`` is only accepted when ``b > a`` (Dag.cpp:136).
    A single clause must therefore be emitted as the bare number ``a`` -- never
    ``a-a``.  ``compact`` guarantees this.
  * GRAPH-literal ranges accept ``b >= a`` (Dag.cpp:102) but we emit bare numbers
    for singletons there too, for uniformity.
"""

from __future__ import annotations

from typing import Iterable, List


def compact(values: Iterable[int]) -> str:
    """Render an iterable of ints as Dagster range notation.

    Duplicates are collapsed and the result is sorted ascending.  Returns the
    empty string for an empty input.  Singletons are emitted bare (never
    ``a-a``) so the output is always accepted by Dagster's parser.
    """
    vals = sorted(set(values))
    if not vals:
        return ""
    terms: List[str] = []
    run_start = run_end = vals[0]
    for v in vals[1:]:
        if v == run_end + 1:
            run_end = v
            continue
        terms.append(_term(run_start, run_end))
        run_start = run_end = v
    terms.append(_term(run_start, run_end))
    return ",".join(terms)


def _term(begin: int, end: int) -> str:
    return str(begin) if begin == end else "{}-{}".format(begin, end)


def expand(text: str) -> List[int]:
    """Parse Dagster range notation back into a sorted list of ints.

    Accepts the output of :func:`compact` as well as whitespace- or
    comma-separated single values and ``a-b`` ranges.  Empty / blank input
    yields an empty list.
    """
    text = text.strip()
    if not text:
        return []
    out: List[int] = []
    # Dagster accepts both commas and whitespace as separators.
    for tok in text.replace(",", " ").split():
        if "-" in tok and not tok.startswith("-"):
            lo_s, hi_s = tok.split("-", 1)
            lo, hi = int(lo_s), int(hi_s)
            out.extend(range(lo, hi + 1))
        else:
            out.append(int(tok))
    return sorted(set(out))
