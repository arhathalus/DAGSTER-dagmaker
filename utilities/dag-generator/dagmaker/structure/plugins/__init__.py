"""Problem-family plugins (tier B).

A plugin recognises a class of CNF encodings whose high-level structure implies a
good, cheap decomposition that the generic backend cannot recover from the flat
primal graph (e.g. time-indexed BMC/planning, grids, graphs).

Protocol (duck-typed):

    name: str
    detect(cnf) -> float            # confidence in [0, 1]; cheap
    build(cnf, *, target_nodes, max_sep, reporting, prune, meta) -> DagModel | None

Register new plugins in :data:`PLUGINS`.
"""

from __future__ import annotations

from typing import List, Optional

from . import timeindexed, grid, graph_family

PLUGINS = [
    timeindexed.TimeIndexedPlugin(),
    grid.GridPlugin(),
    graph_family.GraphFamilyPlugin(),
]


def all_plugins() -> List:
    return PLUGINS


def get_plugin(name: str) -> Optional[object]:
    for p in PLUGINS:
        if p.name == name:
            return p
    return None
