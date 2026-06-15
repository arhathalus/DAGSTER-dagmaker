"""Decomposition backends: CNF -> DagModel.

Each backend exposes ``build(cnf, **opts) -> DagModel``.  The single-node
fallback is always available; richer backends (elimination, partition, rcm,
external) are added incrementally and selected by the CLI / multi-backend
scorer.  Optional backends import heavy libraries lazily and are skipped when
those libraries are absent.
"""

from . import single

__all__ = ["single"]
