"""Structure-aware decomposition dispatcher.

Resolution order (first that yields a DAG wins):

    explicit --family  >  tier A metadata  >  tier B/C plugin auto-detect

If none apply, returns ``None`` and the pipeline falls back to the generic
elimination backend.  This module contributes candidates to the pipeline's
multi-backend scoring -- a structure-aware DAG only *wins* if it actually scores
better than the generic one, so a misfire never degrades the result.
"""

from __future__ import annotations

import json
from typing import List, Optional, Tuple

from ..dagmodel import DagModel
from . import metadata
from .plugins import all_plugins, get_plugin


def all_candidates(cnf, *, family=None, meta=None, target_nodes=8, max_sep=30,
                   reporting=None, prune=True) -> List[Tuple[str, DagModel]]:
    """Every applicable structure-aware DAG (for the pipeline to score).

    With an explicit ``family`` only that plugin is used; otherwise the tier-A
    metadata candidate (if any) plus every plugin whose ``detect`` confidence is
    >= 0.5 each contribute a candidate, so the scorer picks the genuinely best
    one rather than trusting a single confidence number.
    """
    meta_dict = _load_meta(meta)
    out: List[Tuple[str, DagModel]] = []

    if family:
        p = get_plugin(family)
        if p is not None:
            m = p.build(cnf, target_nodes=target_nodes, max_sep=max_sep,
                        reporting=reporting, prune=prune, meta=meta_dict)
            if m is not None:
                out.append((family, m))
        return out

    m = metadata.try_build(cnf, meta=(meta if not isinstance(meta, dict) else None),
                           target_nodes=target_nodes, max_sep=max_sep,
                           reporting=reporting, prune=prune)
    if m is not None:
        out.append(("metadata", m))

    for p in all_plugins():
        try:
            conf = p.detect(cnf, meta=meta_dict)
        except TypeError:
            conf = p.detect(cnf)
        if conf >= 0.5:
            built = p.build(cnf, target_nodes=target_nodes, max_sep=max_sep,
                            reporting=reporting, prune=prune, meta=meta_dict)
            if built is not None:
                out.append((p.name, built))
    return out


def _load_meta(meta):
    if meta is None or isinstance(meta, dict):
        return meta
    with open(meta, "r") as f:
        return json.load(f)


def try_build(cnf, *, family=None, meta=None, target_nodes=8, max_sep=30,
              reporting=None, prune=True) -> Optional[Tuple[str, DagModel]]:
    meta_dict = _load_meta(meta)

    # 1) explicit family override
    if family:
        p = get_plugin(family)
        if p is not None:
            m = p.build(cnf, target_nodes=target_nodes, max_sep=max_sep,
                        reporting=reporting, prune=prune, meta=meta_dict)
            if m is not None:
                return (family, m)

    # 2) tier A: metadata (sidecar path or inline comment groups)
    m = metadata.try_build(cnf, meta=(meta if not isinstance(meta, dict) else None),
                           target_nodes=target_nodes, max_sep=max_sep,
                           reporting=reporting, prune=prune)
    if m is not None:
        return ("metadata", m)

    # 3) tier B/C: best-confidence plugin auto-detect
    best, best_conf = None, 0.0
    for p in all_plugins():
        try:
            conf = p.detect(cnf, meta=meta_dict)
        except TypeError:
            conf = p.detect(cnf)
        if conf > best_conf:
            best, best_conf = p, conf
    if best is not None and best_conf >= 0.5:
        m = best.build(cnf, target_nodes=target_nodes, max_sep=max_sep,
                       reporting=reporting, prune=prune, meta=meta_dict)
        if m is not None:
            return (best.name, m)

    return None
