#!/usr/bin/env python3
"""Verify and visualise a Y-pentacube packing of an SxSxS cube.

Works with the encoding produced by ``ypack_gen.py``: placement variable i
(1-based) corresponds to ``placements(S)[i-1]`` (deterministic order), so a SAT
model's positive literals in ``[1, #placements]`` are exactly the used pieces.

Usage:
    ypack_verify.py SOLUTION_FILE [--size 5] [--index 0] [--all] [--png OUT.png]
    ypack_verify.py --show-shape        # print the piece + its 24 orientations

SOLUTION_FILE may be a SAT solver model (a line of signed literals ending in 0),
or a dagster .sols file (one model per line); non-integer tokens are ignored.
"""

from __future__ import annotations

import argparse
import sys
from typing import List, Set, Tuple

import ypack_gen as g

Cell = Tuple[int, int, int]


def parse_models(path: str, n_placements: int) -> List[Set[int]]:
    """Return a list of models; each model is the set of used placement vars."""
    models = []
    with open(path, errors="replace") as f:
        for line in f:
            used = set()
            any_int = False
            for tok in line.split():
                try:
                    v = int(tok)
                except ValueError:
                    continue
                any_int = True
                if 1 <= v <= n_placements:
                    used.add(v)
            if any_int and used:
                models.append(used)
    return models


def verify(used: Set[int], places: List[List[Cell]], S: int):
    """Check that `used` placements form an exact packing. Returns (ok, problems,
    grid) where grid[cell_id] = piece index (or -1)."""
    problems = []
    n_cells = S ** 3
    expected_pieces = n_cells // 5
    if len(used) != expected_pieces:
        problems.append("uses {} pieces, expected {}".format(len(used), expected_pieces))

    grid = [-1] * n_cells
    overlaps = 0
    for piece_idx, var in enumerate(sorted(used)):
        cells = places[var - 1]
        # confirm the shape really is a Y-pentacube orientation (by construction
        # it is, but verify for an externally-supplied solution)
        norm = g._normalize(cells)
        if norm not in set(g.orientations()):
            problems.append("placement {} is not a valid Y-pentacube orientation".format(var))
        for c in cells:
            cid = g.cell_id(c, S)
            if grid[cid] != -1:
                overlaps += 1
            grid[cid] = piece_idx
    if overlaps:
        problems.append("{} cell(s) covered by more than one piece".format(overlaps))
    uncovered = sum(1 for v in grid if v == -1)
    if uncovered:
        problems.append("{} cell(s) left uncovered".format(uncovered))
    return (not problems), problems, grid


_LABELS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"


def ascii_render(grid: List[int], S: int) -> str:
    out = []
    for z in range(S):
        out.append("  z = {} (rows=x 0..{}, cols=y 0..{}):".format(z, S - 1, S - 1))
        for x in range(S):
            row = []
            for y in range(S):
                p = grid[g.cell_id((x, y, z), S)]
                row.append(_LABELS[p] if 0 <= p < len(_LABELS) else "?")
            out.append("    " + " ".join(row))
        out.append("")
    return "\n".join(out)


def _mpl():
    try:
        import numpy as np
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        from matplotlib import cm
        return np, plt, cm
    except ImportError:
        return None


def _voxels(grid, S, np, cm):
    filled = np.zeros((S, S, S), dtype=bool)
    colors = np.empty((S, S, S, 4), dtype=float)
    n_pieces = max(grid) + 1
    palette = cm.get_cmap("hsv", max(n_pieces, 1))  # distinct hue per piece
    for cid, p in enumerate(grid):
        if p < 0:
            continue
        x, y, z = cid // (S * S), (cid // S) % S, cid % S
        filled[x, y, z] = True
        colors[x, y, z] = palette(p)
    return filled, colors, palette


def png_render(grid: List[int], S: int, path: str) -> bool:
    m = _mpl()
    if not m:
        return False
    np, plt, cm = m
    filled, colors, _ = _voxels(grid, S, np, cm)
    ax = plt.figure(figsize=(6, 6)).add_subplot(projection="3d")
    ax.voxels(filled, facecolors=colors, edgecolor="k", linewidth=0.3)
    ax.set_title("Y-pentacube packing ({0}x{0}x{0})".format(S))
    plt.savefig(path, dpi=120, bbox_inches="tight")
    plt.close()
    return True


def slices_render(grid: List[int], S: int, path: str) -> bool:
    """Per-layer panels: S coloured S×S grids (rows=x, cols=y), labelled by piece."""
    m = _mpl()
    if not m:
        return False
    np, plt, cm = m
    n_pieces = max(grid) + 1
    palette = cm.get_cmap("hsv", max(n_pieces, 1))
    fig, axes = plt.subplots(1, S, figsize=(3.0 * S, 3.4))
    if S == 1:
        axes = [axes]
    for z in range(S):
        ax = axes[z]
        img = np.zeros((S, S, 4))
        for x in range(S):
            for y in range(S):
                p = grid[g.cell_id((x, y, z), S)]
                img[x, y] = palette(p) if p >= 0 else (1, 1, 1, 1)
                ax.text(y, x, _LABELS[p] if 0 <= p < len(_LABELS) else "?",
                        ha="center", va="center", fontsize=9)
        ax.imshow(img, origin="upper")
        ax.set_title("z = {}".format(z))
        ax.set_xticks(range(S)); ax.set_yticks(range(S))
        ax.set_xlabel("y"); ax.set_ylabel("x")
    fig.suptitle("Y-pentacube packing -- layers (same colour = same piece)")
    plt.tight_layout()
    plt.savefig(path, dpi=120, bbox_inches="tight")
    plt.close()
    return True


def gif_render(grid: List[int], S: int, path: str, frames: int = 60) -> bool:
    """Rotating 3D voxel animation."""
    m = _mpl()
    if not m:
        return False
    np, plt, cm = m
    from matplotlib.animation import FuncAnimation, PillowWriter
    filled, colors, _ = _voxels(grid, S, np, cm)
    fig = plt.figure(figsize=(6, 6))
    ax = fig.add_subplot(projection="3d")
    ax.voxels(filled, facecolors=colors, edgecolor="k", linewidth=0.3)
    ax.set_title("Y-pentacube packing ({0}x{0}x{0})".format(S))

    def update(i):
        ax.view_init(elev=22, azim=i * (360 // frames))
        return []

    anim = FuncAnimation(fig, update, frames=frames, blit=False)
    anim.save(path, writer=PillowWriter(fps=15))
    plt.close()
    return True


def show_shape():
    print("Y-pentacube base shape (bar of 4 + foot on the 2nd cube):")
    print("  cells:", g.BASE)
    orients = g.orientations()
    print("\n{} distinct orientations under the 24 cube rotations.".format(len(orients)))
    print("orientation 0 as layers (the canonical piece):")
    o = orients[0]
    mx = max(c[0] for c in o) + 1
    my = max(c[1] for c in o) + 1
    mz = max(c[2] for c in o) + 1
    for z in range(mz):
        print("  z={}:".format(z))
        for x in range(mx):
            print("    " + " ".join("#" if (x, y, z) in o else "." for y in range(my)))


def main():
    ap = argparse.ArgumentParser(description="Verify/visualise a Y-pentacube packing")
    ap.add_argument("solution", nargs="?", help="SAT model / .sols file")
    ap.add_argument("--size", type=int, default=5)
    ap.add_argument("--index", type=int, default=0, help="which model in the file")
    ap.add_argument("--all", action="store_true", help="verify every model in the file")
    ap.add_argument("--png", default=None, help="write a 3D voxel render to this PNG")
    ap.add_argument("--slices", default=None, help="write per-layer slice panels to this PNG")
    ap.add_argument("--gif", default=None, help="write a rotating 3D animation to this GIF")
    ap.add_argument("--show-shape", action="store_true")
    args = ap.parse_args()

    if args.show_shape:
        show_shape()
        return

    if not args.solution:
        ap.error("a SOLUTION_FILE is required (or use --show-shape)")

    S = args.size
    places = g.placements(S)
    models = parse_models(args.solution, len(places))
    if not models:
        print("no models found in {}".format(args.solution), file=sys.stderr)
        sys.exit(1)
    print("parsed {} model(s); {} placements for S={}".format(len(models), len(places), S))

    idxs = range(len(models)) if args.all else [args.index]
    all_ok = True
    for i in idxs:
        ok, problems, grid = verify(models[i], places, S)
        print("\nmodel #{}: {}".format(i, "VALID PACKING" if ok else "INVALID"))
        for p in problems:
            print("  - " + p)
        all_ok = all_ok and ok
        if ok and (not args.all or len(idxs) == 1):
            print(ascii_render(grid, S))
            for flag, fn, label in [(args.png, png_render, "3D render"),
                                    (args.slices, slices_render, "layer slices"),
                                    (args.gif, gif_render, "rotating GIF")]:
                if flag:
                    if fn(grid, S, flag):
                        print("wrote {}: {}".format(label, flag))
                    else:
                        print("(matplotlib/numpy not installed -> skipped {})".format(label))
    sys.exit(0 if all_ok else 2)


if __name__ == "__main__":
    main()
