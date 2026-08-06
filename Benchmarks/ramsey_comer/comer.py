#!/usr/bin/env python3
"""Comer/Cayley-scheme SAT encoder for Ramsey relation-algebra representability.

Instead of colouring all ~N^2 edges of K_N (the raw encoding -> millions of
clauses), we search only for VERTEX-TRANSITIVE (Cayley) representations: put the
vertices on the elements of a finite group G (order n) and colour edge {x,y} by
the colour of the "difference" x^-1 y.  A colouring is then just a symmetric map
    c : G\\{e} -> {1..M},   c(g) = c(g^-1)
and the whole problem collapses from ~N^2 edge variables to ~n/2 * M element
variables -- and the vertex symmetry is baked in for free.  This is the method
(generalised Comer schemes) that is actually cracking the sibling flexible-atom
algebras; strict cyclic/field constructions are ruled out for M=8/13, but an
ARBITRARY symmetric partition (what the SAT solver searches here) subsumes them.

A representation of the M-colour Ramsey (Monk) algebra requires:
  (1) exactly one colour per non-identity element, symmetric under inverse;
  (2) NO monochromatic triangle: no a,b with c(a)=c(b)=c(ab) (a,b,ab != e);
  (3) FLEXIBILITY -- every non-monochromatic triangle appears over EVERY edge:
      for every difference d and every colour pair {i,j} with (i,j,c(d)) not
      monochromatic, some vertex y gives c(y)=i and c(y^-1 d)=j.
By left-translation invariance (colour depends only on the difference) it
suffices to post (2) over triangles through the identity and (3) over each
difference d -- the payoff of vertex-transitivity.

A SAT model is a genuine Cayley representation (verified independently by
`verify`).  Groups are pluggable via the Group protocol; CyclicGroup(n) first.

  comer.py --colours 3 --n 13 [--solve] [--symbreak]      # one instance
  comer.py --validate                                      # reproduce known M=3..7
"""

import argparse
import itertools
import os
import subprocess
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
CADICAL = os.path.join(HERE, "..", "..", "dagster", "cadical_solver", "cadical", "build", "cadical")


# --------------------------------------------------------------------------
# groups (pluggable: cyclic now, non-abelian later via a multiplication table)
# --------------------------------------------------------------------------
class CyclicGroup:
    """Z_n under addition. Identity 0; op = +, inv = negation (mod n)."""
    def __init__(self, n):
        self.n = n
        self.e = 0
        self.elements = list(range(n))
    def op(self, a, b):
        return (a + b) % self.n
    def inv(self, a):
        return (-a) % self.n
    def name(self):
        return "Z%d" % self.n


class DihedralGroup:
    """D_m, the symmetries of an m-gon (order 2m). Non-abelian for m >= 3.
    Elements 0..m-1 are rotations r^i; m..2m-1 are reflections s*r^i."""
    def __init__(self, m):
        self.m = m
        self.e = 0
        self.elements = list(range(2 * m))
    def op(self, a, b):
        m = self.m
        ra, fa = a % m, a >= m
        rb, fb = b % m, b >= m
        if not fa and not fb: rot, refl = (ra + rb) % m, False
        elif not fa and fb:   rot, refl = (rb - ra) % m, True
        elif fa and not fb:   rot, refl = (ra + rb) % m, True
        else:                 rot, refl = (rb - ra) % m, False
        return rot + (m if refl else 0)
    def inv(self, a):
        m = self.m
        return a if a >= m else (-a) % m           # reflections are involutions
    def name(self):
        return "D%d" % self.m


class DirectProduct:
    """G x H. Elements encoded as g*|H| + h."""
    def __init__(self, G, H):
        self.G, self.H = G, H
        self.hn = len(H.elements)
        self.e = G.e * self.hn + H.e
        self.elements = list(range(len(G.elements) * self.hn))
    def _dec(self, a):
        return a // self.hn, a % self.hn
    def op(self, a, b):
        ga, ha = self._dec(a); gb, hb = self._dec(b)
        return self.G.op(ga, gb) * self.hn + self.H.op(ha, hb)
    def inv(self, a):
        ga, ha = self._dec(a)
        return self.G.inv(ga) * self.hn + self.H.inv(ha)
    def name(self):
        return "%sx%s" % (self.G.name(), self.H.name())


class TableGroup:
    """A group given by its Cayley table (table[a][b] = index of a*b). Lets us
    plug in ANY finite group -- in particular every group of an order, dumped
    from GAP's SmallGroups library (see gap_groups)."""
    def __init__(self, table, name="G"):
        self.table = table
        self.n = len(table)
        self.elements = list(range(self.n))
        self._name = name
        self.e = next(a for a in range(self.n) if all(table[a][b] == b for b in range(self.n)))
        self._inv = [next(b for b in range(self.n) if table[a][b] == self.e) for a in range(self.n)]
    def op(self, a, b):
        return self.table[a][b]
    def inv(self, a):
        return self._inv[a]
    def name(self):
        return self._name


def gap_available():
    import shutil
    return shutil.which("gap") is not None


def gap_groups(order, cache_dir=None):
    """Yield a TableGroup for EVERY group of the given order, from GAP's
    SmallGroups library (the exhaustive enumeration). Results are cached per
    order under groups_cache/ so GAP runs at most once per order. Returns []
    (with a note) if GAP isn't installed."""
    if cache_dir is None:
        cache_dir = os.path.join(HERE, "groups_cache")
    os.makedirs(cache_dir, exist_ok=True)
    cache = os.path.join(cache_dir, "order_%d.txt" % order)
    if not os.path.exists(cache):
        if not gap_available():
            return []
        # GAP program: dump each group's Cayley table (0-based indices), row-major.
        # SetPrintFormattingStatus(...,false) disables GAP's line-wrapping so long
        # rows don't split; the parser below is wrapping-agnostic regardless.
        prog = (
            "SetPrintFormattingStatus(\"*stdout*\", false);;\n"
            "n := %d;;\n"
            "for i in [1..NrSmallGroups(n)] do\n"
            "  G := SmallGroup(n, i);; elts := Elements(G);;\n"
            "  Print(\"GROUP \", n, \" \", i, \"\\n\");;\n"
            "  for a in [1..n] do\n"
            "    for b in [1..n] do Print(Position(elts, elts[a]*elts[b])-1, \" \"); od;;\n"
            "  od;;\n"
            "  Print(\"\\nEND\\n\");;\n"
            "od;;\nQUIT;;\n" % order
        )
        p = subprocess.run(["gap", "-q", "-b"], input=prog, capture_output=True, text=True, timeout=1800)
        with open(cache, "w") as f:
            f.write(p.stdout)
    # parse: for each GROUP block, gather ALL integer tokens until END (robust to
    # any line wrapping) and reshape into the n x n Cayley table.
    groups = []
    toks = open(cache).read().split()
    i = 0
    while i < len(toks):
        if toks[i] == "GROUP":
            n, gid = int(toks[i + 1]), toks[i + 2]
            i += 3
            flat = []
            while i < len(toks) and toks[i] != "END":
                flat.append(int(toks[i])); i += 1
            i += 1                           # skip END
            if len(flat) != n * n:
                raise ValueError("order %d group %s: got %d table entries, want %d"
                                 % (n, gid, len(flat), n * n))
            table = [flat[r * n:(r + 1) * n] for r in range(n)]
            groups.append(TableGroup(table, name="G%d_%s" % (n, gid)))
        else:
            i += 1
    return groups


def check_group(G):
    """Sanity-check the group axioms (closure, identity, inverse, associativity)
    so an encoding bug can't masquerade as a maths result. Returns (ok, reason)."""
    els = G.elements
    S = set(els)
    for a in els:
        if G.op(a, G.e) != a or G.op(G.e, a) != a:
            return (False, "identity fails at %s" % a)
        if G.op(a, G.inv(a)) != G.e or G.op(G.inv(a), a) != G.e:
            return (False, "inverse fails at %s" % a)
        for b in els:
            if G.op(a, b) not in S:
                return (False, "not closed at (%s,%s)" % (a, b))
    # associativity on a sample (full check is O(n^3); sample keeps it cheap)
    import itertools as _it
    sample = els if len(els) <= 20 else els[::max(1, len(els) // 20)]
    for a, b, c in _it.product(sample, repeat=3):
        if G.op(G.op(a, b), c) != G.op(a, G.op(b, c)):
            return (False, "assoc fails at (%s,%s,%s)" % (a, b, c))
    return (True, "ok")


def semidirect_products(order):
    """Yield Z_n : Z_m for n*m = order and every twist t (t^m = 1 mod n, t != 1).
    Semidirect products are the main source of non-abelian groups; this covers
    dihedral (t = -1) and much more. Isomorphic duplicates only cost time."""
    import math
    for m in range(2, order):
        if order % m:
            continue
        n = order // m
        if n < 2:
            continue
        for t in range(2, n):
            if math.gcd(t, n) != 1 or pow(t, m, n) != 1:
                continue
            enc = lambda a, b: a * m + b
            table = [[0] * order for _ in range(order)]
            for a1 in range(n):
                for b1 in range(m):
                    tb1 = pow(t, b1, n)
                    for a2 in range(n):
                        for b2 in range(m):
                            table[enc(a1, b1)][enc(a2, b2)] = enc((a1 + tb1 * a2) % n, (b1 + b2) % m)
            yield TableGroup(table, name="Z%d:Z%d(t%d)" % (n, m, t))


def catalog(order):
    """Yield the groups of a given order. If GAP is installed, this is the
    EXHAUSTIVE SmallGroups enumeration (every group of that order). Otherwise it
    falls back to the families we build directly -- cyclic + direct products +
    semidirect products (dihedral included) -- a broad but NOT exhaustive sweep.
    Deduplication across isomorphic builds is left to the solver (only costs time)."""
    if gap_available():
        yield from gap_groups(order)                                # exhaustive
        return
    yield CyclicGroup(order)
    yield from semidirect_products(order)                           # non-abelian families
    for a in range(2, order):                                       # a few direct products
        if order % a == 0:
            b = order // a
            if 2 <= b <= a:
                yield DirectProduct(CyclicGroup(a), CyclicGroup(b))


def inverse_classes(G):
    """Partition non-identity elements into {g, g^-1} classes. Returns
    (classes, elem2class): classes is a list of sorted tuples (the canonical
    representative is class[0]); elem2class maps every non-identity element to
    its class index."""
    seen = set()
    classes = []
    for g in G.elements:
        if g == G.e or g in seen:
            continue
        gi = G.inv(g)
        cls = tuple(sorted({g, gi}))     # {g} if self-inverse (g = g^-1)
        for x in cls:
            seen.add(x)
        classes.append(cls)
    elem2class = {}
    for idx, cls in enumerate(classes):
        for x in cls:
            elem2class[x] = idx
    return classes, elem2class


# --------------------------------------------------------------------------
# encoding
# --------------------------------------------------------------------------
class Encoder:
    def __init__(self):
        self._n = 0
        self.clauses = []
    def new_var(self):
        self._n += 1
        return self._n
    def add(self, *lits):
        self.clauses.append(list(lits))
    @property
    def nvars(self):
        return self._n


def encode(G, M, symbreak=True):
    """Return (enc, x) where enc is an Encoder holding the CNF and
    x[class_idx][colour] (colour in 1..M) is the colour-assignment var id."""
    classes, e2c = inverse_classes(G)
    nc = len(classes)
    enc = Encoder()

    # colour-assignment vars: x[k][c] = "difference-class k has colour c"
    x = [[enc.new_var() for _ in range(M + 1)] for _ in range(nc)]  # index colour 1..M

    # (1) exactly one colour per class
    for k in range(nc):
        enc.add(*[x[k][c] for c in range(1, M + 1)])                 # at least one
        for c1 in range(1, M + 1):
            for c2 in range(c1 + 1, M + 1):
                enc.add(-x[k][c1], -x[k][c2])                        # at most one

    # (2) no monochromatic triangle. By translation invariance, only triangles
    # through the identity: {e, a, b} for distinct non-identity a,b. Its three
    # edge-differences are a, b, and a^-1 b (edge {a,b}); forbid all-same-colour.
    nonid = [g for g in G.elements if g != G.e]
    seen_tri = set()
    for a, b in itertools.combinations(nonid, 2):
        d3 = G.op(G.inv(a), b)                                       # difference of edge {a,b}
        if d3 == G.e:
            continue
        ka, kb, kd = e2c[a], e2c[b], e2c[d3]
        sig = tuple(sorted((ka, kb, kd)))
        if sig in seen_tri:
            continue
        seen_tri.add(sig)
        for c in range(1, M + 1):
            enc.add(-x[ka][c], -x[kb][c], -x[kd][c])                 # not all colour c

    # (3) flexibility, per edge/difference d. Over edge {e,d}, a witness vertex y
    # contributes edges of colour c(y) and c(y^-1 d). We need every non-mono
    # colour pair {i,j} realised. Witness var v -> (c(y)=i AND c(y^-1 d)=j).
    for d in nonid:
        # candidate witnesses y (so that y and y^-1 d are both non-identity)
        ys = [y for y in nonid if G.op(G.inv(y), d) != G.e]
        # precompute the class pair each witness realises
        wit = [(e2c[y], e2c[G.op(G.inv(y), d)]) for y in ys]         # (class of y, class of y^-1 d)
        kd = e2c[d]
        for i in range(1, M + 1):
            for j in range(i, M + 1):
                lits = []
                for (ky, kv) in wit:
                    v = enc.new_var()
                    enc.add(-v, x[ky][i])                            # v -> c(y)=i
                    enc.add(-v, x[kv][j])                            # v -> c(y^-1 d)=j
                    lits.append(v)
                if i == j:
                    # mono pair (i,i): required UNLESS d itself is colour i
                    # (then (i,i,i) is the forbidden mono triangle, excused).
                    enc.add(x[kd][i], *lits)
                else:
                    enc.add(*lits)                                   # at least one witness

    # colour-permutation symmetry breaking (Crawford value precedence): colour c
    # may be used by class k only if colour c-1 is used by some earlier class.
    if symbreak:
        for c in range(2, M + 1):
            for k in range(nc):
                enc.add(-x[k][c], *[x[w][c - 1] for w in range(k)])

    return enc, x, classes, e2c


def write_dimacs(enc, path):
    with open(path, "w") as f:
        f.write("p cnf %d %d\n" % (enc.nvars, len(enc.clauses)))
        for cl in enc.clauses:
            f.write(" ".join(map(str, cl)) + " 0\n")


# --------------------------------------------------------------------------
# solve + decode
# --------------------------------------------------------------------------
def solve(enc, timeout=120):
    """Run standalone CaDiCaL. Returns (verdict, model) where verdict in
    {SAT,UNSAT,TIMEOUT,ERR} and model is a set of true var ids (SAT only)."""
    cnf = tempfile.mktemp(suffix=".cnf")
    write_dimacs(enc, cnf)
    try:
        p = subprocess.run([CADICAL, cnf], capture_output=True, text=True, timeout=timeout)
    except subprocess.TimeoutExpired:
        return ("TIMEOUT", None)
    finally:
        if os.path.exists(cnf):
            os.remove(cnf)
    if p.returncode == 10:
        model = set()
        for line in p.stdout.splitlines():
            if line.startswith("v "):
                for tok in line[2:].split():
                    lit = int(tok)
                    if lit > 0:
                        model.add(lit)
        return ("SAT", model)
    if p.returncode == 20:
        return ("UNSAT", None)
    return ("ERR(%d)" % p.returncode, None)


def decode_colouring(G, M, x, classes, model):
    """Return colouring: dict element -> colour (1..M) for every non-identity
    element, from a SAT model."""
    col = {}
    for k, cls in enumerate(classes):
        c = next((cc for cc in range(1, M + 1) if x[k][cc] in model), None)
        for elem in cls:
            col[elem] = c
    return col


# --------------------------------------------------------------------------
# independent verifier (does NOT reuse the encoder -- the whole point)
# --------------------------------------------------------------------------
def verify(G, M, col):
    """Check `col` is a genuine Cayley representation of the M-colour Ramsey
    algebra. Returns (ok, reason)."""
    nonid = [g for g in G.elements if g != G.e]
    # symmetric + a colour in range for every non-identity element
    for g in nonid:
        if col.get(g) not in range(1, M + 1):
            return (False, "element %s uncoloured/out of range" % g)
        if col[g] != col[G.inv(g)]:
            return (False, "not symmetric at %s" % g)
    # every colour actually used (a representation uses all M atoms)
    used = {col[g] for g in nonid}
    if used != set(range(1, M + 1)):
        return (False, "colours used = %s (need all 1..%d)" % (sorted(used), M))
    # (2) no monochromatic triangle (all triangles through e, by invariance)
    for a, b in itertools.combinations(nonid, 2):
        d3 = G.op(G.inv(a), b)
        if d3 == G.e:
            continue
        if col[a] == col[b] == col[d3]:
            return (False, "mono triangle {e,%s,%s} colour %d" % (a, b, col[a]))
    # (3) flexibility: over every edge d, every non-mono colour pair {i,j} present
    for d in nonid:
        kd = col[d]
        present = set()
        for y in nonid:
            v = G.op(G.inv(y), d)
            if v == G.e:
                continue
            present.add(frozenset((col[y], col[v])))                # unordered {c(y),c(v)}
        for i in range(1, M + 1):
            for j in range(i, M + 1):
                if i == j and i == kd:
                    continue                                        # mono (i,i,i) legitimately absent
                if frozenset((i, j)) not in present:
                    return (False, "edge d=%s (colour %d) missing triangle {%d,%d}" % (d, kd, i, j))
    return (True, "ok")


# --------------------------------------------------------------------------
# drivers
# --------------------------------------------------------------------------
def attempt(G, M, symbreak=True, timeout=120, quiet=False):
    ok, reason = check_group(G)
    if not ok:
        raise ValueError("%s is not a valid group: %s" % (G.name(), reason))
    enc, x, classes, e2c = encode(G, M, symbreak=symbreak)
    verdict, model = solve(enc, timeout=timeout)
    info = "%s M=%d : %d vars, %d clauses -> %s" % (G.name(), M, enc.nvars, len(enc.clauses), verdict)
    if verdict == "SAT":
        col = decode_colouring(G, M, x, classes, model)
        ok, reason = verify(G, M, col)
        info += "  [verify: %s]" % ("REPRESENTATION OK" if ok else "INVALID -- " + reason)
        if not quiet:
            print(info)
        return ("REPRESENTATION" if ok else "SAT-BUT-INVALID"), col
    if not quiet:
        print(info)
    return verdict, None


def validate():
    """Reproduce known small Cayley representations: for M=3..7, sweep cyclic
    group order n and report the smallest n that yields a VERIFIED representation.
    (These M are known-representable, so a correct encoder must find them.)"""
    print("Validation: smallest cyclic (Z_n) representation per M (independently verified)\n")
    for M in range(3, 8):
        found = None
        for n in range(M + 1, 61):           # a rep needs > M elements; cap the sweep
            res, col = attempt(CyclicGroup(n), M, timeout=60, quiet=True)
            if res == "REPRESENTATION":
                found = (n, col)
                print("  M=%d : FOUND on Z_%d  (verified) " % (M, n))
                break
            if res == "SAT-BUT-INVALID":
                print("  M=%d : Z_%d SAT but verify FAILED -- ENCODER BUG" % (M, n))
                return
        if not found:
            print("  M=%d : no cyclic representation up to Z_60" % M)


def sweep(M, n_lo, n_hi, timeout=120, symbreak=True):
    """Sweep every group in the catalog for orders n_lo..n_hi, looking for a
    verified representation of the M-colour Ramsey algebra. Reports SAT/UNSAT/
    TIMEOUT per group; a REPRESENTATION resolves the problem. UNSAT results are
    lower-bound data (certifiable later via a DRAT proof)."""
    print("Sweep: M=%d colours, groups of order %d..%d\n" % (M, n_lo, n_hi))
    found = []
    for order in range(n_lo, n_hi + 1):
        for G in catalog(order):
            try:
                res, col = attempt(G, M, symbreak=symbreak, timeout=timeout, quiet=True)
            except ValueError as e:
                print("  skip %s: %s" % (G.name(), e)); continue
            tag = {"REPRESENTATION": "*** REPRESENTATION ***", "SAT-BUT-INVALID": "SAT/INVALID(bug?)"}.get(res, res)
            print("  n=%-3d %-12s %s" % (order, G.name(), tag))
            if res == "REPRESENTATION":
                found.append((G.name(), col))
    print("\n%s" % ("FOUND: " + ", ".join(g for g, _ in found) if found else
                     "no representation found in this sweep (all UNSAT/TIMEOUT)"))
    return found


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--colours", "-M", type=int, help="number of colours M")
    ap.add_argument("--n", type=int, help="group order n")
    ap.add_argument("--group", choices=["cyclic", "dihedral"], default="cyclic",
                    help="group for a single --n run (dihedral: D_{n/2}, needs n even)")
    ap.add_argument("--no-symbreak", action="store_true")
    ap.add_argument("--timeout", type=int, default=120)
    ap.add_argument("-o", "--out", help="just write the CNF here (no solve)")
    ap.add_argument("--validate", action="store_true", help="reproduce known M=3..7 representations")
    ap.add_argument("--sweep", nargs=2, type=int, metavar=("N_LO", "N_HI"),
                    help="sweep all catalog groups of orders N_LO..N_HI for --colours M")
    args = ap.parse_args()

    if args.validate:
        validate()
        return
    if args.sweep:
        if args.colours is None:
            ap.error("--sweep needs --colours M")
        sweep(args.colours, args.sweep[0], args.sweep[1],
              timeout=args.timeout, symbreak=not args.no_symbreak)
        return
    if args.colours is None or args.n is None:
        ap.error("give --colours and --n (or --validate / --sweep)")
    if args.group == "dihedral":
        if args.n % 2:
            ap.error("dihedral needs even --n (order 2m)")
        G = DihedralGroup(args.n // 2)
    else:
        G = CyclicGroup(args.n)
    if args.out:
        enc, *_ = encode(G, args.colours, symbreak=not args.no_symbreak)
        write_dimacs(enc, args.out)
        print("wrote %s  (%d vars, %d clauses)" % (args.out, enc.nvars, len(enc.clauses)))
        return
    attempt(G, args.colours, symbreak=not args.no_symbreak, timeout=args.timeout)


if __name__ == "__main__":
    main()
