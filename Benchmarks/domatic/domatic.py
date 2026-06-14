#!/usr/bin/env python3
"""Domatic-number CNF generator for the n-dimensional Hamming cube Q_n.

Q_n: vertices = {0,1}^n (the integers 0..2^n-1), edges between Hamming-distance-1
vertices. The DOMATIC NUMBER is the largest k such that the vertices can be
partitioned into k disjoint DOMINATING SETs. "Is the domatic number of Q_n >= k?"
is the SAT question this encodes (k disjoint dominating sets <=> a proper k-colouring
where every colour class dominates). The open research target is Q_10.

Encoding (matches the project's hand-made domatic instances):
  var(v,c) = K*v + c        vertex v has colour c   (v in 0..2^n-1, c in 1..K)
  exactly-one colour per vertex
  domination: for every vertex v and colour c, some u in the CLOSED neighbourhood
              N[v] = {v} u {v xor 2^i} has colour c  (so colour c dominates v)

Symmetry (this is the point -- the cube has a huge automorphism group):
  * colour symmetry S_K: the k dominating sets are interchangeable.
  * cube automorphisms (hyperoctahedral group, order 2^n * n!): bit permutations
    (S_n on coordinates) and bit flips ((Z_2)^n). Each maps a valid colouring to a
    valid colouring.
--symbreak breaks these soundly (each keeps >=1 representative per orbit):
  none   raw encoding (full symmetry; BreakID finds the cube group on this --
         e.g. 14 generators on Q_8,k=6).
  colour value-precedence on colours -- canonical least-colour-first labelling.
         Fully breaks S_K (subsumes "vertex 0 = colour 1") AND, because the
         precedence clauses impose a vertex order, incidentally breaks the cube
         automorphisms too: BreakID finds 0 residual generators after it (Q_3,
         Q_6, Q_8 all checked). Measured: Q_8,k=6 raw 9.9s -> colour 3.8s (2.6x,
         same verdict); colour MATCHES BreakID-on-raw's solve time while adding
         ~13x fewer clauses (1280 vs ~14000) and BreakID then finds nothing more
         to break. So the structure-aware breaking subsumes the generic
         graph-automorphism path at a fraction of the cost. Sound, scales.
  cube   reserved for an explicit cube-automorphism lex-leader layer. Not
         implemented: empirically the colour layer already drives BreakID's
         detectable symmetry to 0, so a separate cube layer has no measured
         marginal value yet (revisit if Q_9/Q_10 show residual structure).

  python3 domatic.py N K [--symbreak colour] -o out.cnf [--map out.map]
"""

import argparse
import sys


def closed_neighbourhood(v, n):
    """v and its n Hamming-neighbours (closed neighbourhood in Q_n)."""
    return [v] + [v ^ (1 << i) for i in range(n)]


def generate(n, k, symbreak="none"):
    """Return (nvars, clauses) for 'domatic number of Q_n >= k?'."""
    V = 1 << n                       # 2^n vertices
    def var(v, c):                   # c in 1..k
        return k * v + c
    nvars = k * V
    clauses = []

    # exactly one colour per vertex
    for v in range(V):
        clauses.append([var(v, c) for c in range(1, k + 1)])          # at least one
        for a in range(1, k + 1):
            for b in range(a + 1, k + 1):
                clauses.append([-var(v, a), -var(v, b)])              # at most one

    # domination: every colour dominates every vertex (closed neighbourhood)
    for v in range(V):
        nb = closed_neighbourhood(v, n)
        for c in range(1, k + 1):
            clauses.append([var(u, c) for u in nb])

    # --- symmetry breaking -------------------------------------------------
    if symbreak in ("colour", "color", "full"):
        # Value precedence (Crawford et al.): colour c may be used by vertex v only
        # if colour c-1 is used by some EARLIER vertex. Canonicalises the colour
        # labelling -> fully breaks S_K. (For v=0, c>=2 this is the unit -var(0,c),
        # i.e. vertex 0 takes colour 1.) Sound: every orbit keeps its least-colour
        # representative.
        for c in range(2, k + 1):
            for v in range(V):
                clauses.append([-var(v, c)] + [var(w, c - 1) for w in range(v)])

    return nvars, clauses


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("n", type=int, help="cube dimension (Q_n has 2^n vertices)")
    ap.add_argument("k", type=int, help="number of colours / dominating sets (the domatic-number target)")
    ap.add_argument("--symbreak", choices=["none", "colour", "color", "cube", "full"], default="none")
    ap.add_argument("-o", "--out", default=None, help="output CNF (default: domatic_<n>_<k>[_sb].cnf)")
    ap.add_argument("--map", default=None, help="also write a var->(*vertex,colour) map")
    args = ap.parse_args()
    if args.symbreak == "cube":
        sys.exit("--symbreak cube is not implemented yet (cube-automorphism breaking is the next layer)")

    nvars, clauses = generate(args.n, args.k, args.symbreak)
    tag = "" if args.symbreak == "none" else "_sb"
    out = args.out or "domatic_%d_%d%s.cnf" % (args.n, args.k, tag)
    with open(out, "w") as f:
        f.write("p cnf %d %d\n" % (nvars, len(clauses)))
        for cl in clauses:
            f.write(" ".join(map(str, cl)) + " 0\n")
    print("domatic Q_%d, k=%d, symbreak=%s -> %s  (%d vars, %d clauses)"
          % (args.n, args.k, args.symbreak, out, nvars, len(clauses)))
    if args.map:
        with open(args.map, "w") as f:
            for v in range(1 << args.n):
                for c in range(1, args.k + 1):
                    f.write("%d v%d c%d\n" % (args.k * v + c, v, c))
        print("  map -> %s" % args.map)


if __name__ == "__main__":
    main()
