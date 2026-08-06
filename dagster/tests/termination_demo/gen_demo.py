#!/usr/bin/env python3
"""Generate a minimal instance that demonstrates the non-interruptible-solve
termination bug (see README.md).

Two DISJOINT single-node subgraphs, solved concurrently by two workers:
  * node 0: a trivially-SAT formula (vars 1,2)   -> solved in ~microseconds
  * node 1: pigeonhole H holes / H+1 pigeons on  -> UNSAT, exponentially hard for
            DISJOINT variables                       CDCL; a worker grinds on it

With `-e 0` (exit on the first terminal-node solution), node 0's instant solution
should end the run immediately. It does with an interruptible backend (tinisat
`-m 0`, which yields), but a backend whose solve() runs to completion and never
yields (minisat `-m 4`, and cadical/ipasir/cryptominisat before the --yield-seconds
fix) leaves its worker stuck in node 1's solve; the master's "wait for every worker
to check in before exit" then blocks until the pigeonhole finishes -- so the run
never terminates even though the answer was found instantly.

Usage:  python3 gen_demo.py [H]     # default H=12; raise H for a longer hang
"""
import sys

H = int(sys.argv[1]) if len(sys.argv) > 1 else 12      # pigeonhole holes (H+1 pigeons -> UNSAT)

clauses = [[1, 2], [-1, 2]]                            # node 0: easy SAT over vars 1,2
n0 = len(clauses)                                      # -> clause indices 0..n0-1
base = 2                                               # node 1's pigeonhole vars start at 3
def v(p, hole):
    return base + p * H + hole + 1
for p in range(H + 1):                                 # each pigeon in some hole
    clauses.append([v(p, hole) for hole in range(H)])
for hole in range(H):                                  # no two pigeons share a hole
    for a in range(H + 1):
        for b in range(a + 1, H + 1):
            clauses.append([-v(a, hole), -v(b, hole)])
nv = base + (H + 1) * H

with open("demo.cnf", "w") as f:
    f.write("p cnf %d %d\n" % (nv, len(clauses)))
    for c in clauses:
        f.write(" ".join(map(str, c)) + " 0\n")
# 2-node DAG with NO edges -> two disjoint subgraphs the master hands to 2 workers
with open("demo.dag", "w") as f:
    f.write("DAG-FILE\nNODES:2\nGRAPH:\nCLAUSES:\n")
    f.write("0:0-%d\n" % (n0 - 1))                     # node 0 = the easy clauses
    f.write("1:%d-%d\n" % (n0, len(clauses) - 1))      # node 1 = the pigeonhole
    f.write("REPORTING:\n1-%d\n" % nv)
print("wrote demo.cnf (%d vars, %d clauses) + demo.dag  [node0 easy: 0-%d ; node1 php%d: %d-%d]"
      % (nv, len(clauses), n0 - 1, H, n0, len(clauses) - 1))
