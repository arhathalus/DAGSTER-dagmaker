"""Symmetry breaking via BreakID (vendored under utilities/symbreak/breakid).

Runs the standalone BreakID preprocessor on a DIMACS CNF, producing an augmented
CNF with symmetry-breaking (lex-leader) clauses. BreakID detects the symmetry
group automatically (graph automorphism via the bundled bliss) and emits breaking
predicates for the generators it finds.

The catch for Dagster: symmetry-breaking chains COUPLE variables, which raises the
formula's treewidth / separator width -- exactly what Dagster's per-node cost is
exponential in. So maximal breaking shrinks the search space but can wreck the DAG
decomposition (fat, serial nodes that can't spread across the HPC). We therefore
expose a spectrum that trades space against decomposability:

  none  -- no breaking. Best DAG / parallelism, largest search space.
  light -- break only LOCAL point symmetries: disable global matrix/row
           interchangeability (--row false) and cap the breaking size (-s).
           Minimal added coupling, keeps the formula decomposable.
  full  -- break everything BreakID finds (defaults). Smallest space, but the
           breaking chains couple variables and can widen DAG separators.

We also parse the detected generators so callers can report how "global" the
symmetry is: a generator with large variable support (e.g. a coordinate
permutation touching the whole problem) is the kind whose breaking hurts the DAG,
whereas small-support generators are cheap to break.
"""

import os
import re
import subprocess

_HERE = os.path.dirname(os.path.abspath(__file__))
# dagmaker/ lives at utilities/dag-generator/dagmaker; BreakID at utilities/symbreak/breakid
_BREAKID_BUILD = os.path.normpath(
    os.path.join(_HERE, "..", "..", "symbreak", "breakid", "build"))
BREAKID = os.path.join(_BREAKID_BUILD, "breakid")
# the breakid binary links libbreakid.so (in build/lib); add it to the loader path
# so the tool works regardless of where the repo lives (the build rpath is absolute).
_BREAKID_LIB = os.path.join(_BREAKID_BUILD, "lib")


def _breakid_env():
    env = dict(os.environ)
    sep = os.pathsep
    env["LD_LIBRARY_PATH"] = _BREAKID_LIB + sep + env.get("LD_LIBRARY_PATH", "")
    return env

LEVELS = ("none", "light", "full")

# breaking aggressiveness per level. `-s` caps auxiliary vars (breaking-chain
# size); `--row false` skips the global matrix/row-interchange breaking that
# couples the most.
_LEVEL_ARGS = {
    "light": ["-s", "20", "--row", "false"],
    "full":  ["-s", "50", "--row", "true"],
}


class SymBreakResult:
    """Outcome of a BreakID run."""

    def __init__(self, out_cnf, supports, dims_before, dims_after, stdout):
        self.out_cnf = out_cnf
        self.supports = supports                       # list[set[int]] -- generator supports
        self.num_generators = len(supports)
        self.vars_before, self.clauses_before = dims_before
        self.vars_after, self.clauses_after = dims_after
        self.stdout = stdout

    @property
    def clauses_added(self):
        return self.clauses_after - self.clauses_before

    @property
    def aux_vars_added(self):
        return self.vars_after - self.vars_before

    def support_sizes(self):
        return sorted((len(s) for s in self.supports), reverse=True)

    def n_global(self, frac=0.25):
        """How many generators have 'global' support (touch >= frac of all vars).
        These are the decomposition-hostile ones."""
        if self.vars_before <= 0:
            return 0
        thresh = max(2, int(frac * self.vars_before))
        return sum(1 for s in self.supports if len(s) >= thresh)

    def summary(self):
        sizes = self.support_sizes()
        return (
            "{} generators (support sizes: {}{}); {} global (>=25% vars); "
            "+{} breaking clauses, +{} aux vars ({} -> {} clauses, {} -> {} vars)"
            .format(self.num_generators,
                    ", ".join(str(s) for s in sizes[:8]),
                    " ..." if len(sizes) > 8 else "",
                    self.n_global(),
                    self.clauses_added, self.aux_vars_added,
                    self.clauses_before, self.clauses_after,
                    self.vars_before, self.vars_after))


def available():
    return os.path.exists(BREAKID)


def _cnf_dims(path):
    """Return (n_vars, n_clauses) from the 'p cnf V C' header."""
    with open(path, errors="replace") as f:
        for line in f:
            if line.startswith("p cnf"):
                parts = line.split()
                return int(parts[2]), int(parts[3])
    return 0, 0


def parse_generators(stdout):
    """Extract generator supports from the FIRST '-- Permutations:' block of
    BreakID's verbose (--verb 2) output. Each generator line looks like
    'c ( 2 3 ) ( -2 -3 ) ( 1 4 ) ...'; its support is the set of |literals|."""
    supports = []
    in_block = False
    for line in stdout.splitlines():
        if "-- Permutations:" in line:
            if in_block:          # second block (subgroup reordering) -- stop
                break
            in_block = True
            continue
        if in_block:
            # the block ends at the next section header ('-- Matrices:', 'Detecting...')
            if ("-- " in line) or ("Detecting" in line) or ("subgroup" in line):
                break
            nums = re.findall(r"-?\d+", line)
            supp = set(abs(int(n)) for n in nums)
            if supp:
                supports.append(supp)
    return supports


def _normalize_cnf(path):
    """Rewrite a DIMACS CNF so every clause has DISTINCT literals and no clause is
    a tautology. BreakID's symmetry-breaking clauses can contain duplicate
    literals (e.g. `-a -a b`), which is logically harmless (a v a == a) but
    dagster's CNF parser rejects as 'duplicate literals in clause'. Tautologies
    (x v -x) are dropped entirely. Returns (n_clauses, n_dropped)."""
    max_var = 0
    tokens = []
    with open(path, errors="replace") as f:
        for line in f:
            s = line.strip()
            if not s or s[0] == "c" or s.startswith("p cnf") or s[0] == "%":
                continue
            tokens.extend(int(t) for t in s.split())
    # split the token stream into clauses at each 0 terminator
    clauses, cur = [], []
    for t in tokens:
        if t == 0:
            if cur:
                clauses.append(cur)
                cur = []
        else:
            cur.append(t)
            if abs(t) > max_var:
                max_var = abs(t)
    if cur:                       # tolerate a missing final 0
        clauses.append(cur)
    out, dropped = [], 0
    for cl in clauses:
        seen, ded, taut = set(), [], False
        for l in cl:
            if -l in seen:        # x and -x -> tautology, drop the clause
                taut = True
                break
            if l not in seen:     # duplicate literal -> keep one copy
                seen.add(l)
                ded.append(l)
        if taut:
            dropped += 1
        else:
            out.append(ded)
    with open(path, "w") as f:
        f.write("p cnf %d %d\n" % (max_var, len(out)))
        for cl in out:
            f.write(" ".join(str(l) for l in cl) + " 0\n")
    return len(out), dropped


def _read_clauses(path):
    """Return list-of-clauses (list[list[int]]) from a DIMACS file."""
    toks = []
    with open(path, errors="replace") as f:
        for line in f:
            s = line.strip()
            if not s or s[0] == "c" or s[0] == "%" or s.startswith("p cnf"):
                continue
            toks.extend(int(t) for t in s.split())
    clauses, cur = [], []
    for t in toks:
        if t == 0:
            if cur:
                clauses.append(cur)
                cur = []
        else:
            cur.append(t)
    if cur:
        clauses.append(cur)
    return clauses


class FilterResult:
    """Outcome of DAG-aware breaking-clause filtering."""

    def __init__(self, n_generators, n_breaking, n_kept, clauses_final, vars_final):
        self.num_generators = n_generators
        self.n_breaking = n_breaking          # breaking clauses BreakID produced
        self.n_kept = n_kept                  # node-local ones we kept
        self.n_dropped = n_breaking - n_kept  # cross-node (DAG-hostile) ones we dropped
        self.clauses_final = clauses_final
        self.vars_final = vars_final

    def summary(self):
        return ("{} generators; {} breaking clauses, kept {} node-local / dropped {} "
                "cross-node (DAG-hostile); final {} clauses, {} vars"
                .format(self.num_generators, self.n_breaking, self.n_kept,
                        self.n_dropped, self.clauses_final, self.vars_final))


def filter_local_breaking(raw_cnf, out_cnf, orig_n_clauses, orig_max_var, node_var_sets):
    """DAG-aware symmetry breaking. Given BreakID's full output `raw_cnf` (the
    original clauses as a prefix, then appended breaking clauses) and the variable
    sets of the nodes of a decomposition of the ORIGINAL formula, keep only the
    breaking clauses that fit ENTIRELY within a single node -- these add symmetry
    reduction without introducing cross-node coupling (which would widen the DAG's
    separators). Cross-node breaking clauses are dropped. Dropping a subset of
    sound breaking clauses is itself sound (it only keeps more assignments).

    Auxiliary variables (id > orig_max_var) introduced by a kept clause are
    'owned' by that node; later clauses referencing a dropped aux var are dropped
    too (their chain is broken). Writes the normalised result to out_cnf."""
    clauses = _read_clauses(raw_cnf)
    originals = clauses[:orig_n_clauses]
    breaking = clauses[orig_n_clauses:]
    nodes = range(len(node_var_sets))
    aux_owner = {}        # aux var -> node id, or None if its chain was dropped
    kept = []
    for c in breaking:
        orig_vars = set(abs(l) for l in c if abs(l) <= orig_max_var)
        aux_vars = set(abs(l) for l in c if abs(l) > orig_max_var)
        # nodes whose variable set already contains every original variable of c
        cand = set(n for n in nodes if orig_vars <= node_var_sets[n])
        ok = True
        for a in aux_vars:
            if a in aux_owner:
                if aux_owner[a] is None:      # depends on a dropped chain
                    ok = False
                    break
                cand &= {aux_owner[a]}         # must agree with the aux var's owner
        if ok and cand:
            node = min(cand)
            kept.append(c)
            for a in aux_vars:
                aux_owner.setdefault(a, node)
        else:
            for a in aux_vars:
                aux_owner.setdefault(a, None)  # mark new aux vars as dropped
    with open(out_cnf, "w") as f:
        f.write("p cnf 1 1\n")  # placeholder header; _normalize_cnf rewrites it
        for c in originals + kept:
            f.write(" ".join(str(l) for l in c) + " 0\n")
    nc, _ = _normalize_cnf(out_cnf)
    vf, cf = _cnf_dims(out_cnf)
    return FilterResult(0, len(breaking), len(kept), cf, vf)


def run_symbreak(cnf_in, cnf_out, level="full", time_kilo=None, timeout=900, normalize=True):
    """Run BreakID at the given level, writing the augmented CNF to cnf_out.
    Returns a SymBreakResult, or None when level == 'none'. Raises on failure."""
    if level == "none":
        return None
    if level not in _LEVEL_ARGS:
        raise ValueError("unknown symbreak level %r (use %s)" % (level, "/".join(LEVELS)))
    if not available():
        raise RuntimeError(
            "BreakID binary not found at %s -- build it:\n"
            "  cd utilities/symbreak/breakid && mkdir -p build && cd build "
            "&& cmake -DCMAKE_BUILD_TYPE=Release .. && make -j breakid-bin" % BREAKID)
    args = list(_LEVEL_ARGS[level])
    if time_kilo:
        args += ["-t", str(time_kilo)]
    cmd = [BREAKID, "--verb", "2"] + args + [cnf_in, cnf_out]
    p = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout, env=_breakid_env())
    if p.returncode != 0 or not os.path.exists(cnf_out):
        raise RuntimeError("BreakID failed (rc=%d):\n%s" % (p.returncode, (p.stderr or p.stdout)[-800:]))
    # normalise so dagster's strict parser accepts it (dedupe literals / drop
    # tautologies). The DAG-aware path skips this to keep BreakID's original-clause
    # prefix intact for filtering, then normalises the filtered result itself.
    if normalize:
        _normalize_cnf(cnf_out)
    supports = parse_generators(p.stdout)
    return SymBreakResult(cnf_out, supports, _cnf_dims(cnf_in), _cnf_dims(cnf_out), p.stdout)
