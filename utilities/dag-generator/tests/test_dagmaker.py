"""Offline test suite for dagmaker (stdlib unittest; no MPI/glog build needed).

Run from the dag-generator directory with the project venv:

    ../../.venv/bin/python -m unittest discover -s tests

Covers: interval round-trips, CNF clause indexing vs Dagster's loader, validity
(coverage / coherence / reporting), determinism, the cost scorer's monotonicity,
the time-indexed structure win, and DAG round-trip.
"""

import os
import random
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from dagmaker import intervals, scorer, validate, assemble, structure, pipeline, preprocess
from dagmaker.cnf import CnfIndex
from dagmaker.dagmodel import DagModel
from dagmaker.decompose import (single, elimination, cutset, biconnected,
                                community, gates, ordering)

HERE = os.path.dirname(__file__)
REPO = os.path.normpath(os.path.join(HERE, "..", "..", ".."))
SU_CNF = os.path.join(REPO, "reports", "dagster_tutorials_youtube", "su.cnf")


def write_cnf(clauses, max_var, comments=None):
    """comments: dict {clause_index_before_which: 'c label'}"""
    path = tempfile.mktemp(suffix=".cnf")
    comments = comments or {}
    with open(path, "w") as f:
        f.write("p cnf %d %d\n" % (max_var, len(clauses)))
        for i, cl in enumerate(clauses):
            if i in comments:
                f.write(comments[i] + "\n")
            f.write(" ".join(map(str, cl)) + " 0\n")
    return path


def make_bmc(P=10, T=20, seed=1, labels=False):
    random.seed(seed)
    def var(s, i):
        return s * P + i
    clauses = []
    comments = {}
    for t in range(T):
        if labels:
            comments[len(clauses)] = "c step {}".format(t)
        for _ in range(15):
            a, b, c = random.sample(range(1, P + 1), 3)
            clauses.append([random.choice([1, -1]) * var(t, a),
                            random.choice([1, -1]) * var(t, b),
                            random.choice([1, -1]) * var(t, c)])
        if t < T - 1:
            for i in range(1, P + 1):
                clauses.append([-var(t, i), var(t + 1, i)])
    return write_cnf(clauses, T * P, comments), P, T


class TestIntervals(unittest.TestCase):
    def test_compact_expand(self):
        self.assertEqual(intervals.compact([1, 2, 3, 5, 6, 7, 9]), "1-3,5-7,9")
        self.assertEqual(intervals.compact([4]), "4")          # never 4-4
        self.assertEqual(intervals.compact([]), "")
        self.assertEqual(intervals.compact([3, 3, 1, 2]), "1-3")  # dedup+sort
        self.assertEqual(intervals.expand("1-3,5-7,9"), [1, 2, 3, 5, 6, 7, 9])

    def test_no_singleton_range(self):
        # Dag.cpp:136 rejects a-a for clauses
        for s in [intervals.compact(x) for x in ([0], [5], [100])]:
            self.assertNotIn("-", s)


class TestCnf(unittest.TestCase):
    def test_indexing_and_comments(self):
        path = write_cnf([[1, -2], [2, 3], [-1, -3]], 3,
                         comments={0: "c first group", 2: "c second group"})
        cnf = CnfIndex.from_file(path)
        self.assertEqual(cnf.n_clauses, 3)
        self.assertEqual(cnf.max_var, 3)
        self.assertEqual(set(cnf.clause_vars(1)), {2, 3})
        self.assertEqual(len(cnf.comment_markers), 2)
        # marker labels the clause index in effect when seen
        self.assertEqual(cnf.comment_markers[0], (0, "first group"))
        self.assertEqual(cnf.comment_markers[1], (2, "second group"))
        os.remove(path)

    def test_comment_digits_not_tokenised(self):
        # a comment containing digits (like sudoku MAPPING) must not become a clause
        path = write_cnf([[1, 2]], 2, comments={0: "c MAPPING r=1 c=1 v=1 : 7"})
        cnf = CnfIndex.from_file(path)
        self.assertEqual(cnf.n_clauses, 1)
        self.assertEqual(cnf.max_var, 2)
        os.remove(path)

    def test_used_vars(self):
        path = write_cnf([[1, 2]], 5, None)  # vars 3,4,5 unused
        cnf = CnfIndex.from_file(path)
        self.assertEqual(cnf.used_vars(), {1, 2})
        os.remove(path)


class TestValidate(unittest.TestCase):
    def setUp(self):
        self.path, _, _ = make_bmc()
        self.cnf = CnfIndex.from_file(self.path)

    def tearDown(self):
        os.remove(self.path)

    def test_single_valid(self):
        self.assertTrue(validate.validate(single.build(self.cnf), self.cnf).ok)

    def test_elimination_valid(self):
        m = elimination.build(self.cnf, target_nodes=5, max_sep=50)
        self.assertTrue(validate.validate(m, self.cnf).ok)

    def test_coverage_violation_detected(self):
        m = single.build(self.cnf)
        m.nodes[0].discard(0)  # drop a clause -> not covered
        self.assertFalse(validate.validate(m, self.cnf).ok)

    def test_coherence_violation_detected(self):
        # 2-node chain but the edge carries no variables: a shared variable is
        # then used in disconnected nodes -> running-intersection violation
        m = assemble.chain(self.cnf, [set(range(0, 200)), set(range(200, self.cnf.n_clauses))],
                           reporting=self.cnf.used_vars(), prune=False)
        # forcibly empty the edge
        for k in list(m.edges):
            m.edges[k] = set()
        rep = validate.validate(m, self.cnf)
        self.assertFalse(rep.ok)
        self.assertTrue(any("running-intersection" in p or "reach a terminal" in p
                            for p in rep.problems))


class TestScorer(unittest.TestCase):
    def setUp(self):
        self.path, _, _ = make_bmc()
        self.cnf = CnfIndex.from_file(self.path)

    def tearDown(self):
        os.remove(self.path)

    def test_monotonic_separator(self):
        narrow = elimination.build(self.cnf, target_nodes=2, max_sep=15,
                                   reporting=set(range(191, 201)))
        wide = elimination.build(self.cnf, target_nodes=8, max_sep=200,
                                 reporting=self.cnf.used_vars())
        sn, sw = scorer.score(narrow, self.cnf), scorer.score(wide, self.cnf)
        # full-reporting wide chain should not have a smaller max separator
        self.assertGreaterEqual(sw.max_sep_width, sn.max_sep_width)


class TestDeterminism(unittest.TestCase):
    def test_elimination_deterministic(self):
        path, _, _ = make_bmc()
        cnf = CnfIndex.from_file(path)
        a = elimination.build(cnf, target_nodes=5, max_sep=50).to_string()
        b = elimination.build(cnf, target_nodes=5, max_sep=50).to_string()
        self.assertEqual(a, b)
        os.remove(path)


class TestStructure(unittest.TestCase):
    def test_timeindexed_beats_generic(self):
        path, P, T = make_bmc()
        cnf = CnfIndex.from_file(path)
        report = set(range((T - 1) * P + 1, T * P + 1))
        gen = scorer.score(elimination.build(cnf, target_nodes=8, max_sep=50,
                                             reporting=report), cnf)
        ti = structure.try_build(cnf, family="timeindexed", target_nodes=8,
                                 max_sep=50, reporting=report)
        self.assertIsNotNone(ti)
        sti = scorer.score(ti[1], cnf)
        self.assertTrue(validate.validate(ti[1], cnf).ok)
        # state-variable separator (P) should beat the generic cut
        self.assertLessEqual(sti.max_sep_width, gen.max_sep_width)
        self.assertLessEqual(sti.max_sep_width, P)
        os.remove(path)

    def test_metadata_tier_a(self):
        path, P, T = make_bmc(labels=True)
        cnf = CnfIndex.from_file(path)
        self.assertEqual(len(cnf.comment_markers), T)
        res = structure.try_build(cnf, target_nodes=8, max_sep=50,
                                  reporting=set(range((T - 1) * P + 1, T * P + 1)))
        self.assertIsNotNone(res)
        self.assertEqual(res[0], "metadata")
        self.assertTrue(validate.validate(res[1], cnf).ok)
        os.remove(path)


class TestRoundTrip(unittest.TestCase):
    def test_dag_roundtrip(self):
        path, _, _ = make_bmc()
        cnf = CnfIndex.from_file(path)
        m = elimination.build(cnf, target_nodes=5, max_sep=50)
        text = m.to_string()
        rt = DagModel.from_string(text, cnf.n_clauses, cnf.max_var)
        self.assertEqual(rt.to_string(), text)
        os.remove(path)


class TestOverlapCutset(unittest.TestCase):
    def setUp(self):
        self.path, _, _ = make_bmc()
        self.cnf = CnfIndex.from_file(self.path)

    def tearDown(self):
        os.remove(self.path)

    def test_cutset_builds_and_is_sound_with_overlap(self):
        m = cutset.build(self.cnf, hubs=20, max_sep=40)
        self.assertIsNotNone(m)
        self.assertEqual(m.num_nodes, 2)
        # separator is exactly the hub count, regardless of (full) reporting
        sc = scorer.score(m, self.cnf)
        self.assertLessEqual(sc.max_sep_width, 20)
        # terminal holds all clauses -> sound under overlap (non-strict)
        self.assertTrue(validate.validate(m, self.cnf, strict=False).ok)

    def test_cutset_rejected_under_strict_partition(self):
        m = cutset.build(self.cnf, hubs=20, max_sep=40)
        rep = validate.validate(m, self.cnf, strict=True)
        self.assertFalse(rep.ok)  # clause overlap is an error in strict mode

    def test_partition_dag_still_valid_in_both_modes(self):
        m = elimination.build(self.cnf, target_nodes=5, max_sep=50)
        self.assertTrue(validate.validate(m, self.cnf, strict=False).ok)
        self.assertTrue(validate.validate(m, self.cnf, strict=True).ok)

    def test_pipeline_prefers_cutset_on_full_reporting(self):
        # full reporting + tight budget: partition backends degrade to 1 node,
        # cutset gives a 2-node decomposition within budget
        res = pipeline.generate(self.cnf, target_nodes=8, max_sep=25)
        self.assertTrue(res.best.report.ok)
        self.assertEqual(res.best.name, "cutset")
        self.assertGreater(res.best.score.num_nodes, 1)


class TestNewBackends(unittest.TestCase):
    def test_ordering_valid(self):
        path, _, _ = make_bmc()
        cnf = CnfIndex.from_file(path)
        for method in ("bfs", "rcm", "spectral"):
            m = ordering.build(cnf, method=method, target_nodes=5, max_sep=60)
            self.assertTrue(validate.validate(m, cnf).ok, method)
        os.remove(path)

    def test_biconnected_finds_articulation(self):
        # two clusters joined only at variable 5 -> size-1 separator under search
        clauses = [[1, 2, 3], [2, 3, 4], [3, 4, 5], [1, 4], [2, 5],
                   [5, 6, 7], [6, 7, 8], [7, 8, 9], [6, 9], [5, 8]]
        cnf = CnfIndex.from_clauses(clauses, 9)
        m = biconnected.build(cnf, reporting=set())
        self.assertIsNotNone(m)
        self.assertTrue(validate.validate(m, cnf).ok)
        self.assertEqual(scorer.score(m, cnf).max_sep_width, 1)
        # one big block -> no articulation -> None
        clique = [[a, b, c] for a in range(1, 6) for b in range(a + 1, 6)
                  for c in range(b + 1, 6)]
        self.assertIsNone(biconnected.build(CnfIndex.from_clauses(clique, 5)))

    def test_community_beats_cutset_on_clusters(self):
        random.seed(2)
        groups = [list(range(1, 9)), list(range(9, 17)), list(range(17, 25))]
        clauses = []
        for g in groups:
            for _ in range(40):
                a, b, c = random.sample(g, 3)
                clauses.append([a, b, c])
        clauses += [[8, 9], [16, 17]]   # the only bridges
        cnf = CnfIndex.from_clauses(clauses, 24)
        m = community.build(cnf, reporting=set())
        self.assertIsNotNone(m)
        self.assertTrue(validate.validate(m, cnf).ok)
        self.assertGreaterEqual(m.num_nodes, 2)
        self.assertLessEqual(scorer.score(m, cnf).max_sep_width, 4)

    def test_gates_detects_circuit_chain(self):
        n = 10
        def a(i): return i
        def gg(i): return (n + 1) + i
        clauses = [[gg(1), -a(1), -a(2)], [-gg(1), a(1)], [-gg(1), a(2)]]
        for i in range(2, n + 1):
            clauses += [[gg(i), -gg(i - 1), -a(i + 1)], [-gg(i), gg(i - 1)], [-gg(i), a(i + 1)]]
        clauses += [[gg(n)]]
        cnf = CnfIndex.from_clauses(clauses, 2 * n + 1)
        self.assertEqual(len(gates.detect_definitions(clauses)), n)
        m = gates.build(cnf, clauses, target_nodes=6, max_sep=30, reporting=set())
        self.assertIsNotNone(m)
        self.assertTrue(validate.validate(m, cnf).ok)
        self.assertLessEqual(scorer.score(m, cnf).max_sep_width, 2)


class TestPreprocess(unittest.TestCase):
    def test_unit_and_pure(self):
        # clause [1] forces 1=true; then [-1,2] forces 2=true; 3 is pure (only +3)
        clauses = [[1], [-1, 2], [3, 4], [3, 5]]
        s = preprocess.simplify(clauses, 5)
        self.assertTrue(s.sat)
        # 1 and 2 forced true; 3 pure-positive -> assigned; clauses all satisfied
        self.assertIn(1, s.trail)
        self.assertIn(2, s.trail)
        self.assertEqual(s.n_clauses_after, 0)  # everything satisfied

    def test_unsat_detected(self):
        s = preprocess.simplify([[1], [-1]], 1)  # 1 and not-1
        self.assertFalse(s.sat)

    def test_literal_removed_not_clause(self):
        # [1] forces 1=true -> [-1,2,3] loses the falsified -1, keeps [2,3].
        # 2,3 appear in both polarities (via [-2,-3]) so PLE does not remove them.
        s = preprocess.simplify([[1], [-1, 2, 3], [-2, -3]], 3)
        self.assertTrue(s.sat)
        self.assertEqual(s.n_clauses_after, 2)
        kept = next(c for c in s.clauses if set(map(abs, c)) == {2, 3} and len(c) == 2
                    and all(l > 0 for l in c))
        self.assertEqual(sorted(kept), [2, 3])

    def test_end_to_end_decompose(self):
        path, _, _ = make_bmc()
        clauses, mv = preprocess.read_dimacs(path)
        s = preprocess.simplify(clauses, mv)
        self.assertTrue(s.sat)
        cnf = CnfIndex.from_clauses(s.clauses, s.max_var)
        res = pipeline.generate(cnf, target_nodes=5, max_sep=50)
        self.assertTrue(res.best.report.ok)
        os.remove(path)


CORPUS = os.path.join(REPO, "Benchmarks", "corpus")


@unittest.skipUnless(os.path.exists(os.path.join(CORPUS, "run_corpus.py")), "corpus absent")
class TestCorpus(unittest.TestCase):
    def test_all_classes_match_their_strategy(self):
        sys.path.insert(0, CORPUS)
        import run_corpus
        self.assertEqual(run_corpus.run(verbose=False), 0,
                         "a corpus class was not matched by its intended strategy")


@unittest.skipUnless(os.path.exists(SU_CNF), "su.cnf fixture not present")
class TestGoldenSudoku(unittest.TestCase):
    def test_pipeline_valid(self):
        cnf = CnfIndex.from_file(SU_CNF)
        res = pipeline.generate(cnf, target_nodes=4, max_sep=30)
        self.assertTrue(res.best.report.ok)
        # whatever wins must cover every clause (overlap allowed by default)
        covered = set().union(*res.best.model.nodes)
        self.assertEqual(covered, set(range(cnf.n_clauses)))


if __name__ == "__main__":
    unittest.main(verbosity=2)
