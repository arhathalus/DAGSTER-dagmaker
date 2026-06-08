/*************************
Copyright 2026 Dagster contributors

This file is part of Dagster (GNU GPL v2+; see CadicalSolver.h header).
*************************/

#include "CadicalSolver.h"
#include "cadical.hpp"
#include "../exceptions.h"
#include "../SlsChannel.h"
#include "../clause_share/ClauseChannel.h"

#include <algorithm>
#include <cstdlib>
#include <cmath>
#include <vector>
#include <glog/logging.h>

// Receives CaDiCaL's learned (conflict) clauses and forwards them to the clause
// hub via the channel. learning(size) is CaDiCaL's size gate (return true to be
// offered a clause of that size); learn(lit) then streams the literals,
// 0-terminated. We additionally drop any clause touching a variable beyond the
// node's original variable set (factor/elim extension vars are not guaranteed
// entailed by the original formula), keeping the shared pool sound to import.
class ClauseExportLearner : public CaDiCaL::Learner {
public:
  ClauseExportLearner(ClauseChannel* ch, int max_size, int node_vc)
      : channel(ch), max_size(max_size), node_vc(node_vc) {}
  bool learning(int size) override { return size >= 3 && size <= max_size; }
  void learn(int lit) override {
    if (lit != 0) { pending.push_back(lit); return; }
    bool ok = pending.size() >= 3;
    for (size_t i = 0; ok && i < pending.size(); i++)
      if (std::abs(pending[i]) > node_vc) ok = false;
    if (ok) channel->export_clause(pending.data(), (int)pending.size());
    pending.clear();
  }
private:
  ClauseChannel* channel;
  int max_size, node_vc;
  std::vector<int> pending;
};

// CaDiCaL uses DIMACS literals directly: add(lit)/add(0) to add a clause,
// assume(lit) for the next solve, solve() -> 10 SAT / 20 UNSAT / 0 unknown,
// val(var) -> >0 if true, <0 if false.  No 0-based offset (unlike MiniSat).

CadicalSolver::CadicalSolver(Cnf* c, int inprocess_level,
                             MPI_Comm* clause_comm, int clause_max_size,
                             const char* proof_path) {
  this->cnf = new Cnf(c);
  this->mark2 = (bool*)calloc(sizeof(bool), c->vc + 1);
  this->solver_unit_contradiction = false;
  this->sls = NULL;            // plain mode: no SLS guidance
  this->sls_phase = 0;
  this->sls_suggestion_size = 0;
  this->sls_prefix = NULL;
  this->sls_sol_buf = NULL;
  this->clause_channel = NULL; // clause sharing off unless clause_comm given
  this->clause_learner = NULL;
  this->clause_node_vc = c->vc;
  this->has_proof = false;
  this->solver = new CaDiCaL::Solver();
  // DRAT proof tracing must be opened in CONFIGURING (before any var/clause), or
  // CaDiCaL only writes a partial proof -- so do it first thing. The proof is a
  // checkable certificate of an UNSAT solve of this node (drat-trim / LRAT).
  if (proof_path != NULL) {
    this->has_proof = this->solver->trace_proof(proof_path);
    if (!this->has_proof)
      VLOG(0) << "CaDiCaL could not open proof trace '" << proof_path << "'";
  }
  // Inprocessing options must be set in CaDiCaL's CONFIGURING state -- i.e.
  // BEFORE any variable is declared or clause added -- so apply them first.
  set_inprocessing(inprocess_level);
  // CaDiCaL's bounded variable addition ("factor", on by default and worth
  // keeping for speed) requires user variables to be DECLARED before use, so
  // they don't clash with factor's extension variables. Every literal Dagster
  // ever adds/assumes for this node (clauses, additional clauses, interface
  // assumptions, blocking clauses) lies within the node's own variable set, so
  // declaring vc up front -- before the first solve -- covers them all.
  if (c->vc > 0)
    this->solver->declare_more_variables(c->vc);
  for (int i = 0; i < c->cc; i++) {
    for (int j = 0; j < c->cl[i]; j++)
      this->solver->add(c->clauses[i][j]);
    this->solver->add(0);
  }
  // Clause sharing: connect a learned-clause exporter to the hub (the last rank
  // of clause_comm). The solver must persist across cubes for this to pay off --
  // the Worker keeps a clause-sharing CaDiCaL incremental (see Worker.cpp).
  if (clause_comm != NULL) {
    int comm_size;
    MPI_Comm_size(*clause_comm, &comm_size);
    this->clause_channel = new ClauseChannel(clause_comm, comm_size - 1, clause_max_size);
    this->clause_learner = new ClauseExportLearner(clause_channel, clause_max_size, c->vc);
    this->solver->connect_learner(this->clause_learner);
  }
}

// -m 6: same base load as -m 5, then stand up the SLS guidance channel. The
// SlsChannel constructor performs a COLLECTIVE MPI_Win_allocate, so this must be
// reached in lockstep with the gnovelty helpers' own window allocation (the
// Worker guarantees this by sending each helper the node "filename" immediately
// before constructing the solver, mirroring the SatSolver / -m 1 path).
CadicalSolver::CadicalSolver(Cnf* c, MPI_Comm* communicator_sls,
                             int suggestion_size, int max_vc, int phase,
                             int inprocess_level)
    : CadicalSolver(c, inprocess_level) {
  this->sls_phase = phase;
  this->sls_suggestion_size = suggestion_size;
  this->sls_prefix = (int*)calloc(c->vc + 1, sizeof(int));
  this->sls_sol_buf = (int*)calloc(max_vc + 2, sizeof(int));
  this->sls = new SlsChannel(communicator_sls, suggestion_size, max_vc + 2);
}

// Tune CaDiCaL's own inprocessing (the backend-native equivalent of the external
// strengthener). set() silently ignores unknown options, so this is safe across
// CaDiCaL versions.
void CadicalSolver::set_inprocessing(int level) {
  switch (level) {
    case INPROCESS_OFF:
      // master toggle off, plus each technique explicitly disabled
      solver->set("inprocessing", 0);
      solver->set("vivify", 0);   solver->set("subsume", 0);
      solver->set("probe", 0);    solver->set("elim", 0);
      solver->set("transred", 0); solver->set("decompose", 0);
      solver->set("cover", 0);    solver->set("condition", 0);
      solver->set("walk", 0);
      break;
    case INPROCESS_LIGHT:
      // keep cheap clause strengthening, drop the expensive techniques
      solver->set("inprocessing", 1);
      solver->set("vivify", 1);   solver->set("subsume", 1);
      solver->set("transred", 1); solver->set("decompose", 1);
      solver->set("probe", 0);    solver->set("elim", 0);
      solver->set("cover", 0);    solver->set("condition", 0);
      break;
    case INPROCESS_HEAVY:
      // enable everything, including the off-by-default eliminations
      solver->set("inprocessing", 1);
      solver->set("vivify", 1);   solver->set("subsume", 1);
      solver->set("probe", 1);    solver->set("elim", 1);
      solver->set("transred", 1); solver->set("decompose", 1);
      solver->set("cover", 1);    solver->set("condition", 1);
      solver->set("walk", 1);
      break;
    default: /* INPROCESS_DEFAULT / UNSET: leave CaDiCaL defaults */ break;
  }
}

// declare user variables up to index v if not already present (factor-safe)
void CadicalSolver::ensure_var(int v) {
  int cur = this->solver->vars();
  if (v > cur)
    this->solver->declare_more_variables(v - cur);
}

CadicalSolver::~CadicalSolver() {
  // delete the channel FIRST: its destructor signals completion (length-0
  // prefix) to the helpers and frees the collective window before teardown.
  if (this->sls != NULL)
    delete this->sls;
  // Disconnect the learner before tearing the solver down, then free the
  // clause-sharing endpoints. (The hub teardown is signalled by the Worker, not
  // here, so an idle worker that never built a solver still releases the hub.)
  if (this->clause_learner != NULL) {
    this->solver->disconnect_learner();
    delete this->clause_learner;
  }
  if (this->clause_channel != NULL)
    delete this->clause_channel;
  // flush + close the DRAT proof so the trace file is complete before teardown
  if (this->has_proof)
    this->solver->close_proof_trace(false);
  free(this->sls_prefix);
  free(this->sls_sol_buf);
  free(this->mark2);
  delete this->cnf;
  delete this->solver;
}

bool CadicalSolver::append_cnf(Cnf* c) {
  for (int i = 0; i < c->cc; i++) {
    for (int j = 0; j < c->cl[i]; j++) {
      ensure_var(abs(c->clauses[i][j]));   // additional clauses may use new vars
      this->solver->add(c->clauses[i][j]);
    }
    this->solver->add(0);
    if (c->cl[i] == 1) {               // track unit clauses (re-assumed each solve)
      int u = c->clauses[i][0];
      if (std::find(unit_assignments.begin(), unit_assignments.end(), u) == unit_assignments.end())
        unit_assignments.push_back(u);
    }
  }
  return true;
}

// Mirror of MinisatSolver::prune_solution: mark a satisfying "reason" subset of
// variables. model values come from solver->val() instead of MiniSat's model[].
bool CadicalSolver::prune_solution(Message* reference_message) {
  for (int i = 1; i <= cnf->vc; i++)
    mark2[i] = false;
  if (reference_message != NULL)
    for (auto it = reference_message->assignments.begin(); it != reference_message->assignments.end(); it++)
      if (abs(*it) <= cnf->vc)
        mark2[abs(*it)] = true;
  for (int i = 0; i < cnf->cc; i++) {
    bool satisfied = false;
    int min_satisfying_var = -1;
    int min_satisfying_lit = -1;
    for (int j = 0; j < cnf->cl[i]; j++) {
      int lit = cnf->clauses[i][j];
      int var = abs(lit);
      int v = solver->val(var);  // >0 true, <0 false
      if (((lit > 0) && (v > 0)) || ((lit < 0) && (v < 0))) {  // literal is set
        satisfied = true;
        if (mark2[var] == true) {
          min_satisfying_var = var;
          min_satisfying_lit = lit;
          break;
        }
        if ((min_satisfying_var == -1) || (
              ((lit > 0) && (min_satisfying_lit < 0)) ||  // priority towards positive literals
              (var < min_satisfying_var)))
          min_satisfying_var = var;
      }
    }
    if (!satisfied)  // an unsatisfied clause => not a real solution
      return false;
    mark2[min_satisfying_var] = true;
  }
  return true;
}

int CadicalSolver::run(Message* m) {
  if (solver_unit_contradiction == true)
    return false;

  // Clause sharing: pull in clauses other workers learned and add them to our
  // database before this solve. Sound because each is entailed by the (shared)
  // node formula; permanent because this CaDiCaL is incremental across cubes.
  if (clause_channel != NULL) {
    int imported = 0;
    clause_channel->import([&](const int* lits, int n) {
      for (int i = 0; i < n; i++) solver->add(lits[i]);
      solver->add(0);
      imported++;
    });
    if (imported > 0) VLOG(3) << "imported " << imported << " shared clauses";
  }

  for (size_t j = 0; j < m->assignments.size(); j++)
    solver->assume(m->assignments[j]);          // interface assignment as assumptions
  for (size_t i = 0; i < unit_assignments.size(); i++)
    solver->assume(unit_assignments[i]);

  if (sls != NULL && sls->active()) {
    // 1. hand the current partial assignment to a helper as a search prefix
    int len = 0;
    for (size_t j = 0; j < m->assignments.size() && len < cnf->vc; j++)
      sls_prefix[len++] = m->assignments[j];
    for (size_t i = 0; i < unit_assignments.size() && len < cnf->vc; i++)
      sls_prefix[len++] = unit_assignments[i];
    sls->send_prefix(sls_prefix, len, 0);

    // 2. apply whatever the helpers have suggested so far as preferred phases.
    //    (decision level 1 -> any helper working a shallower prefix qualifies)
    int applied = 0, s;
    while (applied < sls_suggestion_size && (s = sls->next_suggestion(1)) != 0) {
      if (abs(s) <= cnf->vc) {
        solver->phase(s);   // soft polarity hint; CaDiCaL is free to override
        applied++;
      }
    }
    if (applied > 0)
      VLOG(3) << "SLS seeded " << applied << " phase hints into CaDiCaL";

    // 3. drain any full solution a helper shipped (keeps the channel clear;
    //    adoption of SLS-found models is a deferred follow-up).
    sls->poll_solution(sls_sol_buf, cnf->vc + 2, sls_phase);
  }

  int res = solver->solve();                      // 10 = SATISFIABLE

  // ship clauses learned during this solve (buffered by the export learner) on
  // to the hub for the other workers.
  if (clause_channel != NULL)
    clause_channel->flush();

  return (res == 10) ? 1 : 0;
}

void CadicalSolver::load_into_message(Message* m, RangeSet &r, Message* reference_message) {
  if (!prune_solution(reference_message))
    throw ConsistencyException("Cadical returned false solution\n");
  m->assignments.clear();
  for (auto var = r.buffer.begin(); var != r.buffer.end(); var++) {
    for (int variable = (*var).first; variable <= (*var).second; variable++) {
      if ((variable > cnf->vc) || (!mark2[variable]))
        continue;
      int v = solver->val(variable);
      if (v > 0)
        m->assignments.push_back(variable);
      else if (v < 0)
        m->assignments.push_back(-variable);
    }
  }
  // fold in reference_message constraints (mirrors MinisatSolver exactly)
  for (auto it = reference_message->assignments.begin(); it != reference_message->assignments.end(); it++) {
    for (auto it2 = m->assignments.begin(); it2 != m->assignments.end(); it2++) {
      if (*it == *it2) {
        break;
      } else if (*it == -*it2) {
        throw ConsistencyException("Cadical returned solution contradicting reference message\n");
      }
    }
    m->assignments.push_back(*it);
  }
}

bool CadicalSolver::is_solver_unit_contradiction() {
  return this->solver_unit_contradiction;
}

// incremental solver: no backtracking reset needed between enumerated solutions
bool CadicalSolver::reset_solver() {
  return true;
}

bool CadicalSolver::solver_add_conflict_clause(std::deque<int> d) {
  for (auto it = d.begin(); it != d.end(); it++)
    solver->add(*it);
  solver->add(0);
  if (d.size() == 1) {
    int u = d.front();
    if (std::find(unit_assignments.begin(), unit_assignments.end(), u) == unit_assignments.end())
      unit_assignments.push_back(u);
  }
  return true;
}
