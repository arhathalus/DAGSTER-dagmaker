/*************************
Copyright 2026 Dagster contributors

This file is part of Dagster (GNU GPL v2+; see CryptominisatSolver.h header).
*************************/

#include "CryptominisatSolver.h"
#include "cryptominisat.h"
#include "../exceptions.h"
#include "../SlsChannel.h"
#include <glog/logging.h>

#include <algorithm>
#include <cstdlib>

// CryptoMiniSat uses 0-BASED variables wrapped in CMSat::Lit(var, is_inverted).
// A DIMACS literal L (1-based, signed) maps to Lit(abs(L)-1, L < 0). solve()
// takes a vector<Lit> of assumptions and returns l_True/l_False/l_Undef;
// get_model() then holds one lbool per variable (indexed 0-based).

static inline CMSat::Lit to_cms_lit(int dimacs_lit) {
  return CMSat::Lit(std::abs(dimacs_lit) - 1, dimacs_lit < 0);
}

CryptominisatSolver::CryptominisatSolver(Cnf* c, int inprocess_level) {
  this->cnf = new Cnf(c);
  this->mark2 = (bool*)calloc(sizeof(bool), c->vc + 1);
  this->solver_unit_contradiction = false;
  this->sls = NULL;            // plain mode: no SLS guidance
  this->sls_phase = 0;
  this->sls_suggestion_size = 0;
  this->sls_prefix = NULL;
  this->sls_sol_buf = NULL;
  this->solver = new CMSat::SATSolver();
  set_inprocessing(inprocess_level);   // before adding vars/clauses (matches CaDiCaL ordering)
  if (c->vc > 0)
    this->solver->new_vars(c->vc);
  std::vector<CMSat::Lit> clause;
  for (int i = 0; i < c->cc; i++) {
    clause.clear();
    for (int j = 0; j < c->cl[i]; j++)
      clause.push_back(to_cms_lit(c->clauses[i][j]));
    this->solver->add_clause(clause);
  }
}

// -m 9: same base load as -m 7, then stand up the SLS guidance channel. The
// SlsChannel constructor is COLLECTIVE, so this must be reached in lockstep with
// the gnovelty helpers' window allocation (the Worker guarantees this).
CryptominisatSolver::CryptominisatSolver(Cnf* c, MPI_Comm* communicator_sls,
                                         int suggestion_size, int max_vc, int phase,
                                         int inprocess_level)
    : CryptominisatSolver(c, inprocess_level) {
  this->sls_phase = phase;
  this->sls_suggestion_size = suggestion_size;
  this->sls_prefix = (int*)calloc(c->vc + 1, sizeof(int));
  this->sls_sol_buf = (int*)calloc(max_vc + 2, sizeof(int));
  this->sls = new SlsChannel(communicator_sls, suggestion_size, max_vc + 2);
}

// Tune CryptoMiniSat's own simplification/inprocessing (its native equivalent of
// the external strengthener).
void CryptominisatSolver::set_inprocessing(int level) {
  switch (level) {
    case INPROCESS_OFF:
      solver->set_no_simplify();        // never run simplification/inprocessing
      break;
    case INPROCESS_LIGHT:
      // keep simplification but drop the expensive structural transforms
      solver->set_no_bve();             // no bounded variable elimination
      solver->set_no_bva();             // no bounded variable addition
      solver->set_no_simplify_at_startup();
      break;
    case INPROCESS_HEAVY:
      solver->set_distill(1);
      solver->set_intree_probe(1);
      solver->set_full_bve(1);          // exhaustive variable elimination
      solver->set_bva(1);
      break;
    default: /* INPROCESS_DEFAULT / UNSET: leave CMS defaults */ break;
  }
}

// grow the solver's variable set so that 1-based index v is addressable
void CryptominisatSolver::ensure_var(int v) {
  int cur = (int)this->solver->nVars();
  if (v > cur)
    this->solver->new_vars(v - cur);
}

CryptominisatSolver::~CryptominisatSolver() {
  // delete the channel FIRST: its destructor signals completion to the helpers
  // and frees the collective window before teardown.
  if (this->sls != NULL)
    delete this->sls;
  free(this->sls_prefix);
  free(this->sls_sol_buf);
  free(this->mark2);
  delete this->cnf;
  delete this->solver;
}

bool CryptominisatSolver::append_cnf(Cnf* c) {
  std::vector<CMSat::Lit> clause;
  for (int i = 0; i < c->cc; i++) {
    clause.clear();
    for (int j = 0; j < c->cl[i]; j++) {
      ensure_var(std::abs(c->clauses[i][j]));   // additional clauses may use new vars
      clause.push_back(to_cms_lit(c->clauses[i][j]));
    }
    this->solver->add_clause(clause);
    if (c->cl[i] == 1) {               // track unit clauses (re-assumed each solve)
      int u = c->clauses[i][0];
      if (std::find(unit_assignments.begin(), unit_assignments.end(), u) == unit_assignments.end())
        unit_assignments.push_back(u);
    }
  }
  return true;
}

int CryptominisatSolver::run(Message* m) {
  if (solver_unit_contradiction == true)
    return false;
  std::vector<CMSat::Lit> assumptions;
  // The interface assignment can name a separator variable that does not occur
  // in this node's local clauses (so it was never declared). CryptoMiniSat
  // aborts on assuming an undeclared variable (unlike CaDiCaL), so grow the
  // variable set to cover every assumption first.
  for (size_t j = 0; j < m->assignments.size(); j++) {
    ensure_var(std::abs(m->assignments[j]));
    assumptions.push_back(to_cms_lit(m->assignments[j]));   // interface assignment
  }
  for (size_t i = 0; i < unit_assignments.size(); i++) {
    ensure_var(std::abs(unit_assignments[i]));
    assumptions.push_back(to_cms_lit(unit_assignments[i]));
  }

  if (sls != NULL && sls->active()) {
    // Feed the current partial assignment to a helper as a search prefix, then
    // drain any full solution it shipped (channel hygiene). CryptoMiniSat has no
    // public per-variable phase setter, so suggestions are NOT injected as
    // decision hints here (see header note) -- this keeps the SLS topology and
    // collective window lifecycle correct without per-var biasing.
    int len = 0;
    for (size_t j = 0; j < m->assignments.size() && len < cnf->vc; j++)
      sls_prefix[len++] = m->assignments[j];
    sls->send_prefix(sls_prefix, len, 0);
    sls->poll_solution(sls_sol_buf, cnf->vc + 2, sls_phase);
  }

  CMSat::lbool ret = solver->solve(&assumptions);
  if (ret != CMSat::l_True)
    return 0;

  // cache the model as +1/-1/0 per (1-based) variable so prune/load stay
  // independent of CMSat types
  const std::vector<CMSat::lbool>& mdl = solver->get_model();
  model_val.assign(cnf->vc + 1, 0);
  for (int v = 1; v <= cnf->vc; v++) {
    if ((size_t)(v - 1) < mdl.size()) {
      if (mdl[v - 1] == CMSat::l_True)       model_val[v] = 1;
      else if (mdl[v - 1] == CMSat::l_False) model_val[v] = -1;
    }
  }
  return 1;
}

// Mirror of MinisatSolver/CadicalSolver prune_solution: mark a satisfying
// "reason" subset of variables, reading the cached model_val[].
bool CryptominisatSolver::prune_solution(Message* reference_message) {
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
      int v = (var <= cnf->vc) ? model_val[var] : 0;  // +1 true / -1 false
      if (((lit > 0) && (v > 0)) || ((lit < 0) && (v < 0))) {  // literal is set true
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

void CryptominisatSolver::load_into_message(Message* m, RangeSet &r, Message* reference_message) {
  if (!prune_solution(reference_message))
    throw ConsistencyException("CryptoMiniSat returned false solution\n");
  m->assignments.clear();
  for (auto var = r.buffer.begin(); var != r.buffer.end(); var++) {
    for (int variable = (*var).first; variable <= (*var).second; variable++) {
      if ((variable > cnf->vc) || (!mark2[variable]))
        continue;
      int v = model_val[variable];
      if (v > 0)
        m->assignments.push_back(variable);
      else if (v < 0)
        m->assignments.push_back(-variable);
    }
  }
  // fold in reference_message constraints (mirrors MinisatSolver/CadicalSolver exactly)
  for (auto it = reference_message->assignments.begin(); it != reference_message->assignments.end(); it++) {
    for (auto it2 = m->assignments.begin(); it2 != m->assignments.end(); it2++) {
      if (*it == *it2) {
        break;
      } else if (*it == -*it2) {
        throw ConsistencyException("CryptoMiniSat returned solution contradicting reference message\n");
      }
    }
    m->assignments.push_back(*it);
  }
}

bool CryptominisatSolver::is_solver_unit_contradiction() {
  return this->solver_unit_contradiction;
}

// incremental solver: no backtracking reset needed between enumerated solutions
bool CryptominisatSolver::reset_solver() {
  return true;
}

bool CryptominisatSolver::solver_add_conflict_clause(std::deque<int> d) {
  std::vector<CMSat::Lit> clause;
  for (auto it = d.begin(); it != d.end(); it++) {
    ensure_var(std::abs(*it));
    clause.push_back(to_cms_lit(*it));
  }
  solver->add_clause(clause);
  if (d.size() == 1) {
    int u = d.front();
    if (std::find(unit_assignments.begin(), unit_assignments.end(), u) == unit_assignments.end())
      unit_assignments.push_back(u);
  }
  return true;
}
