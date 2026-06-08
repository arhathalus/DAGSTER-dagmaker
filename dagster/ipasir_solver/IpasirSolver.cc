/*************************
Copyright 2026 Dagster contributors

This file is part of Dagster.

Dagster is free software; you can redistribute it
and/or modify it under the terms of the GNU General
Public License as published by the Free Software
Foundation; either version 2 of the License, or
(at your option) any later version.

Dagster is distributed in the hope that it will be
useful, but WITHOUT ANY WARRANTY; without even the
implied warranty of MERCHANTABILITY or FITNESS FOR
A PARTICULAR PURPOSE. See the GNU General Public
License for more details.
*************************/

#include "IpasirSolver.h"
#include "../exceptions.h"

#include <algorithm>
#include <cstdlib>
#include <dlfcn.h>
#include <glog/logging.h>

// resolve one IPASIR symbol or die (the whole point is a clear message if the
// shared library isn't actually IPASIR-compliant).
template <typename T>
static T dlsym_or_die(void* lib, const char* name) {
  dlerror();
  void* sym = dlsym(lib, name);
  const char* err = dlerror();
  if (err != NULL || sym == NULL)
    LOG(FATAL) << "ipasir backend: shared library is missing symbol '" << name << "'";
  return reinterpret_cast<T>(sym);
}

IpasirSolver::IpasirSolver(Cnf* c, const char* load_path) {
  this->cnf = new Cnf(c);
  this->mark2 = (bool*)calloc(sizeof(bool), c->vc + 1);
  this->solver_unit_contradiction = false;

  // RTLD_LOCAL keeps the solver's symbols private to this handle, so distinct
  // IpasirSolver instances could even load different IPASIR libraries at once.
  this->lib = dlopen(load_path, RTLD_NOW | RTLD_LOCAL);
  if (this->lib == NULL)
    LOG(FATAL) << "ipasir backend: cannot dlopen '" << (load_path ? load_path : "(null)")
               << "': " << dlerror();
  this->f_signature = dlsym_or_die<const char* (*)()>(lib, "ipasir_signature");
  this->f_init      = dlsym_or_die<void* (*)()>(lib, "ipasir_init");
  this->f_release   = dlsym_or_die<void (*)(void*)>(lib, "ipasir_release");
  this->f_add       = dlsym_or_die<void (*)(void*, int)>(lib, "ipasir_add");
  this->f_assume    = dlsym_or_die<void (*)(void*, int)>(lib, "ipasir_assume");
  this->f_solve     = dlsym_or_die<int (*)(void*)>(lib, "ipasir_solve");
  this->f_val       = dlsym_or_die<int (*)(void*, int)>(lib, "ipasir_val");

  this->solver = f_init();
  VLOG(2) << "ipasir backend: loaded '" << f_signature() << "'";
  for (int i = 0; i < c->cc; i++) {
    for (int j = 0; j < c->cl[i]; j++)
      f_add(solver, c->clauses[i][j]);
    f_add(solver, 0);
  }
}

IpasirSolver::~IpasirSolver() {
  if (solver != NULL) f_release(solver);
  if (lib != NULL) dlclose(lib);
  free(this->mark2);
  delete this->cnf;
}

bool IpasirSolver::append_cnf(Cnf* c) {
  for (int i = 0; i < c->cc; i++) {
    for (int j = 0; j < c->cl[i]; j++)
      f_add(solver, c->clauses[i][j]);
    f_add(solver, 0);
    if (c->cl[i] == 1) {                       // track unit clauses (re-assumed each solve)
      int u = c->clauses[i][0];
      if (std::find(unit_assignments.begin(), unit_assignments.end(), u) == unit_assignments.end())
        unit_assignments.push_back(u);
    }
  }
  return true;
}

// Mirror of CadicalSolver::prune_solution -- model values come from ipasir_val
// (returns the literal's variable signed: >0 means the var is true).
bool IpasirSolver::prune_solution(Message* reference_message) {
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
      int v = f_val(solver, var);   // >0 true, <0 false
      if (((lit > 0) && (v > 0)) || ((lit < 0) && (v < 0))) {
        satisfied = true;
        if (mark2[var] == true) {
          min_satisfying_var = var;
          min_satisfying_lit = lit;
          break;
        }
        if ((min_satisfying_var == -1) || (
              ((lit > 0) && (min_satisfying_lit < 0)) ||
              (var < min_satisfying_var)))
          min_satisfying_var = var;
      }
    }
    if (!satisfied)
      return false;
    mark2[min_satisfying_var] = true;
  }
  return true;
}

int IpasirSolver::run(Message* m) {
  if (solver_unit_contradiction == true)
    return false;
  for (size_t j = 0; j < m->assignments.size(); j++)
    f_assume(solver, m->assignments[j]);       // interface assignment as assumptions
  for (size_t i = 0; i < unit_assignments.size(); i++)
    f_assume(solver, unit_assignments[i]);
  int res = f_solve(solver);                    // 10 = SATISFIABLE, 20 = UNSAT
  return (res == 10) ? 1 : 0;
}

void IpasirSolver::load_into_message(Message* m, RangeSet &r, Message* reference_message) {
  if (!prune_solution(reference_message))
    throw ConsistencyException("ipasir backend returned false solution\n");
  m->assignments.clear();
  for (auto var = r.buffer.begin(); var != r.buffer.end(); var++) {
    for (int variable = (*var).first; variable <= (*var).second; variable++) {
      if ((variable > cnf->vc) || (!mark2[variable]))
        continue;
      int v = f_val(solver, variable);
      if (v > 0)
        m->assignments.push_back(variable);
      else if (v < 0)
        m->assignments.push_back(-variable);
    }
  }
  for (auto it = reference_message->assignments.begin(); it != reference_message->assignments.end(); it++) {
    for (auto it2 = m->assignments.begin(); it2 != m->assignments.end(); it2++) {
      if (*it == *it2) {
        break;
      } else if (*it == -*it2) {
        throw ConsistencyException("ipasir backend returned solution contradicting reference message\n");
      }
    }
    m->assignments.push_back(*it);
  }
}

bool IpasirSolver::is_solver_unit_contradiction() {
  return this->solver_unit_contradiction;
}

// incremental solver: no backtracking reset needed between enumerated solutions
bool IpasirSolver::reset_solver() {
  return true;
}

bool IpasirSolver::solver_add_conflict_clause(std::deque<int> d) {
  for (auto it = d.begin(); it != d.end(); it++)
    f_add(solver, *it);
  f_add(solver, 0);
  if (d.size() == 1) {
    int u = d.front();
    if (std::find(unit_assignments.begin(), unit_assignments.end(), u) == unit_assignments.end())
      unit_assignments.push_back(u);
  }
  return true;
}
