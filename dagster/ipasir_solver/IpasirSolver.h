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

#ifndef IPASIR_SOLVER_H_
#define IPASIR_SOLVER_H_

#include <deque>
#include <vector>
#include "../SatSolverInterface.h"
#include "../Cnf.h"

// A SatSolverInterface backend over the standard incremental SAT API, IPASIR.
// Dagster's interface IS essentially IPASIR (run = assume+solve, load_into_message
// = val, conflict/append = add), so this is a thin adapter. The IPASIR solver is
// loaded at RUN TIME from a shared library via dlopen -- so ANY IPASIR-compliant
// solver (Glucose, Maple, Lingeling, ...) drops in by building it as a .so and
// pointing --ipasir-lib at it, with no Dagster recompile and no symbol collisions.
//
// Variable/projection handling mirrors CadicalSolver exactly so DAG-level results
// (projected solutions) match across all backends.
class IpasirSolver : public SatSolverInterface {
public:
  Cnf* cnf;                          // copy of the node's base CNF (for pruning)
  bool* mark2;                       // reason-marking scratch, size cnf->vc+1
  bool solver_unit_contradiction;
  std::vector<int> unit_assignments; // units seen (re-assumed each solve)

  void* lib;     // dlopen handle (owned)
  void* solver;  // the IPASIR solver instance (from ipasir_init)
  // resolved IPASIR entry points (dlsym)
  const char* (*f_signature)();
  void* (*f_init)();
  void  (*f_release)(void*);
  void  (*f_add)(void*, int);
  void  (*f_assume)(void*, int);
  int   (*f_solve)(void*);
  int   (*f_val)(void*, int);

  // load_path = the libipasir<solver>.so to dlopen. Aborts if it cannot be opened
  // or is missing IPASIR symbols.
  IpasirSolver(Cnf* cnf, const char* load_path);
  bool append_cnf(Cnf* cnf);
  int run(Message* m);
  void load_into_message(Message* m, RangeSet &r, Message* reference_message);
  bool is_solver_unit_contradiction();
  bool reset_solver();
  bool solver_add_conflict_clause(std::deque<int> d);
  bool prune_solution(Message* reference_message);
  ~IpasirSolver();
};

#endif // IPASIR_SOLVER_H_
