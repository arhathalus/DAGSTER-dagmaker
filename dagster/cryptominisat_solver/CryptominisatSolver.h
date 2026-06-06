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

#ifndef CRYPTOMINISAT_SOLVER_H_
#define CRYPTOMINISAT_SOLVER_H_

#include <deque>
#include <vector>
#include <mpi.h>
#include "../SatSolverInterface.h"
#include "../Cnf.h"

// forward-declare so callers (Worker.cpp) don't need CryptoMiniSat's headers;
// cryptominisat.h is included only in CryptominisatSolver.cc.
namespace CMSat { class SATSolver; }
// forward-declare the SLS guidance channel (mpi-only, defined in ../SlsChannel.h)
class SlsChannel;

// A SatSolverInterface backend wrapping CryptoMiniSat (incremental: new_vars +
// add_clause + solve(assumptions) + get_model + blocking-clause enumeration).
// Mirrors MinisatSolver / CadicalSolver semantics exactly so the DAG-level
// projected solutions are identical across backends. CryptoMiniSat additionally
// supports native XOR clauses (add_xor_clause) -- not used by the generic CNF
// path but available for XOR-heavy problems as a future enhancement.
class CryptominisatSolver : public SatSolverInterface {
public:
  Cnf* cnf;                          // copy of the node's base CNF (for pruning)
  bool* mark2;                       // reason-marking scratch, size cnf->vc+1
  bool solver_unit_contradiction;
  std::vector<int> unit_assignments; // units seen (re-assumed each solve)
  CMSat::SATSolver* solver;
  std::vector<int> model_val;        // last model: index 1..vc -> +1 true / -1 false / 0 undef

  // --- optional SLS (gnovelty+) guidance (used by -m 9; NULL for plain -m 7) ---
  // NOTE: CryptoMiniSat's public API exposes no PER-VARIABLE phase setter (only a
  // global set_default_polarity), so this mode feeds search prefixes to the
  // helpers and drains SLS-found solutions, but cannot inject per-variable
  // suggestions as decision hints the way tinisat/cadical/minisat do. Per-var
  // hint injection would require CMS internals or enumeration-safe solution
  // adoption (a documented follow-up).
  SlsChannel* sls;     // owned; constructed/destroyed in lockstep with the helpers
  int sls_phase;
  int sls_suggestion_size;
  int* sls_prefix;
  int* sls_sol_buf;

  CryptominisatSolver(Cnf* cnf, int inprocess_level = INPROCESS_UNSET); // plain incremental CMS
  // CMS with gnovelty helpers over communicator_sls (prefix-feed + drain)
  CryptominisatSolver(Cnf* cnf, MPI_Comm* communicator_sls, int suggestion_size,
                      int max_vc, int phase, int inprocess_level = INPROCESS_UNSET);
  bool append_cnf(Cnf* cnf);
  int run(Message* m);
  void load_into_message(Message* m, RangeSet &r, Message* reference_message);
  bool is_solver_unit_contradiction();
  bool reset_solver();
  bool solver_add_conflict_clause(std::deque<int> d);
  bool prune_solution(Message* reference_message);
  void ensure_var(int v);   // grow the solver's variable set up to index v (1-based)
  void set_inprocessing(int level);  // tune CryptoMiniSat simplify/bve/distill/probe
  ~CryptominisatSolver();
};

#endif // CRYPTOMINISAT_SOLVER_H_
