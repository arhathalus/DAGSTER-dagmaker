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

#ifndef CADICAL_SOLVER_H_
#define CADICAL_SOLVER_H_

#include <deque>
#include <vector>
#include <mpi.h>
#include "../SatSolverInterface.h"
#include "../Cnf.h"

// forward-declare so callers (Worker.cpp) don't need CaDiCaL's headers;
// cadical.hpp is included only in CadicalSolver.cc.
namespace CaDiCaL { class Solver; class Learner; }
// forward-declare the SLS guidance channel (mpi-only, defined in ../SlsChannel.h)
class SlsChannel;
// forward-declare the clause-sharing endpoint (defined in ../clause_share/)
class ClauseChannel;

// A SatSolverInterface backend wrapping CaDiCaL (incremental: assume + solve +
// blocking-clause enumeration). Mirrors MinisatSolver semantics exactly so that
// the DAG-level results (projected solutions) are identical across backends.
class CadicalSolver : public SatSolverInterface {
public:
  Cnf* cnf;                          // copy of the node's base CNF (for pruning)
  bool* mark2;                       // reason-marking scratch, size cnf->vc+1
  bool solver_unit_contradiction;
  std::vector<int> unit_assignments; // units seen (re-assumed each solve)
  CaDiCaL::Solver* solver;

  // --- optional SLS (gnovelty+) guidance (used by -m 6; NULL for plain -m 5) ---
  SlsChannel* sls;     // owned; constructed/destroyed in lockstep with the helpers
  int sls_phase;       // phase tag identifying this message to the helpers
  int sls_suggestion_size;
  int* sls_prefix;     // scratch buffer (vc+1 ints) for the assignment prefix
  int* sls_sol_buf;    // scratch buffer for an SLS-supplied solution

  // --- optional clause sharing (cube-and-conquer; NULL when disabled) ---
  // The learned-clause exporter (a CaDiCaL::Learner) pushes conflict clauses to
  // the channel; run() imports clauses other workers shared. Sound because CDCL
  // learned clauses are formula-entailed independent of the cube assumptions.
  ClauseChannel* clause_channel;     // owned; transport to the clause hub
  CaDiCaL::Learner* clause_learner;  // owned; connected to `solver`
  int clause_node_vc;                // export only clauses over vars <= this

  bool has_proof;                    // a DRAT proof trace was opened (close on dtor)

  // plain incremental CaDiCaL (no SLS). inprocess_level tunes CaDiCaL's own
  // inprocessing and MUST be applied before clauses are added (CaDiCaL only
  // accepts set() in its CONFIGURING state), so it is passed to the ctor.
  // clause_comm (when non-NULL) enables clause sharing to the hub at its last
  // rank; clause_max_size bounds the length of exported learned clauses.
  // proof_path (when non-NULL) writes a DRAT proof of this solve -- it too must
  // be opened in CONFIGURING, so it is a ctor param. Intended for an UNSAT solve
  // of a single node (no enumeration / sharing); see utilities/cube/PROOF_SCOPE.md.
  CadicalSolver(Cnf* cnf, int inprocess_level = INPROCESS_UNSET,
                MPI_Comm* clause_comm = NULL, int clause_max_size = 8,
                const char* proof_path = NULL);
  // CaDiCaL guided by gnovelty helpers over communicator_sls. max_vc bounds the
  // SLS solution buffer; phase tags this message (matches the helpers).
  CadicalSolver(Cnf* cnf, MPI_Comm* communicator_sls, int suggestion_size,
                int max_vc, int phase, int inprocess_level = INPROCESS_UNSET);
  bool append_cnf(Cnf* cnf);
  int run(Message* m);
  void load_into_message(Message* m, RangeSet &r, Message* reference_message);
  bool is_solver_unit_contradiction();
  bool reset_solver();
  bool solver_add_conflict_clause(std::deque<int> d);
  bool prune_solution(Message* reference_message);
  void ensure_var(int v);   // factor-safe variable declaration up to index v
  void set_inprocessing(int level);  // tune CaDiCaL vivify/subsume/probe/elim/...
  ~CadicalSolver();
};

#endif // CADICAL_SOLVER_H_
