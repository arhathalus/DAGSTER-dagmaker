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
namespace CaDiCaL { class Solver; class Learner; class ExternalPropagator; class Terminator; }
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

  // --- optional LIVE clause import (phase 2; NULL unless --share-live) ---
  // When set, shared clauses are injected DURING the CDCL search (not just
  // between cubes) via CaDiCaL's external-propagator hook. Requires all node
  // vars to be `observed` (hence frozen -> no variable elimination), so it is
  // an opt-in experiment; see utilities/cube/CLAUSE_SHARING_SCOPE.md.
  CaDiCaL::ExternalPropagator* clause_propagator;  // owned; connected to `solver`
  bool live_share;                                 // import mid-solve (else only at run() start)

  bool has_proof;                    // a DRAT proof trace was opened (close on dtor)

  // --- optional solve yielding (cube-and-conquer termination responsiveness) ---
  // CaDiCaL runs solve() to completion by default, so a worker stuck on a hard
  // cube can't see the master's terminate/reassign until it finishes. A terminator
  // aborts the solve after yield_seconds of wall-clock, so run() returns 2
  // ("paused") and the worker polls the master (which may resume, reassign, or
  // kill it) -- mirroring the native solver's periodic pause. The solver state is
  // retained (incremental), so a resumed solve continues where it left off.
  CaDiCaL::Terminator* terminator;   // owned; connected to `solver` (NULL if disabled)
  double yield_seconds;              // solve budget before yielding; <=0 disables

  // plain incremental CaDiCaL (no SLS). inprocess_level tunes CaDiCaL's own
  // inprocessing and MUST be applied before clauses are added (CaDiCaL only
  // accepts set() in its CONFIGURING state), so it is passed to the ctor.
  // clause_comm (when non-NULL) enables clause sharing to the hub at its last
  // rank; clause_max_size bounds the length of exported learned clauses.
  // proof_path (when non-NULL) writes a DRAT proof of this solve -- it too must
  // be opened in CONFIGURING, so it is a ctor param. Intended for an UNSAT solve
  // of a single node (no enumeration / sharing); see utilities/cube/PROOF_SCOPE.md.
  // live_share (only meaningful with clause_comm) imports shared clauses during
  // the solve via an external propagator, instead of only at the start of run().
  // yield_seconds (>0) makes a single solve() yield ("paused", run() returns 2)
  // after that many wall-clock seconds so the worker can poll the master; 0 = run
  // solves to completion (old behaviour).
  CadicalSolver(Cnf* cnf, int inprocess_level = INPROCESS_UNSET,
                MPI_Comm* clause_comm = NULL, int clause_max_size = 8,
                const char* proof_path = NULL, bool live_share = false,
                double yield_seconds = 0.0);
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
