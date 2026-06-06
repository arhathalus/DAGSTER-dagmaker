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

#ifndef SLS_CHANNEL_H_
#define SLS_CHANNEL_H_

#include <mpi.h>

/**
 * SlsChannel -- the reusable CDCL-side client of Dagster's SLS (gnovelty+)
 * guidance protocol.
 *
 * One CDCL solving unit (rank 0 of communicator_sls) is helped by N gnovelty
 * stochastic-local-search processes (ranks 1..N of the same communicator). The
 * exchange is:
 *   - a COLLECTIVE MPI_Win_allocate over communicator_sls (the CDCL side
 *     contributes a zero-length window; each helper exposes a suggestion
 *     buffer). This MUST be entered in lockstep with the helpers' own
 *     MPI_Win_allocate (gnovelty_main.cc) or the run deadlocks.
 *   - send_prefix(): the CDCL process round-robins its current partial
 *     assignment to a helper via MPI_Send(PREFIX_TAG); the helper then searches
 *     completions of that prefix. A length-0 prefix is the reset/teardown
 *     signal (handled in the destructor).
 *   - next_suggestion(): the CDCL process double-buffer MPI_Gets a window of
 *     suggested literals from whichever helper is working the deepest prefix
 *     still below the current decision level.
 *   - poll_solution(): a separate non-blocking MPI_Irecv(SLS_SOLUTION_TAG)
 *     channel by which a helper can hand a full satisfying assignment back.
 *
 * The class owns only the *transport*; backend-specific policy (when to send a
 * prefix, how to turn a raw suggested literal into a decision / phase hint,
 * whether to adopt an SLS solution) stays in the SatSolverInterface backend.
 * This is what lets SatSolver and CadicalSolver share one implementation.
 */
class SlsChannel {
public:
  MPI_Comm* communicator_sls;   // CDCL = rank 0, gnovelty helpers = ranks 1..numSLSProcesses
  int numSLSProcesses;          // = comm size - 1
  int suggestion_size;          // ints per suggestion window
  MPI_Win window;               // collective; CDCL allocates length 0, GETs from helpers

  // prefix dispatch / round-robin bookkeeping
  int* processDLevel;           // [numSLSProcesses] decision depth helper i is working on (-1 = idle)
  int currentSLS;               // next helper to receive a prefix

  // double-buffered suggestion read
  int** suggestions;            // [2][suggestion_size]
  int currentSuggestion;        // index into the active buffer
  int currentSuggestionBuffer;  // 0/1
  bool onGoingGet;              // an RMA epoch (lock) is open on lockedSLS
  int lockedSLS;                // rank currently locked (helper index + 1)

  // SLS-found full-solution channel (non-blocking receive)
  int* sls_solution_buffer;
  int sls_solution_capacity;
  MPI_Request sls_solution_request;

  /**
   * COLLECTIVE. Construct on the CDCL rank in lockstep with the gnovelty
   * helpers' MPI_Win_allocate. sls_solution_capacity bounds the size of an
   * SLS-supplied solution (typically max_vc + 2).
   */
  SlsChannel(MPI_Comm* communicator_sls, int suggestion_size, int sls_solution_capacity);

  /** Sends the length-0 prefix completion signal to every helper, closes any
   *  open RMA epoch, and frees the collective window. */
  ~SlsChannel();

  /** True iff there is at least one helper to talk to. */
  bool active() const { return communicator_sls != NULL && numSLSProcesses > 0; }

  /**
   * Send a partial assignment (prefix[0..len) signed literals) to the next
   * helper and record that helper as working at decision depth `depth`. The
   * helper is only sent to (and the round-robin only advances) when len > 0; an
   * empty prefix still marks the depth so the caller's cadence guard advances.
   */
  void send_prefix(const int* prefix, int len, int depth);

  /** On backtrack to `level`, mark every helper working deeper than `level` as
   *  idle so its (now-stale) suggestions are not chosen. */
  void backtrack_reset(int level);

  /**
   * Return the next raw suggested literal usable at decision level `dLevel`, or
   * 0 if none is currently available. Internally refills the double buffer via
   * RMA from the best helper (deepest prefix below dLevel) when exhausted. The
   * caller decides whether the literal is usable (e.g. still free) and loops.
   */
  int next_suggestion(int dLevel);

  /**
   * Non-blocking poll of the SLS-found-solution channel. If a solution whose
   * trailing phase tag matches `phase` has arrived, copies its 0-terminated
   * literal list into out[0..capacity) and returns the literal count (>0);
   * otherwise returns 0. The caller checks novelty / decides adoption.
   */
  int poll_solution(int* out, int capacity, int phase);
};

#endif // SLS_CHANNEL_H_
