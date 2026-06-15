/*************************
Copyright 2026 Dagster contributors

This file is part of Dagster (GNU GPL v2+; see SlsChannel.h header).
*************************/

#include "SlsChannel.h"
#include "mpi_global.h"
#include "utilities.h"

#include <glog/logging.h>
#include <stdlib.h>

// Mirrors the SLS transport that previously lived inline in SatSolver.cpp:
// SatSolver constructor (window alloc), get_suggestion (prefix send + RMA read),
// sls__get_solutions (solution receive), backtrack_func (depth reset) and the
// SatSolver destructor (completion signal + window free).

SlsChannel::SlsChannel(MPI_Comm* communicator_sls, int suggestion_size,
                       int sls_solution_capacity) {
  this->communicator_sls = communicator_sls;
  this->suggestion_size = suggestion_size;
  this->currentSLS = 0;
  this->currentSuggestion = 0;
  this->currentSuggestionBuffer = 0;
  this->onGoingGet = false;
  this->lockedSLS = -1;
  this->sls_solution_request = NULL;

  // helper count = communicator size - 1 (rank 0 is this CDCL process)
  if (communicator_sls != NULL) {
    MPI_Comm_size(*communicator_sls, &numSLSProcesses);
    numSLSProcesses--;
  } else {
    numSLSProcesses = 0;
  }

  // per-helper working depth, initially idle (-1)
  TEST_NOT_NULL(processDLevel = (int*)calloc(numSLSProcesses > 0 ? numSLSProcesses : 1, sizeof(int)))
  for (int i = 0; i < numSLSProcesses; i++)
    processDLevel[i] = -1;

  // two alternating suggestion buffers
  TEST_NOT_NULL(suggestions = (int**)calloc(2, sizeof(int*)))
  TEST_NOT_NULL(suggestions[0] = (int*)calloc(suggestion_size, sizeof(int)))
  TEST_NOT_NULL(suggestions[1] = (int*)calloc(suggestion_size, sizeof(int)))

  // SLS-found-solution receive buffer
  this->sls_solution_capacity = sls_solution_capacity;
  if (communicator_sls != NULL && sls_solution_capacity > 0) {
    TEST_NOT_NULL(sls_solution_buffer = (int*)malloc(sls_solution_capacity * sizeof(int)))
  } else {
    sls_solution_buffer = NULL;
  }

  // COLLECTIVE: the CDCL side contributes a zero-length window; each helper
  // exposes its own suggestion buffer. Must pair with the helpers' allocate.
  int* dummy;
  if (numSLSProcesses > 0)
    MPI_Win_allocate(0, sizeof(int), MPI_INFO_NULL, *communicator_sls, &dummy, &window);
}

SlsChannel::~SlsChannel() {
  if (numSLSProcesses > 0)
    VLOG(5) << "signalling completion to SLS";
  // length-0 prefix tells each helper to drop its window and await a new CNF
  int dummy = 0;
  for (int i = 1; i <= numSLSProcesses; i++)
    MPI_Send(&dummy, 0, MPI_INT, i, PREFIX_TAG, *communicator_sls);
  if (numSLSProcesses > 0) {
    if (onGoingGet)
      MPI_Win_unlock(lockedSLS, window);
    MPI_Win_free(&window);
  }
  if (sls_solution_request != NULL)
    MPI_Cancel(&sls_solution_request);
  free(sls_solution_buffer);
  free(processDLevel);
  free(suggestions[0]);
  free(suggestions[1]);
  free(suggestions);
}

void SlsChannel::send_prefix(const int* prefix, int len, int depth) {
  if (numSLSProcesses <= 0)
    return;
  // record the depth this helper is being set to work on (even for an empty
  // prefix, so the caller's cadence guard advances and does not re-fire)
  processDLevel[currentSLS] = depth;
  if (len > 0) {
    MPI_Send(const_cast<int*>(prefix), len, MPI_INT, currentSLS + 1, PREFIX_TAG, *communicator_sls);
    currentSLS = (currentSLS + 1) % numSLSProcesses;
  }
}

void SlsChannel::backtrack_reset(int level) {
  for (int i = 0; i < numSLSProcesses; i++)
    if (processDLevel[i] > level)
      processDLevel[i] = -1;
}

int SlsChannel::next_suggestion(int dLevel) {
  if (numSLSProcesses <= 0)
    return 0;

  // refill when the active buffer is exhausted (end reached or hit a 0 terminator)
  if ((currentSuggestion >= suggestion_size) ||
      (suggestions[currentSuggestionBuffer][currentSuggestion] == 0)) {
    int highestDLevelBelowCurrent = 0;
    int bestSLS = -1;
    int nextSuggestionBuffer = (currentSuggestionBuffer + 1) % 2;
    // wipe the active buffer
    suggestions[currentSuggestionBuffer][0] = 0;
    // pick the helper working the deepest prefix still below our decision level
    for (int i = 0; i < numSLSProcesses; i++) {
      if (processDLevel[i] > highestDLevelBelowCurrent && processDLevel[i] < dLevel) {
        highestDLevelBelowCurrent = processDLevel[i];
        bestSLS = i;
      }
    }
    if (bestSLS >= 0) {
      if (onGoingGet) {
        // close the previous epoch, open a new one on bestSLS, swap buffers
        MPI_Win_unlock(lockedSLS, window);
        MPI_Win_lock(MPI_LOCK_EXCLUSIVE, bestSLS + 1, MPI_MODE_NOCHECK, window);
        MPI_Get(suggestions[currentSuggestionBuffer], suggestion_size, MPI_INT,
                bestSLS + 1, 0, suggestion_size, MPI_INT, window);
        onGoingGet = true;
        lockedSLS = bestSLS + 1;
        currentSuggestionBuffer = nextSuggestionBuffer;
      } else {
        // first time: prime both buffers
        MPI_Win_lock(MPI_LOCK_EXCLUSIVE, bestSLS + 1, MPI_MODE_NOCHECK, window);
        MPI_Get(suggestions[currentSuggestionBuffer], suggestion_size, MPI_INT,
                bestSLS + 1, 0, suggestion_size, MPI_INT, window);
        MPI_Win_unlock(bestSLS + 1, window);
        MPI_Win_lock(MPI_LOCK_EXCLUSIVE, bestSLS + 1, MPI_MODE_NOCHECK, window);
        MPI_Get(suggestions[nextSuggestionBuffer], suggestion_size, MPI_INT,
                bestSLS + 1, 0, suggestion_size, MPI_INT, window);
        onGoingGet = true;
        lockedSLS = bestSLS + 1;
      }
      currentSuggestion = 0;
    }
  }

  // hand back the next non-zero literal, advancing the cursor
  if (currentSuggestion < suggestion_size &&
      suggestions[currentSuggestionBuffer][currentSuggestion] != 0) {
    return suggestions[currentSuggestionBuffer][currentSuggestion++];
  }
  return 0;
}

int SlsChannel::poll_solution(int* out, int capacity, int phase) {
  if (communicator_sls == NULL || sls_solution_buffer == NULL)
    return 0;
  MPI_Status status;
  if (sls_solution_request == NULL)
    MPI_Irecv(sls_solution_buffer, sls_solution_capacity, MPI_INT, MPI_ANY_SOURCE,
              SLS_SOLUTION_TAG, *communicator_sls, &sls_solution_request);
  int incoming = 0;
  if (MPI_Test(&sls_solution_request, &incoming, &status) != MPI_SUCCESS)
    return 0;
  if (!incoming)
    return 0;

  int recv_count = 0;
  MPI_Get_count(&status, MPI_INT, &recv_count);
  int n = 0;
  // trailing element carries the phase tag; only adopt matching-phase solutions
  if (recv_count > 0 && sls_solution_buffer[recv_count - 1] == phase) {
    for (int i = 0; i < recv_count && i + 1 < capacity && sls_solution_buffer[i] != 0; i++) {
      out[i] = sls_solution_buffer[i];
      n++;
    }
    out[n] = 0;  // 0-terminate so callers can iterate `for (; out[i]; i++)`
  } else {
    VLOG(3) << "SLS solution rejected: phase mismatch";
  }
  // re-arm the receive
  MPI_Irecv(sls_solution_buffer, sls_solution_capacity, MPI_INT, MPI_ANY_SOURCE,
            SLS_SOLUTION_TAG, *communicator_sls, &sls_solution_request);
  return n;
}
