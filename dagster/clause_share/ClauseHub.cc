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

#include "ClauseHub.h"
#include "../mpi_global.h"
#include "../strengthener/MpiBuffer.h"

#include <algorithm>
#include <set>
#include <vector>
#include <unistd.h>
#include <glog/logging.h>

// Star relay: one MpiBuffer per worker. Each loop, drain every worker's inbound
// clauses; for each clause not seen before, rebroadcast it to the other workers.
// Dedup is exact (sorted-clause set) so a clause crosses the hub at most once.
void clause_hub_main(MPI_Comm* clause_comm) {
  int comm_size;
  MPI_Comm_size(*clause_comm, &comm_size);
  const int N = comm_size - 1;        // workers are ranks 0..N-1; hub is rank N
  if (N <= 0) return;                 // nothing to relay between

  std::vector<MpiBuffer*> bufs(N);
  for (int i = 0; i < N; i++)
    bufs[i] = new MpiBuffer(clause_comm, CLAUSE_SHARE_PHASE, i,
                            CLAUSE_SHARE_W2H_TAG, CLAUSE_SHARE_H2W_TAG);

  std::set<std::vector<int>> seen;    // sorted-clause signatures already relayed
  // Memory guard: clause sharing is best-effort, so if the dedup set grows huge
  // we simply forget history (re-relaying a few clauses is harmless).
  const size_t SEEN_CAP = 2000000;

  long long relayed = 0, received = 0;
  int kills = 0;
  std::vector<int> sig, out;

  while (kills < N) {
    // 1. teardown: each worker sends exactly one KILL as it shuts down.
    int flag = 0;
    MPI_Status st;
    MPI_Iprobe(MPI_ANY_SOURCE, CLAUSE_SHARE_KILL_TAG, *clause_comm, &flag, &st);
    if (flag) {
      int dummy;
      MPI_Recv(&dummy, 1, MPI_INT, st.MPI_SOURCE, CLAUSE_SHARE_KILL_TAG,
               *clause_comm, MPI_STATUS_IGNORE);
      kills++;
      continue;
    }

    // 2. relay: drain each worker, dedupe, rebroadcast to the others.
    bool any = false;
    for (int i = 0; i < N; i++) {
      ClauseWithPos* cw;
      while ((cw = bufs[i]->getClause()) != NULL) {
        any = true;
        received++;
        sig = cw->clause;
        std::sort(sig.begin(), sig.end());
        if (seen.insert(sig).second) {
          if (seen.size() > SEEN_CAP) seen.clear();
          const int n = (int)cw->clause.size();
          out.clear();
          out.reserve(n + 2);
          out.insert(out.end(), cw->clause.begin(), cw->clause.end());
          out.push_back(0);  // dummy litPool
          out.push_back(0);  // dummy litPoolPos
          for (int j = 0; j < N; j++)
            if (j != i) { bufs[j]->pushClause(out.data(), n + 2); relayed++; }
        }
        delete cw;
      }
    }

    // 3. push queued outbound buffers onto the wire.
    for (int i = 0; i < N; i++) bufs[i]->readyToSend();

    // 4. yield when idle so the hub doesn't spin a core at 100%.
    if (!any) usleep(200);
  }

  // flush any last queued clauses before tearing down the buffers.
  for (int i = 0; i < N; i++) bufs[i]->readyToSend();
  VLOG(1) << "CLAUSE HUB: received " << received << " clauses, relayed "
          << relayed << " (unique " << seen.size() << ")";
  for (int i = 0; i < N; i++) delete bufs[i];
}
