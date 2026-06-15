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

#include "ClauseChannel.h"
#include "../mpi_global.h"
#include "../strengthener/MpiBuffer.h"

// Worker endpoint: receive from the hub on H2W, send to the hub on W2H.
ClauseChannel::ClauseChannel(MPI_Comm* clause_comm, int hub_rank, int max_size)
    : max_size(max_size) {
  this->buffer = new MpiBuffer(clause_comm, CLAUSE_SHARE_PHASE, hub_rank,
                               CLAUSE_SHARE_H2W_TAG, CLAUSE_SHARE_W2H_TAG);
}

ClauseChannel::~ClauseChannel() {
  delete this->buffer;
}

void ClauseChannel::export_clause(const int* lits, int n) {
  if (!accept_size(n)) return;
  // MpiBuffer wire format: [lit_0..lit_{n-1}, litPool, litPoolPos]. We don't use
  // the pool fields, so emit dummy zeros; the receiver strips them.
  scratch.clear();
  scratch.reserve(n + 2);
  for (int i = 0; i < n; i++) scratch.push_back(lits[i]);
  scratch.push_back(0);  // litPool    (unused)
  scratch.push_back(0);  // litPoolPos (unused)
  buffer->pushClause(scratch.data(), (int)scratch.size());
}

void ClauseChannel::flush() {
  // readyToSend() == testAndSwitchBufferOut(): ships a filled outbound buffer
  // once the previous send completes. Calling it opportunistically drains queued
  // clauses without blocking.
  buffer->readyToSend();
}

void ClauseChannel::import(const std::function<void(const int*, int)>& sink) {
  ClauseWithPos* cw;
  // getClause() returns NULL both when empty and when it has just rotated to a
  // freshly arrived buffer, so make a few passes to keep up with the hub.
  for (int pass = 0; pass < 2; pass++) {
    while ((cw = buffer->getClause()) != NULL) {
      sink(cw->clause.data(), (int)cw->clause.size());
      delete cw;  // MpiBuffer::getClause allocates; caller owns it
    }
  }
}
