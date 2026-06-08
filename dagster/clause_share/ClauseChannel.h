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

#ifndef CLAUSE_CHANNEL_H_
#define CLAUSE_CHANNEL_H_

#include <functional>
#include <vector>
#include <mpi.h>

class MpiBuffer;  // ../strengthener/MpiBuffer.h (included only in the .cc)

// Worker-side endpoint of the clause-sharing star: a thin wrapper over a single
// MpiBuffer that talks to the clause hub. Reused by CadicalSolver (analogous to
// SlsChannel for the SLS helpers).
//
// SOUNDNESS: only CDCL learned (conflict) clauses are exported; these are
// entailed by the node formula regardless of the cube assumptions, so any worker
// solving the SAME formula may import them. See utilities/cube/CLAUSE_SHARING_SCOPE.md.
//
// TRANSPORT: MpiBuffer refuses clauses of size < 3 (it asserts) and carries two
// trailing "litPool" position ints per clause. We therefore (a) export only
// clauses of size in [3, max_size], and (b) append two dummy position ints that
// the receiver strips and we ignore. Sharing is best-effort: a dropped clause
// (full buffer, phase skew) only forgoes a hint -- it never affects correctness.
class ClauseChannel {
public:
  // clause_comm holds the workers (ranks 0..N-1) and the hub (rank N); hub_rank
  // is therefore comm_size-1. max_size bounds exported clause length.
  ClauseChannel(MPI_Comm* clause_comm, int hub_rank, int max_size);
  ~ClauseChannel();

  // size filter used by the producer (CadicalSolver::learning) before buffering
  // a learned clause -- avoids accumulating literals we would only discard.
  bool accept_size(int n) const { return n >= 3 && n <= max_size; }

  // export one learned clause (lits, length n). n must satisfy accept_size(n).
  void export_clause(const int* lits, int n);

  // flush queued outbound clauses onto the wire (call between solves / often).
  void flush();

  // drain all clauses the hub has delivered, invoking sink(lits, n) for each.
  void import(const std::function<void(const int*, int)>& sink);

private:
  MpiBuffer* buffer;
  int max_size;
  std::vector<int> scratch;  // [lits..., 0, 0] formatting buffer for export
};

#endif // CLAUSE_CHANNEL_H_
