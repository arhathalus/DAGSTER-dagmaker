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

#ifndef CLAUSE_HUB_H_
#define CLAUSE_HUB_H_

#include <mpi.h>

// Entry point for the clause-hub process in the clause-sharing topology.
// clause_comm contains the N conquer workers (ranks 0..N-1) and this hub (the
// last rank, N). The hub collects learned clauses from every worker, dedupes
// them, and rebroadcasts each new clause to the OTHER workers. It returns when
// all N workers have signalled teardown (CLAUSE_SHARE_KILL_TAG).
void clause_hub_main(MPI_Comm* clause_comm);

#endif // CLAUSE_HUB_H_
