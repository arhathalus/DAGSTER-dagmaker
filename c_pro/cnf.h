/*************************
Copyright 2026 Charles Gretton

This file is part of the Dagster toolchain.

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

You should have received a copy of the GNU General 
Public License along with Dagster.
If not, see <http://www.gnu.org/licenses/>.
*************************/

#ifndef _CNF_H_
#define _CNF_H_

struct CNF {
  int num_vars = 0;
  int num_clauses = 0;
  std::vector<int> lits; 
  std::vector<size_t> clause_offsets;  // size == num_clauses + 1
};


#endif
