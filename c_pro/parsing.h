/*************************
Copyright 2026 Charles Gretton

This file is part of Dagster toolchain.

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


#ifndef _PARSING_H_
#define _PARSING_H_

#include <cstdio>
#include <cstdlib>
#include <cctype>
#include <vector>
#include <string>
#include <stdexcept>
#include <cstring>

#include "cnf.h"

struct Quick_Reader {
  static constexpr size_t BUFFER_SIZE = 1u << 20; // apprx. 1 MiB
  FILE* f;
  unsigned char buf[BUFFER_SIZE];
  size_t i = 0, n = 0;

  explicit Quick_Reader(FILE* fp) : f(fp) {}

  inline int refill() {
    n = std::fread(buf, 1, BUFFER_SIZE, f);
    i = 0;
    return (n == 0) ? EOF : 0;
  }
  inline int peek() { //returns int for EOF
    if (i >= n) if (refill() == EOF) return EOF;
    return buf[i];
  }
  inline int get() {
    if (i >= n) if (refill() == EOF) return EOF;
    return buf[i++];
  }
  inline void skip_line() {
    int c;
    do { c = get(); } while (c != EOF && c != '\n');
  }
  inline void skip_all_skippables() {
    for (;;) {
      int c = peek();
      if (c == EOF) return;
      if (c == 'c') { skip_line(); continue; } // DIMACS comment line
      if (std::isspace(c)) { get(); continue; }
      break;
    }
  }
};

static inline bool read_int(Quick_Reader& in, int& out) {
  in.skip_all_skippables();
  int c = in.peek();
  if (c == EOF) return false;
  int sign = 1;
  if (c == '+' || c == '-') {
    sign = (c == '-') ? -1 : 1;
    in.get();
    c = in.peek();
    if (!std::isdigit(c)) throw std::runtime_error("Invalid integer in DIMACS");
  }
  if (!std::isdigit(c)) return false; // next token isn't an int
  long val = 0;
  do {
    c = in.get();
    val = val * 10 + (c - '0');
    c = in.peek();
  } while (std::isdigit(c));
  out = static_cast<int>(sign * val);
  return true;
}

static void parse_header(Quick_Reader& in, int& nvars, int& nclauses) {
  if (in.peek() != 'p') throw std::runtime_error("DIMACS header: expected 'p'"); // e.g., in case EOF
  in.get();

  int c;
  do { c = in.get(); } while (c != EOF && std::isspace(c));

  char kw[8]; int k = 0;
  for (; c != EOF && !std::isspace(c) && k < 7; c = in.get()) kw[k++] = char(c);
  if (k == 0) throw std::runtime_error("DIMACS header: missing format keyword");
  kw[k] = '\0';
  if (std::strcmp(kw, "cnf") != 0) throw std::runtime_error("DIMACS header: expected 'cnf'");
  if (!read_int(in, nvars))   throw std::runtime_error("DIMACS header: missing <num_vars>");
  if (!read_int(in, nclauses)) throw std::runtime_error("DIMACS header: missing <num_clauses>");
}




static void parse_dimacs(FILE* fp, CNF& cnf, bool reserve_heuristic = true) {
  Quick_Reader in(fp);

  for (;;) {
    in.skip_all_skippables();
    int c = in.peek();
    if ( c == 'p' || c == EOF) break; // found header or end-of-file 
    break;
  }

  parse_header(in
	       , cnf.num_vars
	       , cnf.num_clauses);

  cnf.lits.clear();
  cnf.clause_offsets.clear();
  cnf.clause_offsets.reserve(static_cast<size_t>(cnf.num_clauses) + 1);
  if (reserve_heuristic) cnf.lits.reserve(static_cast<size_t>(cnf.num_clauses) * 3); // heuristic is 3SAT
  cnf.clause_offsets.push_back(0);

  int lit;
  int clauses_read = 0;
  size_t lit_count = 0;

  while (clauses_read < cnf.num_clauses) {
    bool got_any = false;
    for (;;) {
      if (!read_int(in, lit)) {
	if (clauses_read == cnf.num_clauses) break; // ignore any spurious clauses at end of file
	throw std::runtime_error("Unexpected EOF while reading clause literals");
      }
      if (lit == 0) {
	// end-of-clause
	if (!got_any) { // no literals parsed
	  // Empty clause representing UNSAT
	}
	++clauses_read;
	cnf.clause_offsets.push_back(cnf.lits.size());
	break;
      } else {
	cnf.lits.push_back(lit);
	got_any = true;
	++lit_count;
      }
    }
  }

  if ((int)cnf.clause_offsets.size() != cnf.num_clauses + 1)
    throw std::runtime_error("Clause count mismatch after parsing");
}


static void parse_file(FILE* file_pointer, CNF& cnf){
  parse_dimacs(file_pointer, cnf);
}


#endif
