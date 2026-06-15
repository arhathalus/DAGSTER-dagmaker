/*************************
IPASIR glue for the Glucose SAT solver (vendored under glucose/).

Implements the standard IPASIR C entry points over Glucose's core Solver, so it
builds into a libipasirglucose.so that Dagster's IpasirSolver dlopen's. We wrap the
core Solver (not SimpSolver) -- no variable elimination, hence fully incremental
with no need to freeze variables.

Build: see build_glucose.sh (compiles glucose core + this glue -fPIC into the .so).
*************************/

#include <cstdlib>
#include "core/Solver.h"

using namespace Glucose;

namespace {
struct Wrap {
  Solver* s;
  vec<Lit> clause;       // literals buffered until a 0 terminator
  vec<Lit> assumptions;  // assumptions for the next solve
};
// map a DIMACS literal to a Glucose Lit, growing the variable set as needed.
inline Lit to_lit(Wrap* w, int lit) {
  int v = abs(lit) - 1;
  while (v >= w->s->nVars()) w->s->newVar();
  return mkLit(v, lit < 0);
}
}  // namespace

extern "C" {

const char* ipasir_signature() { return "glucose-4.2.1-ipasir (dagster)"; }

void* ipasir_init() {
  Wrap* w = new Wrap();
  w->s = new Solver();
  return w;
}

void ipasir_release(void* solver) {
  Wrap* w = (Wrap*)solver;
  delete w->s;
  delete w;
}

void ipasir_add(void* solver, int lit) {
  Wrap* w = (Wrap*)solver;
  if (lit == 0) {
    w->s->addClause(w->clause);
    w->clause.clear();
  } else {
    w->clause.push(to_lit(w, lit));
  }
}

void ipasir_assume(void* solver, int lit) {
  Wrap* w = (Wrap*)solver;
  w->assumptions.push(to_lit(w, lit));
}

int ipasir_solve(void* solver) {
  Wrap* w = (Wrap*)solver;
  bool sat = w->s->solve(w->assumptions);   // runs to completion (no budget)
  w->assumptions.clear();                   // IPASIR: assumptions last one solve
  return sat ? 10 : 20;
}

int ipasir_val(void* solver, int lit) {
  Wrap* w = (Wrap*)solver;
  int v = abs(lit) - 1;
  lbool val = (v < w->s->model.size()) ? w->s->model[v] : l_Undef;
  return (val == l_True) ? abs(lit) : -abs(lit);
}

// minimal IPASIR completeness (Dagster's adapter does not call these):
int ipasir_failed(void* solver, int lit) { (void)solver; (void)lit; return 0; }
void ipasir_set_terminate(void* solver, void* state, int (*cb)(void*)) {
  (void)solver; (void)state; (void)cb;
}
void ipasir_set_learn(void* solver, void* state, int max_len, void (*cb)(void*, int*)) {
  (void)solver; (void)state; (void)max_len; (void)cb;
}

}  // extern "C"
