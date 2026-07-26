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
  int (*terminate)(void*);  // IPASIR terminate callback (NULL = run to completion)
  void* term_state;
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
  w->terminate = NULL;
  w->term_state = NULL;
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
  int result;
  if (w->terminate == NULL) {
    result = w->s->solve(w->assumptions) ? 10 : 20;   // run to completion (no budget)
  } else {
    // Yielding: Glucose has no during-search callback, so chunk the search by a
    // conflict budget and poll the terminate callback between chunks. solveLimited
    // returns l_Undef only when the budget is hit (not yet solved); conflicts and
    // learned clauses persist across chunks, so this resumes rather than restarts.
    // Return 0 (unknown) when the callback aborts -> IpasirSolver reports "paused".
    const int64_t CHUNK = 50000;            // conflicts between callback polls
    result = 0;
    for (;;) {
      w->s->setConfBudget(CHUNK);
      lbool res = w->s->solveLimited(w->assumptions);
      if (res != l_Undef) { result = (res == l_True) ? 10 : 20; break; }
      if (w->terminate(w->term_state)) break;   // asked to abort -> unknown (0)
    }
    w->s->budgetOff();
  }
  w->assumptions.clear();                   // IPASIR: assumptions last one solve
  return result;
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
  Wrap* w = (Wrap*)solver;
  w->terminate = cb;
  w->term_state = state;
}
void ipasir_set_learn(void* solver, void* state, int max_len, void (*cb)(void*, int*)) {
  (void)solver; (void)state; (void)max_len; (void)cb;
}

}  // extern "C"
