#include "up.h"

#include <queue>
#include <algorithm>
#include <cassert>
#include <iostream>

namespace up {

  // internal linkage only
  namespace {

    inline int lit_index(int lit) {
      const int v = (lit > 0 ? lit : -lit);
      return (v << 1) ^ (lit < 0);
    }

    inline int lit_value(int lit, const std::vector<int>& value) {
      const int v = (lit > 0 ? lit : -lit);
      assert(v >= 0 && static_cast<size_t>(v) < value.size());
      const int a = value[v];
      if (a == 0) return 0;
      return ( (a > 0) == (lit > 0) ) ? +1 : -1;
    }
    
    struct Clause {
      size_t begin = 0, end = 0;     // CNF::lits [begin, end)
      int watch_pos[2] = {0, 0};     // watched literals
      bool satisfied = false;        // assign true if a watched literal is true
    };

    struct WatchRef {
      int clause = -1;               // clause index, initially invalid
      int which = 0;                 // 0 or 1 -> which watched literal this entry refers to
    };
    
    struct Engine {
      const CNF& in;
      int nvars = 0, nclauses = 0;

      std::vector<Clause> cls;                       // problem
      std::vector<std::vector<WatchRef>> watchers;   // map literals to clauses where they are watched

      std::vector<int> value;                        // assignments in {-1 - False, 0 - Free, +1 - True}
      std::vector<int> trail;
      std::queue<int> q;                             // q(ueued) assignments

      Engine(const CNF& cnf) : in(cnf) {
        nvars = in.num_vars;
        nclauses = in.num_clauses;
        value.assign(nvars + 1, 0);
        watchers.resize(2 * (nvars + 1));
        cls.resize(nclauses);

	std::cerr<< "initialising watchers for problem p cnf "<<in.num_vars<<" "<<in.num_clauses<<"  \n" ;
        build_watches();
      }

      void build_watches() {
        for (int ci = 0; ci < nclauses; ++ci) {
	  size_t b = in.clause_offsets[ci];
	  size_t e = in.clause_offsets[ci + 1];
	  Clause& C = cls[ci];
	  C.begin = b; C.end = e;
	  const int len = static_cast<int>(e - b);
	  //std::cerr<< "Clause "<<ci<<" is of length "<<len<<std::endl; 
	  if (len == 0) {
	    std::cerr<< "got an empty clause; problem is UNSAT. \n";assert(0);
	    // immediate conflict as empty clause is not satisfiable
	    continue;
	  }
	  if (len == 1) { // Unit clause
	    C.watch_pos[0] = 0;
	    const int lit0 = in.lits[b];
	    watchers[lit_index(lit0)].push_back({ci, 0});
	    //std::cerr << " forcing : "<< lit0 << std::endl;
	    enqueue(lit0);// forced decision, add to $q$
	  } else { // has two or more literals
	    C.watch_pos[0] = 0;
	    C.watch_pos[1] = 1;
	    const int lit0 = in.lits[b + 0]; // first literal in C
	    const int lit1 = in.lits[b + 1]; // second literal in C
	    watchers[lit_index(lit0)].push_back({ci, 0});
	    watchers[lit_index(lit1)].push_back({ci, 1});
	  }
        }
      }
      
      bool enqueue(int lit) {
        const int v = (lit > 0 ? lit : -lit);
        const int s = (lit > 0 ? +1 : -1);
        if (value[v] != 0) return value[v] == s;
        value[v] = s;
        trail.push_back(lit);
        q.push(lit);
        return true;
      }

      // Try to move a watch off a falsified literal; returns:
      //  Returns true if clause satisfied or watch moved
      //  Otherwise watch not moved because:
      //   - given trail clause now equiv. unit -> enqueue, still returns true
      //   - conflicted, returns false
      //
      // Caller must clean up old watch
      bool process_watch_on_falsified(int ci, int which, bool& conflict) {
        Clause& C = cls[ci];
        if (C.satisfied) return true; // nothing to do

        const int len = static_cast<int>(C.end - C.begin);
        int& pos_f = C.watch_pos[which];
        const int other = 1 - which;
        int& pos_o = C.watch_pos[other];

        const int lit_o = in.lits[C.begin + pos_o];
        if (lit_value(lit_o, value) == +1) { // C is SAT
	  C.satisfied = true;
	  return true;
        }

        // Try to find a new watch that is not false
        for (int k = 0; k < len; ++k) {
	  if (k == pos_f || k == pos_o) continue; // not a new watch
	  const int lit_k = in.lits[C.begin + k];
	  const int v = lit_value(lit_k, value);
	  if (v != -1) { // literal is unassigned or satisfied
	    pos_f = k;
	    watchers[lit_index(lit_k)].push_back({ci, which});// add C to new watch clauses
	    return true; // caller must clean up old watch
	  }
        }

        const int v_o = lit_value(lit_o, value);
        if (v_o == 0) { // try to enqueue lit_o as it's forced
	  if (!enqueue(lit_o)) { conflict = true; return false; }
	  return true;
        }
        if (v_o == +1) { // Clause is satisfied
	  C.satisfied = true;
	  return true;
        }

        // both watched literals are false with no possible move -> conflict
        conflict = true; return false;
      }

      // Process queue
      // Returns false on conflict.
      bool propagate() {
        while (!q.empty()) {
	  const int lit_true = q.front();
	  q.pop();
	  
	  // Impacted clauses will be in vec
	  const int lit_false = -lit_true;
	  auto& vec = watchers[lit_index(lit_false)];

	  // iterate with manual index because we might swap-pop inside
	  for (size_t i = 0; i < vec.size();) {
	    const WatchRef wr = vec[i]; // watcher detail
	    bool conflict = false;
	    if (!process_watch_on_falsified(wr.clause, wr.which, conflict)) {
	      return false; // conflict
	    }

	    // Cleanup if watcher moved
	    Clause& C = cls[wr.clause];
	    const int pos = C.watch_pos[wr.which];
	    const int now_lit = in.lits[C.begin + pos];
	    if (now_lit != lit_false) { // watcher vec[i] defunct, cleanup
	      vec[i] = vec.back();
	      vec.pop_back();
	    } else { // no change, no cleanup and carry on to next clause
	      ++i;
	    }
	  }
        }
        return true;
      }

      int pure_literal_elimination() {
        const int L = 2 * (nvars + 1);
        std::vector<int> occ(L, 0); // zero initialisation

        for (int ci = 0; ci < nclauses; ++ci) {
	  Clause& C = cls[ci];
	  if (C.satisfied) continue; // nothing to do
	  
	  bool clause_true = false;
	  for (size_t t = C.begin; t < C.end; ++t) {
	    const int v = lit_value(in.lits[t], value);
	    if (v == +1) { clause_true = true; break; } // nothing to do, clause satisfied and set below
	  }
	  if (clause_true) { C.satisfied = true; continue; }

	  for (size_t t = C.begin; t < C.end; ++t) {
	    const int lit = in.lits[t];
	    const int v = lit_value(lit, value);
	    if (v == 0) {
	      occ[lit_index(lit)]++; // increment occurrence count
	    }
	  }
        }

        int newly_assigned = 0; // return value, is positive is pure literals found
        // check for purity -- i.e., only occurs in one polarity
        for (int v = 1; v <= nvars; ++v) {
	  if (value[v] != 0) continue; // an assignment has been made to v, nothing to do as purity is irrelevant 
	  const int pos = (v << 1);
	  const int neg = pos ^ 1;
	  const bool has_pos = occ[pos] > 0;
	  const bool has_neg = occ[neg] > 0;
	  if (has_pos && !has_neg) { // pure positive
	    if (enqueue(+v)) ++newly_assigned;// queue decision
	  } else if (!has_pos && has_neg) { // pure negative
	    if (enqueue(-v)) ++newly_assigned; // queue decision
	  }
        }
        return newly_assigned;
      }

      // Run PLE  and then BCP until a fixpoint, return false on conflict.
      bool to_fixpoint() {
        size_t assigned_before = 0;
        while (true) {
	  // PLE
	  int num_pures = pure_literal_elimination();
	  std::cerr << "got "<<num_pures<< " pure literals.\n";
	  if (!propagate()) { return false; // UNSAT as PLE + BCP propagated to unsat
	  }

	  // If fixpoint
	  if (trail.size() == assigned_before && num_pures == 0){
	    std::cerr << "No further decisions to propagate, fixpoint reached.\n";
	    return true; //finished
	  }
	  
	  assigned_before = trail.size(); // track number of assignments made this iteration for fixpoint detection
        }
      }

      // Argument should represent result of iterating PLE + BCP to a fixpoint
      // We do not rename variables or anything like that. 
      bool rebuild(CNF& out) {
        out.num_vars = nvars;
        out.num_clauses = 0;
        out.lits.clear();
        out.clause_offsets.clear();
        out.clause_offsets.reserve(in.num_clauses + 1);
        out.clause_offsets.push_back(0);

        for (int ci = 0; ci < nclauses; ++ci) {
	  Clause& C = cls[ci];
	  bool clause_true = C.satisfied;
	  if (!clause_true) { // perhaps keep
	    for (size_t t = C.begin; t < C.end; ++t) {
	      if (lit_value(in.lits[t], value) == +1) { clause_true = true; break; } // drop, is SAT see below
	    }
	  }
	  if (clause_true) continue; // drop, is SAT

	  // Retain unassigned literals
	  size_t before = out.lits.size();
	  for (size_t t = C.begin; t < C.end; ++t) {
	    const int lv = lit_value(in.lits[t], value);
	    if (lv == 0) out.lits.push_back(in.lits[t]);
	  }
	  size_t after = out.lits.size();
	  if (after == before) {
	    assert(0); // not expecting UNSAT discovery via empty clause here. 
	    // Empty clause left => unsat (should not occur if BCP caught all)
	    return false;
	  }
	  ++out.num_clauses;
	  out.clause_offsets.push_back(after);
        }
        return true;
      }
    };

  } // local linking only

  Result simplify(const CNF& in, CNF& out, bool eager_rebuild) {
    Engine E(in);

    Result R;
    // Run PLE + BCP to fixpoint
    if (!E.to_fixpoint()) {
      R.sat = false;
      return R;
    }
    

    if (eager_rebuild) {
      if (!E.rebuild(out)) {
	R.sat = false; 
      } else {
	out.num_vars = in.num_vars;
      }
    }

    
    R.sat   = true;
    R.value = std::move(E.value);
    R.trail = std::move(E.trail);
    
    
    return R;
  }

} 
