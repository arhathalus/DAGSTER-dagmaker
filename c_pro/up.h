#ifndef _UP_H_
#define _UP_H_

#include "cnf.h"

namespace up {

  struct Result {
    bool sat = true;                  // false if known to be UNSAT
    std::vector<int> value;           // size num_vars+1, {-1 - False, 0 - Free, +1 - True}
    std::vector<int> trail;           // PLE + BCP fixpoint prefix as signed literals (positive => true)
  };

  Result simplify(const CNF& in, CNF& out, bool eager_rebuild = true);

} 

#endif
