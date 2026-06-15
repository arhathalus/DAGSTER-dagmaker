#!/bin/bash
# Build libipasirglucose.so: vendored Glucose core + the IPASIR glue, position-
# independent, into a shared library that Dagster's IpasirSolver dlopen's
# (`--backend glucose`). Re-run if glucose/ or glucose_glue.cc changes.
set -e
cd "$(dirname "$0")"
CXX=${CXX:-g++}
SRC="glucose/core/Solver.cc glucose/core/lcm.cc glucose/utils/Options.cc glucose/utils/System.cc glucose_glue.cc"
$CXX -fPIC -shared -O3 -std=c++17 -DNDEBUG -D__STDC_LIMIT_MACROS -D__STDC_FORMAT_MACROS \
     -Iglucose -I. $SRC -o libipasirglucose.so
echo "built $(pwd)/libipasirglucose.so"
