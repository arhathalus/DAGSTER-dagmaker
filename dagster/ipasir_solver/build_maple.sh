#!/bin/bash
# Build libipasirmaplecomsps.so: MapleCOMSPS core + the IPASIR glue, position-
# independent, into a shared library that Dagster's IpasirSolver dlopen's
# (`--backend maple`, or `--backend ipasir --ipasir-lib .../libipasirmaplecomsps.so`).
#
# The MapleSAT source is VENDORED in this repo at ipasir_solver/maplesat/ (core/
# mtl/ utils/ simp/), so this builds standalone -- no fetch needed. It is the
# `assumptions-incremental` branch of bitbucket JLiangWaterloo/maplesat, NOT the
# `maplecomsps` branch: Dagster drives the node solver INCREMENTALLY with
# assumptions (the DAG interface assignment / cubes), and the competition
# `maplecomsps` branch mishandles the model under assumptions -> Dagster rejects it
# ("ipasir backend returned false solution") on any multi-node DAG. This tree is
# verified to match CaDiCaL on multi-node enumeration (and is free of the MathCheck
# programmatic hooks in maplecomsps/). Pass a source root as $1 to override.
set -e
cd "$(dirname "$0")"
SRC_ROOT="${1:-maplesat}"
if [ ! -f "$SRC_ROOT/core/Solver.cc" ]; then
  echo "MapleSAT source not found at $SRC_ROOT/core/Solver.cc" >&2
  echo "  (it should be vendored at ipasir_solver/maplesat/; or pass a path: build_maple.sh /path/to/maplesat)" >&2
  exit 1
fi
CXX=${CXX:-g++}
SRC="$SRC_ROOT/core/Solver.cc $SRC_ROOT/utils/Options.cc $SRC_ROOT/utils/System.cc maple_glue.cc"
# -fpermissive: MapleCOMSPS (2017) has a friend mkLit() decl with a default arg,
#   which modern g++ rejects; -fpermissive downgrades it to a warning (standard
#   workaround for MiniSat-era code). -w silences the noisy PRIi64/format warnings.
$CXX -fPIC -shared -O3 -std=c++17 -fpermissive -w \
     -DNDEBUG -D__STDC_LIMIT_MACROS -D__STDC_FORMAT_MACROS \
     -I"$SRC_ROOT" -I. $SRC -o libipasirmaplecomsps.so
echo "built $(pwd)/libipasirmaplecomsps.so"
