#!/bin/bash
# Build libipasirmaplecomsps.so: MapleCOMSPS core + the IPASIR glue, position-
# independent, into a shared library that Dagster's IpasirSolver dlopen's
# (`--backend maple`, or `--backend ipasir --ipasir-lib .../libipasirmaplecomsps.so`).
#
# You SUPPLY the source (the build does NOT fetch), like build_lingeling.sh:
#   git clone -b assumptions-incremental https://bitbucket.org/JLiangWaterloo/maplesat maple
# then the clean MapleSAT tree lives at maple/maplesat (core/ mtl/ utils/).
#
# IMPORTANT: use the `assumptions-incremental` branch, NOT the `maplecomsps`
# branch. Dagster drives the node solver INCREMENTALLY with assumptions (the DAG
# interface assignment / cubes). The competition `maplecomsps` branch mishandles
# the model under assumptions -> Dagster rejects it ("ipasir backend returned
# false solution") on any multi-node DAG. The `assumptions-incremental` branch's
# maplesat/ is verified to match CaDiCaL on multi-node enumeration. (That branch
# also happens to be free of the MathCheck programmatic hooks in maplecomsps/.)
# Pass a different source root as $1 if your checkout is elsewhere.
set -e
cd "$(dirname "$0")"
SRC_ROOT="${1:-maple/maplesat}"
if [ ! -f "$SRC_ROOT/core/Solver.cc" ]; then
  echo "Maple source not found at $SRC_ROOT/core/Solver.cc" >&2
  echo "  git clone -b assumptions-incremental https://bitbucket.org/JLiangWaterloo/maplesat maple" >&2
  echo "  (then it lives at maple/maplesat; or pass a checkout path: build_maple.sh /path/to/maplesat)" >&2
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
