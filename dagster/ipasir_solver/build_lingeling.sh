#!/bin/bash
# Build libipasirlingeling.so from a LOCAL Lingeling checkout (no fetching).
#
# Provide the source yourself first (GitHub, not the website):
#     git clone https://github.com/arminbiere/lingeling ipasir_solver/lingeling
# then:
#     bash ipasir_solver/build_lingeling.sh            # uses ./lingeling
#     bash ipasir_solver/build_lingeling.sh /path/lgl  # or a custom source dir
#
# It builds liblgl.a (-fPIC) if not already present, compiles the vendored IPASIR
# glue (lingeling_glue.cpp), and links them into libipasirlingeling.so for
# `dagster --backend lingeling`. The source dir is git-ignored (you supply it).
set -e
cd "$(dirname "$0")"
LGL="${1:-lingeling}"

if [ ! -d "$LGL" ]; then
  echo "Lingeling source not found at '$LGL'."
  echo "Get it:  git clone https://github.com/arminbiere/lingeling $LGL"
  exit 1
fi

if [ ! -f "$LGL/liblgl.a" ]; then
  echo "building liblgl.a (-fPIC) in $LGL ..."
  ( cd "$LGL" && ./configure.sh -fPIC && make liblgl.a )
fi

g++ -fPIC -O3 -DNDEBUG -DVERSION='"github"' -I. -I"$LGL" -c lingeling_glue.cpp -o lingeling_glue.o
g++ -fPIC -shared -o libipasirlingeling.so lingeling_glue.o "$LGL/liblgl.a" -lm
echo "built $(pwd)/libipasirlingeling.so"
