#!/bin/bash
# Build libipasirlingeling.so: fetch Lingeling (Biere), build liblgl.a -fPIC, compile
# the vendored IPASIR glue, and link them into a shared library that Dagster's
# IpasirSolver dlopen's (`--backend lingeling`). Lingeling source is fetched on
# demand (not vendored); the glue (lingeling_glue.cpp) and this script are tracked.
set -e
cd "$(dirname "$0")"
URL="http://fmv.jku.at/lingeling/lingeling-bcj-78ebb86-180517.tar.gz"
DIR=lingeling-build
if [ ! -f "$DIR/lgl/liblgl.a" ]; then
  rm -rf "$DIR"; mkdir -p "$DIR"
  ( cd "$DIR"
    wget -q "$URL" -O lgl.tar.gz
    tar xf lgl.tar.gz && mv lingeling-bcj-* lgl
    cd lgl && ./configure.sh -fPIC && make liblgl.a )
fi
g++ -fPIC -O3 -DNDEBUG -DVERSION='"bcj"' -I. -I"$DIR/lgl" -c lingeling_glue.cpp -o "$DIR/glue.o"
g++ -fPIC -shared -o libipasirlingeling.so "$DIR/glue.o" "$DIR/lgl/liblgl.a" -lm
echo "built $(pwd)/libipasirlingeling.so"
