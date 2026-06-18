# Building Dagster

The tricky part isn't `make` — it's getting the **dependencies in the right
order**, because `dagster/Makefile` *auto-detects* the two optional solver
libraries (`libcadical.a`, `libcryptominisat5.a`) at build time. If they aren't
built **before** you run `make`, those backends are silently left out (you'll see
`NOTE: ... not found -- building WITHOUT the cadical backend`). So the order is:

> **submodules → system libs → CUDD → CaDiCaL → CryptoMiniSat → `make` dagster**

## TL;DR (Ubuntu/Debian)

```bash
# 0. get the repo WITH submodules (CaDiCaL + CryptoMiniSat are git submodules)
git clone --recursive <repo-url> dagster        # or, in an existing clone:
git submodule update --init                      #   pull the two solver submodules

cd dagster                                        # repo root

# 1. system packages
sudo apt-get update && sudo apt-get install -y \
    build-essential cmake git wget \
    libopenmpi-dev openmpi-bin \
    libgoogle-glog-dev zlib1g-dev \
    libgtest-dev                                 # only needed for the C++ unit tests

# 2. CUDD (NOT in apt — build from source, install to /usr/local).
#    Get cudd-3.0.0.tar.gz from https://davidkebo.com/cudd (direct link below may
#    change — grab whatever 3.0.0 tarball that page links if wget 404s):
wget https://davidkebo.com/source/cudd_versions/cudd-3.0.0.tar.gz
tar xzf cudd-3.0.0.tar.gz && cd cudd-3.0.0
./configure --enable-shared --enable-obj       # if configure/make hits aclocal skew: autoreconf -fi
make -j
sudo make install && sudo ldconfig             # installs libcudd.{so,a} + cudd.h into /usr/local
cd ..

# 3. CaDiCaL static lib   ->  cadical_solver/cadical/build/libcadical.a
( cd dagster/cadical_solver/cadical && ./configure && make -j )

# 4. CryptoMiniSat static lib -> cryptominisat_solver/cryptominisat/build/lib/libcryptominisat5.a
( cd dagster/cryptominisat_solver/cryptominisat && mkdir -p build && cd build && cmake .. && make -j )

# 5. Dagster itself (auto-detects the two .a libs built above)
( cd dagster && make )
```

The repo layout has the solvers under `dagster/cadical_solver/cadical` and
`dagster/cryptominisat_solver/cryptominisat`; adjust the `cd` paths if you run the
commands from a different directory. (`dagster` appears twice: the repo root and
the `dagster/` source subdir that holds the Makefile.)

## What each dependency is for

| dependency | provides | how to get it |
|---|---|---|
| **MPI** (OpenMPI) | `mpic++`/`mpirun` — Dagster is MPI parallel | `apt install libopenmpi-dev openmpi-bin` |
| **glog** | logging (`-lglog`) | `apt install libgoogle-glog-dev` |
| **zlib** | gzip'd CNF I/O (`-lz`) | `apt install zlib1g-dev` |
| **CUDD** | BDD master / solution sets (`-lcudd`, `cudd.h`) | **source** — https://davidkebo.com/cudd |
| **CaDiCaL** *(optional)* | `--backend cadical` (+ clause sharing, DRAT) | git submodule → `./configure && make` |
| **CryptoMiniSat** *(optional)* | `--backend cryptominisat` | git submodule → CMake build |
| googletest *(tests only)* | `dagster/tests/minimal/unit_tests` | `apt install libgtest-dev` |
| PySAT *(some test scripts)* | `tests/check.py` validation | `pip install python-sat` |

`stdc++fs`/`dl` come with the toolchain; nothing to install.

**Optional backends are truly optional.** If you skip steps 3 and/or 4, dagster
still builds — it just prints a `NOTE` and those `--backend` choices error with a
clear message at run time. The core tinisat + minisat backends always build.

## Verify the build

```bash
# the libs the optional backends need (present => backend will be compiled in)
ls -l dagster/cadical_solver/cadical/build/libcadical.a
ls -l dagster/cryptominisat_solver/cryptominisat/build/lib/libcryptominisat5.a

# CUDD visible to the loader (should print a libcudd line)
ldconfig -p | grep cudd

# dagster built, and which backends made it in
./dagster/dagster --help | grep -A1 backend
# fast end-to-end check across every wired-up backend + sls combo (~25s):
python3 dagster/tests/backend_matrix/matrix.py --smoke
```

## Gotchas (the usual cross-machine failures)

- **A backend is missing after `make`.** You ran `make` before building that
  solver's `.a`. Build the lib (step 3/4) **then** `make clean && make` in
  `dagster/` — toggling `-DHAVE_*` does not auto-recompile, so a plain `make`
  won't pick it up.
- **`cannot find -lcudd` at link, or `libcudd-*.so: cannot open shared object` at
  run.** CUDD wasn't installed to a standard prefix, or `ldconfig` wasn't run.
  Fix: `sudo make install && sudo ldconfig` in the CUDD tree. If you installed to
  a non-standard prefix (no root), add it explicitly:
  `make INCLUDES=-I<prefix>/include LDFLAGS='-L<prefix>/lib -lglog -lstdc++fs -lcudd -lz'`
  and at run time `export LD_LIBRARY_PATH=<prefix>/lib:$LD_LIBRARY_PATH`
  (the test/solve scripts already prepend `/usr/local/lib`).
- **CUDD `make` fails in autotools (aclocal/automake timestamp skew).** Run
  `autoreconf -fi` in the CUDD tree, then `./configure ... && make` again.
- **Submodule dirs are empty** (`cadical_solver/cadical` has no `configure`).
  You cloned without `--recursive`: run `git submodule update --init`.
- **Header/ABI change crashes mysteriously.** Editing a shared header (e.g.
  `Arguments.h`) should recompile dependents via `-MMD -MP`; when in doubt
  `make clean && make`.
- **macOS / ARM:** the Makefile adds `-fsigned-char` on arm64 automatically
  (Dagster's CNF parser assumes signed `char`).

## Docker

A `Dockerfile` is provided (see the README) that bakes these dependencies, if you
prefer a container to installing CUDD/MPI on the host.
