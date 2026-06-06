# Builds CryptominisatSolver.o (the SatSolverInterface backend wrapping CryptoMiniSat).
# CryptoMiniSat itself must be built first under cryptominisat/ via CMake:
#   cd cryptominisat && git checkout 5.11.21 && mkdir build && cd build
#   cmake -DCMAKE_BUILD_TYPE=Release -DBUILD_SHARED_LIBS=OFF -DENABLE_TESTING=OFF -DNOBREAKID=ON ..
#   make -j cryptominisat5
# -> build/lib/libcryptominisat5.a, public header src/cryptominisat.h
CXX      = mpic++
CXXFLAGS = -ggdb -O3 -std=c++17 -I.. -Icryptominisat/src

all: CryptominisatSolver.o

CryptominisatSolver.o: CryptominisatSolver.cc CryptominisatSolver.h ../SatSolverInterface.h cryptominisat/src/cryptominisat.h
	$(CXX) $(CXXFLAGS) -c CryptominisatSolver.cc -o CryptominisatSolver.o

clean:
	-rm -f *.o
