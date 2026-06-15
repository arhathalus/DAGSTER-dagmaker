# Builds CadicalSolver.o (the SatSolverInterface backend wrapping CaDiCaL).
# CaDiCaL itself must be built first under cadical/ (build/libcadical.a, src/cadical.hpp).
CXX      = mpic++
CXXFLAGS = -ggdb -O3 -std=c++17 -I.. -Icadical/src

all: CadicalSolver.o

CadicalSolver.o: CadicalSolver.cc CadicalSolver.h ../SatSolverInterface.h ../SlsChannel.h ../clause_share/ClauseChannel.h ../mpi_global.h cadical/src/cadical.hpp
	$(CXX) $(CXXFLAGS) -c CadicalSolver.cc -o CadicalSolver.o

clean:
	-rm -f *.o
