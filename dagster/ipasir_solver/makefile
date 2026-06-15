# Builds IpasirSolver.o -- a SatSolverInterface backend over any IPASIR solver,
# loaded at run time via dlopen (so no link dependency on a specific solver).
CXX      = mpic++
CXXFLAGS = -ggdb -O3 -std=c++17 -I..

all: IpasirSolver.o

IpasirSolver.o: IpasirSolver.cc IpasirSolver.h ../SatSolverInterface.h ../Cnf.h
	$(CXX) $(CXXFLAGS) -c IpasirSolver.cc -o IpasirSolver.o

clean:
	-rm -f *.o
