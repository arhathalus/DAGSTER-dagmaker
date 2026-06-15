# Builds the clause-sharing module: ClauseChannel (worker endpoint) and
# ClauseHub (the relay process). Both wrap ../strengthener/MpiBuffer.
CXX      = mpic++
CXXFLAGS = -ggdb -O3 -std=c++17 -I..

all: ClauseChannel.o ClauseHub.o

ClauseChannel.o: ClauseChannel.cc ClauseChannel.h ../mpi_global.h ../strengthener/MpiBuffer.h
	$(CXX) $(CXXFLAGS) -c ClauseChannel.cc -o ClauseChannel.o

ClauseHub.o: ClauseHub.cc ClauseHub.h ../mpi_global.h ../strengthener/MpiBuffer.h
	$(CXX) $(CXXFLAGS) -c ClauseHub.cc -o ClauseHub.o

clean:
	-rm -f *.o
