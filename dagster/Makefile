UNAME                           = $(shell uname -a)
CRAY_PLATFORM_NAME              = nakuru

ifeq (${CRAY_PLATFORM_NAME}, $(findstring ${CRAY_PLATFORM_NAME}, ${UNAME}))
        #Variables for Nakuru Cray system
        MPICXX                  = CC
        CXXFLAGS                += -O3 -std=c++17 -DVERSION=1.0.0 -DNO_PROVENANCE 
        INCLUDES                = -I/sw/UNCLASSIFIED/glog-master/include -I/sw/UNCLASSIFIED/cudd-release/include \
                                  -I/sw/UNCLASSIFIED/zlib-1.2.11/include
        LDFLAGS                 = -L/sw/UNCLASSIFIED/glog-master/lib -L/sw/UNCLASSIFIED/cudd-release/lib \
                                  -L/sw/UNCLASSIFIED/zlib-1.2.11/lib -lglog -lstdc++fs -lcudd -lz 
else
	#Default variables
	MPICXX                  = mpic++
	# -MMD -MP emits per-object .d header-dependency files (see -include below) so
	# that editing a shared header (e.g. Arguments.h) recompiles every dependent
	# .o -- without this, stale objects read changed structs at old offsets (ABI
	# mismatch) and crash mysteriously.
	CXXFLAGS                += -ggdb -O3 -std=c++17 -I. -Iminisat -MMD -MP
	LDFLAGS                 += -lglog -lstdc++fs -lcudd -lz

        # macOS workaround for char/EOF parser behavior
	#ifeq ($(shell uname -s),Darwin) ## first idea
	#CHAR_IS_UNSIGNED := $(shell printf '' | $(MPICXX) -dM -E -x c++ - 2>/dev/null | grep -c "__CHAR_UNSIGNED__") ## Copilot suggestion
	#ifneq ($(CHAR_IS_UNSIGNED),0)
	ARCH := $(shell uname -m)
	# strong ARM the compiler
	ifneq (,$(filter $(ARCH),arm64 aarch64))
		CXXFLAGS += -fsigned-char -DDAGSTER_MAC_CHAR_WORKAROUND=1
	endif
endif
#Profiling
#       LDFLAGS                 += -L/usr/local/lib/openmpi -lmpiP -lm -lbfd -liberty -lunwind -L.. -ldummy_mpi1


OBJS 				= Cnf.o CnfManager.o SatSolver.o Dag.o Message.o TableSolutions.o BDDSolutions.o main.o Master.o Worker.o MPICommsInterface.o utilities.o Arguments.o CnfHolder.o MasterOrganiser.o SatSolverInterface.o SlsChannel.o

.PHONY: gnovelty

dagster: $(OBJS) gnovelty/*.cc gnovelty/*.hh strengthener/*.cc strengthener/*.h minisat_solver/*.cc minisat_solver/*.h cadical_solver/*.cc cadical_solver/*.h cryptominisat_solver/*.cc cryptominisat_solver/*.h
	$(MAKE) -C strengthener
	$(MAKE) -C gnovelty
	$(MAKE) -C minisat_solver
	$(MAKE) -C cadical_solver
	$(MAKE) -C cryptominisat_solver
	$(MPICXX) $(CPPFLAGS) $(CXXFLAGS) ./*.o gnovelty/*.o strengthener/*.o* minisat/*/*.o* minisat_solver/*.o* cadical_solver/*.o* cadical_solver/cadical/build/libcadical.a cryptominisat_solver/*.o* cryptominisat_solver/cryptominisat/build/lib/libcryptominisat5.a $(LDFLAGS) -o dagster

%.o: %.cpp %.h
	$(MPICXX) $(CPPFLAGS) $(CXXFLAGS) $(INCLUDES) -c $< -o $@

%.o: %.cpp
	$(MPICXX) $(CPPFLAGS) $(CXXFLAGS) $(INCLUDES) -c $< -o $@

%.o: %.cc %.h
	$(MPICXX) $(CPPFLAGS) $(CXXFLAGS) $(INCLUDES) -c $< -o $@

%.o: %.cc
	$(MPICXX) $(CPPFLAGS) $(CXXFLAGS) $(INCLUDES) -c $< -o $@

clean:
	$(MAKE) -C strengthener clean
	$(MAKE) -C gnovelty clean
	$(MAKE) -C minisat_solver clean
	$(MAKE) -C cadical_solver clean
	$(MAKE) -C cryptominisat_solver clean
	-rm dagster *.o *.d
	-rm ./dag_out.txt

# pull in auto-generated header dependencies (from -MMD), if present. MUST be at
# the end: an -include of a .d file before the `dagster` rule would make the
# first .d's object the default goal (so plain `make` builds one .o and stops).
-include $(OBJS:.o=.d)
