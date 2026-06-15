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


# ---- optional vendored backends -------------------------------------------
# CaDiCaL and CryptoMiniSat are compiled in ONLY if their built static library is
# present. Without it, dagster still builds -- those backends are simply
# unavailable and selecting one errors with a clear message. To enable a backend,
# build its vendored solver (so the .a below exists) and then `make clean && make`
# (a clean rebuild is needed because toggling -DHAVE_* doesn't auto-recompile).
CADICAL_LIB := cadical_solver/cadical/build/libcadical.a
CMS_LIB     := cryptominisat_solver/cryptominisat/build/lib/libcryptominisat5.a

OPT_DEFS    :=
OPT_SUBDIRS :=
OPT_OBJS    :=

ifneq ($(wildcard $(CADICAL_LIB)),)
  OPT_DEFS    += -DHAVE_CADICAL
  OPT_SUBDIRS += cadical_solver
  OPT_OBJS    += cadical_solver/*.o* $(CADICAL_LIB)
else
  $(info NOTE: $(CADICAL_LIB) not found -- building WITHOUT the cadical backend)
endif

ifneq ($(wildcard $(CMS_LIB)),)
  OPT_DEFS    += -DHAVE_CRYPTOMINISAT
  OPT_SUBDIRS += cryptominisat_solver
  OPT_OBJS    += cryptominisat_solver/*.o* $(CMS_LIB)
else
  $(info NOTE: $(CMS_LIB) not found -- building WITHOUT the cryptominisat backend)
endif

# make every translation unit see which optional backends are compiled in
CXXFLAGS += $(OPT_DEFS)

OBJS 				= Cnf.o CnfManager.o SatSolver.o Dag.o Message.o TableSolutions.o BDDSolutions.o main.o Master.o Worker.o MPICommsInterface.o utilities.o Arguments.o CnfHolder.o MasterOrganiser.o SatSolverInterface.o SlsChannel.o

.PHONY: gnovelty

dagster: $(OBJS) gnovelty/*.cc gnovelty/*.hh strengthener/*.cc strengthener/*.h minisat_solver/*.cc minisat_solver/*.h clause_share/*.cc clause_share/*.h ipasir_solver/*.cc ipasir_solver/*.h $(wildcard cadical_solver/*.cc cadical_solver/*.h cryptominisat_solver/*.cc cryptominisat_solver/*.h)
	$(MAKE) -C strengthener
	$(MAKE) -C gnovelty
	$(MAKE) -C minisat_solver
	$(MAKE) -C clause_share
	$(MAKE) -C ipasir_solver
	for d in $(OPT_SUBDIRS); do $(MAKE) -C $$d || exit 1; done
	$(MPICXX) $(CPPFLAGS) $(CXXFLAGS) ./*.o gnovelty/*.o strengthener/*.o* minisat/*/*.o* minisat_solver/*.o* clause_share/*.o* ipasir_solver/*.o* $(OPT_OBJS) $(LDFLAGS) -ldl -o dagster

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
	-$(MAKE) -C cadical_solver clean        # optional backend: ignore if absent
	-$(MAKE) -C cryptominisat_solver clean  # optional backend: ignore if absent
	$(MAKE) -C clause_share clean
	$(MAKE) -C ipasir_solver clean
	-rm dagster *.o *.d
	-rm ./dag_out.txt

# pull in auto-generated header dependencies (from -MMD), if present. MUST be at
# the end: an -include of a .d file before the `dagster` rule would make the
# first .d's object the default goal (so plain `make` builds one .o and stops).
-include $(OBJS:.o=.d)
