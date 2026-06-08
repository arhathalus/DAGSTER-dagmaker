/*************************
Copyright 2020 Mark Burgess

This file is part of Dagster.

Dagster is free software; you can redistribute it 
and/or modify it under the terms of the GNU General 
Public License as published by the Free Software 
Foundation; either version 2 of the License, or
(at your option) any later version.

Dagster is distributed in the hope that it will be
useful, but WITHOUT ANY WARRANTY; without even the
implied warranty of MERCHANTABILITY or FITNESS FOR 
A PARTICULAR PURPOSE. See the GNU General Public 
License for more details.

You should have received a copy of the GNU General 
Public License along with Dagster.
If not, see <http://www.gnu.org/licenses/>.
*************************/


#include <algorithm>
#include <glog/logging.h>
#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

using namespace std;

#include "Cnf.h"
#include "Dag.h"
#include "Arguments.h"
#include "gnovelty/gnovelty_main.hh"
#include "Master.h"
#include "Worker.h"
#include "MPICommsInterface.h"
#include "strengthener/StrengthenerInterface.h"
#include "clause_share/ClauseHub.h"
#include "CnfHolder.h"
#include <zlib.h>

#include "SolutionsInterface.h"
#include "TableSolutions.h"
#include "BDDSolutions.h"

#include "mpi_global.h"

// True global variables
int world_rank; // absolute MPI global world rank
int world_size; // absolute MPI global world size
Arguments command_line_arguments; // holder for parsed command line arguments
CnfHolder* cnf_holder; // the cnf_holder object for retrieving CNF components for a dag


// for a given message, output it to the results file
void process_solution(Dag* dag, Message* m) {
  FILE *fout;
  TEST_NOT_NULL(fout = fopen(command_line_arguments.output_filename, "a"))
  for (int i = 0; i < m->assignments.size(); i++)
    if (dag->reporting.find( abs(m->assignments[i])) )    // only print the variable if it is in reporting
      fprintf(fout, "%i ", m->assignments[i]);
  fprintf(fout, "\n");
  fclose(fout);
}



// if mode 0, we dont need to worry about any gnovelty or strengthener stuff
// and we can proceed with a tested vanilla TinySAT structure, where there is one master and the rest are tinisats.
// No SLS, no strengthener: one master + the rest CDCL workers, all using the
// given backend. Unifies the former modes 0 (tinisat), 4 (minisat), 5 (cadical)
// and 7 (cryptominisat), which were identical apart from the backend selector.
vector<Message*> simple_execute(WrappedSolutionsInterface *master_implementation, int backend) {
  MPI_Comm mastercommunicator; //  = MPI_COMM_WORLD;
  vector<Message*> solutions;
  // We are assuming at least 2 processes for this task
  if (world_size < 2) {
    LOG(ERROR) << "World size must be greater than 1";
    MPI_Abort(MPI_COMM_WORLD, 1);
  }
  MPI_Comm_split(MPI_COMM_WORLD, 0, world_rank, &mastercommunicator);
  MPICommsInterface* comms = new MPICommsInterface(&mastercommunicator);
  if (world_rank == 0) { // enter the master loop if rank zero
    auto master = Master(comms,master_implementation,command_line_arguments.ENUMERATE_SOLUTIONS,command_line_arguments.BREADTH_FIRST_NODE_ALLOCATIONS,true,command_line_arguments.checkpoint_frequency);
    solutions = master.loop(command_line_arguments.checkpoint_filename);
  } else { // enter the worker loop otherwise
    Worker* worker = new Worker(cnf_holder->dag, comms, NULL, NULL, backend);
    worker->loop();
    delete worker;
  }
  delete comms;
  return solutions;
}




// if mode 2, make partitions and subcommunicators to glue together gnovelties with HybridSAT solvers and a strengthener each
// need to do some index juggling to figure out which processes should be gnovelties and linked with tinisats and strengthener
vector<Message*> mode_2_execute(WrappedSolutionsInterface *master_implementation) {
  MPI_Comm subcommunicator_sls;
  MPI_Comm mastercommunicator;
  MPI_Comm subcommunicator_strengthener;
  vector<Message*> solutions;
  // check that we can even boot a master, and one worker, with its allocated novelties
  if (world_size < 3 + command_line_arguments.novelty_number) {
    LOG(ERROR) << "World size must be at least enough to support a master, worker, strengthener and associated gnovelties";
    MPI_Abort(MPI_COMM_WORLD, 1);
  }
  // enter the master loop if rank zero
  if (world_rank == 0) {
    MPI_Comm_split(MPI_COMM_WORLD, 0, 0, &mastercommunicator);
    MPI_Comm_split(MPI_COMM_WORLD, MPI_UNDEFINED, 0, &subcommunicator_sls);
    MPI_Comm_split(MPI_COMM_WORLD, MPI_UNDEFINED, 0, &subcommunicator_strengthener);
    //enter master loop
    MPICommsInterface* comms = new MPICommsInterface(&mastercommunicator);
    auto master = Master(comms,master_implementation,command_line_arguments.ENUMERATE_SOLUTIONS,command_line_arguments.BREADTH_FIRST_NODE_ALLOCATIONS,true,command_line_arguments.checkpoint_frequency);
    solutions = master.loop(command_line_arguments.checkpoint_filename);
    delete comms;
  } else {
    // we are a worker of some kind
    int bin_index = (world_rank - 1) / (2 + command_line_arguments.novelty_number);    // groups the processes into sizes of 1+novelty_number
    int bin_modulo = (world_rank - 1) % (2 + command_line_arguments.novelty_number);   // the rank of the processes in each of the groups
    if ((bin_index + 1) * (2 + command_line_arguments.novelty_number) >= world_size) { // if we have an underfull final group then merge it into the previous one, to create an overfull group
      bin_index--;
      bin_modulo += 2 + command_line_arguments.novelty_number;
    }
    // Splitting communicators
    if (bin_modulo == 0) { // Tinisat
      MPI_Comm_split(MPI_COMM_WORLD, 0, bin_index + 1, &mastercommunicator);
      MPI_Comm_split(MPI_COMM_WORLD, bin_index, bin_modulo, &subcommunicator_sls);
      MPI_Comm_split(MPI_COMM_WORLD, bin_index, bin_modulo, &subcommunicator_strengthener);
    } else if (bin_modulo == 1) { // Strengthener
      MPI_Comm_split(MPI_COMM_WORLD, MPI_UNDEFINED, 0, &mastercommunicator);
      MPI_Comm_split(MPI_COMM_WORLD, MPI_UNDEFINED, 0, &subcommunicator_sls);
      MPI_Comm_split(MPI_COMM_WORLD, bin_index, bin_modulo, &subcommunicator_strengthener);
    } else { // SLS
      MPI_Comm_split(MPI_COMM_WORLD, MPI_UNDEFINED, 0, &mastercommunicator);
      MPI_Comm_split(MPI_COMM_WORLD, bin_index, bin_modulo, &subcommunicator_sls);
      MPI_Comm_split(MPI_COMM_WORLD, MPI_UNDEFINED, 0, &subcommunicator_strengthener);
    }
    // Loading up exectuables
    if (bin_modulo == 0) { // we are a specific HybridSatSolver
      MPICommsInterface* comms = new MPICommsInterface(&mastercommunicator);
      Worker* worker = new Worker(cnf_holder->dag, comms, &subcommunicator_sls, &subcommunicator_strengthener);
      worker->loop(); // enter the worker loop
      delete comms;
      delete worker;
    } else if (bin_modulo == 1) { // we are a strengthener instance
      strengthener_surrogate_main(&subcommunicator_strengthener, cnf_holder);
    } else if (command_line_arguments.novelty_number > 0) { // we are a gnovelty instance
      int subcommunicator_sls_world_rank;
      MPI_Comm_rank(subcommunicator_sls, &subcommunicator_sls_world_rank);
      // dump the process into gnovelty_main with the appropriate subcommunicator_sls, and hope everything works >_<
      gnovelty_main(&subcommunicator_sls, command_line_arguments.suggestion_size, command_line_arguments.advise_scheme, command_line_arguments.dynamic_local_search);
    } else {
      VLOG(2) << "process " << world_rank << " is left over and will not contribute to SAT solving" << std::endl;
    }
  }
  return solutions;
}


// if mode 3, make partitions and subcommunicators to glue together each SAT solvers and a strengthener each
vector<Message*> mode_3_execute(WrappedSolutionsInterface *master_implementation) {
  MPI_Comm mastercommunicator;
  MPI_Comm subcommunicator_strengthener;
  vector<Message*> solutions;
  // check that we can even boot a master, and one worker, with its allocated novelties
  if ((world_size >=3) && ((world_size-1)%2 ==0)) {} else {
    LOG(ERROR) << "World size must be at least enough to support a master, and worker + strengthener pairs";
    MPI_Abort(MPI_COMM_WORLD, 1);
  }
  // enter the master loop if rank zero
  if (world_rank == 0) {
    MPI_Comm_split(MPI_COMM_WORLD, 0, 0, &mastercommunicator);
    MPI_Comm_split(MPI_COMM_WORLD, MPI_UNDEFINED, 0, &subcommunicator_strengthener);
    //enter master loop
    MPICommsInterface* comms = new MPICommsInterface(&mastercommunicator);
    auto master = Master(comms,master_implementation,command_line_arguments.ENUMERATE_SOLUTIONS,command_line_arguments.BREADTH_FIRST_NODE_ALLOCATIONS,true,command_line_arguments.checkpoint_frequency);
    solutions = master.loop(command_line_arguments.checkpoint_filename);
    delete comms;
  } else {
    // we are a worker of some kind
    int bin_index = (world_rank - 1) / 2;    // groups the processes into sizes of 1+novelty_number
    int bin_modulo = (world_rank - 1) % 2;   // the rank of the processes in each of the groups
    // Splitting communicators
    if (bin_modulo == 0) { // Tinisat
      MPI_Comm_split(MPI_COMM_WORLD, 0, bin_index + 1, &mastercommunicator);
      MPI_Comm_split(MPI_COMM_WORLD, bin_index, bin_modulo, &subcommunicator_strengthener);
      MPICommsInterface* comms = new MPICommsInterface(&mastercommunicator);
      Worker* worker = new Worker(cnf_holder->dag, comms, NULL, &subcommunicator_strengthener);
      worker->loop(); // enter the worker loop
      delete comms;
      delete worker;
    } else { // Strengthener
      MPI_Comm_split(MPI_COMM_WORLD, MPI_UNDEFINED, 0, &mastercommunicator);
      MPI_Comm_split(MPI_COMM_WORLD, bin_index, bin_modulo, &subcommunicator_strengthener);
      strengthener_surrogate_main(&subcommunicator_strengthener, cnf_holder);
    }
  }
  return solutions;
}






// SLS-guided execution: a CDCL worker + gnovelty SLS helpers per hybrid group,
// parameterised by the CDCL backend (tinisat / minisat / cadical / cryptominisat).
// The gnovelty helper topology and SLS communicator are independent of the
// backend; only the Worker's backend changes, routing the helper exchange
// through that backend's SlsChannel (SatSolver's own SLS path for tinisat).
// Covers the former modes 1 (tinisat), 6 (cadical), 8 (minisat), 9 (cryptominisat).
vector<Message*> sls_execute(WrappedSolutionsInterface *master_implementation, int backend) {
  MPI_Comm subcommunicator;
  MPI_Comm mastercommunicator;
  vector<Message*> solutions;
  if (world_size < 2 + command_line_arguments.novelty_number) {
    LOG(ERROR) << "World size must be at least enough to support a master, worker and associated gnovelties";
    MPI_Abort(MPI_COMM_WORLD, 1);
  }
  if (world_rank == 0) {
    MPI_Comm_split(MPI_COMM_WORLD, MPI_UNDEFINED, 0, &subcommunicator);
    MPI_Comm_split(MPI_COMM_WORLD, 0, 0, &mastercommunicator);
    MPICommsInterface* comms = new MPICommsInterface(&mastercommunicator);
    auto master = Master(comms,master_implementation,command_line_arguments.ENUMERATE_SOLUTIONS,command_line_arguments.BREADTH_FIRST_NODE_ALLOCATIONS,true,command_line_arguments.checkpoint_frequency);
    solutions = master.loop(command_line_arguments.checkpoint_filename);
    delete comms;
  } else {
    int num_procs_per_hybrid_group = 1 + command_line_arguments.novelty_number;
    int hybrid_group_num = (world_rank - 1) / num_procs_per_hybrid_group;
    int index_in_hybrid_group = (world_rank - 1) % num_procs_per_hybrid_group;
    if ((hybrid_group_num + 1) * num_procs_per_hybrid_group >= world_size) {
      hybrid_group_num--;
      index_in_hybrid_group += 1 + (command_line_arguments.novelty_number);
    }
    MPI_Comm_split(MPI_COMM_WORLD, hybrid_group_num, index_in_hybrid_group, &subcommunicator);
    if (index_in_hybrid_group == 0) {
      MPI_Comm_split(MPI_COMM_WORLD, 0, hybrid_group_num + 1, &mastercommunicator);
      MPICommsInterface* comms = new MPICommsInterface(&mastercommunicator);
      Worker* worker = new Worker(cnf_holder->dag, comms, &subcommunicator, NULL, backend);
      worker->loop();
      delete comms;
      delete worker;
    } else {
      MPI_Comm_split(MPI_COMM_WORLD, MPI_UNDEFINED, 0, &mastercommunicator);
      gnovelty_main(&subcommunicator, command_line_arguments.suggestion_size, command_line_arguments.advise_scheme, command_line_arguments.dynamic_local_search);
    }
  }
  return solutions;
}


// Clause-sharing topology (cube-and-conquer, CaDiCaL): one master, N conquer
// workers, and one clause hub. Two communicators are carved from MPI_COMM_WORLD:
//   - mastercommunicator : master (rank 0) + workers              (assignments/cubes)
//   - clausecommunicator : workers + hub (hub last)               (learned-clause relay)
// The hub is the final world rank; the master is excluded from the clause comm
// and the hub from the master comm (mirrors how SLS helpers sit only in the SLS
// comm). All ranks must call both splits.
vector<Message*> share_execute(WrappedSolutionsInterface *master_implementation, int backend) {
  MPI_Comm mastercommunicator, clausecommunicator;
  vector<Message*> solutions;
  if (world_size < 3) {
    LOG(ERROR) << "Clause sharing needs at least 3 ranks (master + worker + hub)";
    MPI_Abort(MPI_COMM_WORLD, 1);
  }
  bool is_master = (world_rank == 0);
  bool is_hub    = (world_rank == world_size - 1);
  // workers + master form the master comm (hub excluded); workers + hub form the
  // clause comm (master excluded). key=world_rank keeps workers ahead of the hub
  // so the hub lands at the last rank of the clause comm.
  MPI_Comm_split(MPI_COMM_WORLD, is_hub    ? MPI_UNDEFINED : 0, world_rank, &mastercommunicator);
  MPI_Comm_split(MPI_COMM_WORLD, is_master ? MPI_UNDEFINED : 1, world_rank, &clausecommunicator);

  if (is_master) {
    MPICommsInterface* comms = new MPICommsInterface(&mastercommunicator);
    auto master = Master(comms,master_implementation,command_line_arguments.ENUMERATE_SOLUTIONS,command_line_arguments.BREADTH_FIRST_NODE_ALLOCATIONS,true,command_line_arguments.checkpoint_frequency);
    solutions = master.loop(command_line_arguments.checkpoint_filename);
    delete comms;
  } else if (is_hub) {
    clause_hub_main(&clausecommunicator);
  } else {
    MPICommsInterface* comms = new MPICommsInterface(&mastercommunicator);
    Worker* worker = new Worker(cnf_holder->dag, comms, NULL, NULL, backend, &clausecommunicator);
    worker->loop();
    delete comms;
    delete worker;   // worker dtor sends the hub its teardown signal
  }
  return solutions;
}


int main(int argc, char **argv) {
  // initialise google logging and load command line arguments
  google::InitGoogleLogging(argv[0]);
  google::InstallFailureSignalHandler();
  command_line_arguments.load(argc, argv);

  //initialise all the MPI stuff, find out MPI world rank and size
  MPI_Init(NULL, NULL);
  MPI_Comm_rank(MPI_COMM_WORLD, &world_rank);
  MPI_Comm_size(MPI_COMM_WORLD, &world_size);

  // load the dag, and do all required CNF splitting
  Dag* dag = new Dag(command_line_arguments.dag_filename);
  cnf_holder = new CnfHolder(dag, command_line_arguments.cnf_directory, command_line_arguments.cnf_filename, 2);
  if ((command_line_arguments.cnf_directory == NULL) || (world_rank == 0))
    cnf_holder->generate_decomposition();
  else
    cnf_holder->generate_pseudo_decomposition();
  
  // initialise the respective master solution object, and populate with seed messages
  WrappedSolutionsInterface *master_implementation = NULL;
  if (world_rank == 0) {
    if (command_line_arguments.master_sub_mode==0) {
      master_implementation = new TableSolutions(dag,false);
    } else if (command_line_arguments.master_sub_mode==1) {
      master_implementation = new BDDSolutions(dag,cnf_holder->max_vc);
      if ("" != command_line_arguments.BDD_compilation_scheme) {
        VLOG(4) <<__LINE__<<__PRETTY_FUNCTION__<< "MASTER: setting compilation scheme to: "<<command_line_arguments.BDD_compilation_scheme<<".\n";
        ((BDDSolutions*)master_implementation)->set__BDD_compilation_scheme(command_line_arguments.BDD_compilation_scheme);
      } else {
        VLOG(4) <<__LINE__<<__PRETTY_FUNCTION__<<"MASTER: no BDD compilation set on command line.\n";
      }
    } else {
      VLOG(2) << "WARNING: using TableMaster in dumb mode... I hope you know what you are doing.";
      master_implementation = new TableSolutions(dag,true);
    }
    if (command_line_arguments.cubes_filename != NULL) {
      // CUBE-AND-CONQUER: seed the conquer node (node 0 = the whole formula) with
      // one message per march cube. Each becomes a "solve node 0 under this cube"
      // work item (node 0 is a root, so its self-loop edge messages[0][0] are the
      // per-cube jobs the master distributes to workers).
      std::ifstream cf(command_line_arguments.cubes_filename);
      if (!cf)
        throw BadParameterException("cube-and-conquer: cannot open cubes file");
      std::string line;
      int ncubes = 0;
      while (std::getline(cf, line)) {
        if (line.empty() || line[0] != 'a')   // march cube lines start with 'a'
          continue;
        std::istringstream iss(line.substr(1));
        Message *m = new Message(0, 0);
        int lit;
        while (iss >> lit) {
          if (lit == 0) break;
          m->assignments.push_back(lit);
        }
        master_implementation->add_message(m);
        delete m;
        ncubes++;
      }
      VLOG(0) << "MASTER: cube-and-conquer -- seeded " << ncubes
              << " cubes into the conquer node (node 0)";
      if (ncubes == 0)
        throw BadParameterException("cube-and-conquer: no cubes ('a ... 0' lines) in file");
    } else {
      // seed dag with ininital empty messages for each node.
      for (int i = 0; i < dag->no_nodes; i++) {
        if (dag->node_status[i] == 1) {
          Message *m = new Message(i, i);
          master_implementation->add_message(m);
          delete m;
        }
      }
    }
    //clear dag_out file
    FILE *fout;
    TEST_NOT_NULL(fout = fopen(command_line_arguments.output_filename, "w"))
    fclose(fout);

  }

  // Resolve the run configuration into (backend, sls, strengthen). The orthogonal
  // --backend/--sls/--strengthen flags are preferred; if none is supplied we fall
  // back to the legacy numeric -m selector (kept for backward compatibility).
  int backend = BACKEND_TINISAT;
  bool sls = false, strengthen = false, share = false;
  bool flags_used = (!command_line_arguments.backend.empty())
                    || (command_line_arguments.use_sls != -1)
                    || (command_line_arguments.use_strengthen != -1)
                    || (command_line_arguments.use_share != -1);
  if (flags_used) {
    const std::string& b = command_line_arguments.backend;
    if (b.empty() || b == "tinisat")            backend = BACKEND_TINISAT;
    else if (b == "minisat")                    backend = BACKEND_MINISAT;
    else if (b == "cadical")                    backend = BACKEND_CADICAL;
    else if (b == "cryptominisat" || b == "cms") backend = BACKEND_CRYPTOMINISAT;
    else if (b == "ipasir")                     backend = BACKEND_IPASIR;
    else if (b == "glucose") {                  // convenience: ipasir + vendored Glucose .so
      backend = BACKEND_IPASIR;
      if (command_line_arguments.ipasir_lib.empty())
        command_line_arguments.ipasir_lib = "ipasir_solver/libipasirglucose.so";
    }
    else if (b == "lingeling") {                // convenience: ipasir + built Lingeling .so
      backend = BACKEND_IPASIR;
      if (command_line_arguments.ipasir_lib.empty())
        command_line_arguments.ipasir_lib = "ipasir_solver/libipasirlingeling.so";
    }
    else throw BadParameterException("unknown --backend (use tinisat|minisat|cadical|cryptominisat|glucose|ipasir)");
    sls = (command_line_arguments.use_sls == 1);
    strengthen = (command_line_arguments.use_strengthen == 1);
    share = (command_line_arguments.use_share == 1);
  } else {
    switch (command_line_arguments.mode) {       // legacy -m mapping
      case 0: backend = BACKEND_TINISAT; break;
      case 1: backend = BACKEND_TINISAT; sls = true; break;
      case 2: backend = BACKEND_TINISAT; sls = true; strengthen = true; break;
      case 3: backend = BACKEND_TINISAT; strengthen = true; break;
      case 4: backend = BACKEND_MINISAT; break;
      case 5: backend = BACKEND_CADICAL; break;
      case 6: backend = BACKEND_CADICAL; sls = true; break;
      case 7: backend = BACKEND_CRYPTOMINISAT; break;
      case 8: backend = BACKEND_MINISAT; sls = true; break;
      case 9: backend = BACKEND_CRYPTOMINISAT; sls = true; break;
      case 10: backend = BACKEND_CADICAL; share = true; break;  // cube-and-conquer + clause hub
      default: throw BadParameterException("Dagster called with non existant mode");
    }
  }
  // The clause-strengthening reducer is currently wired for the tinisat backend only.
  if (strengthen && backend != BACKEND_TINISAT)
    throw BadParameterException("--strengthen is currently only supported with the tinisat backend");
  // The IPASIR backend needs a shared library to dlopen (any IPASIR solver).
  if (backend == BACKEND_IPASIR && command_line_arguments.ipasir_lib.empty())
    throw BadParameterException("--backend ipasir requires --ipasir-lib <libipasirSOLVER.so>");
  // SLS helpers attach via the solver ctor; the IPASIR adapter has no SLS variant.
  if (backend == BACKEND_IPASIR && sls)
    throw BadParameterException("--backend ipasir does not support --sls");
  // Clause sharing (phase 1) is CaDiCaL-only and does not yet compose with the
  // SLS helpers or the strengthener (each owns the worker's helper communicator).
  if (share) {
    if (backend != BACKEND_CADICAL)
      throw BadParameterException("--share is currently only supported with the cadical backend");
    if (sls || strengthen)
      throw BadParameterException("--share does not yet compose with --sls or --strengthen");
    if (command_line_arguments.clause_share_max_size < 3)
      throw BadParameterException("--share-max-size must be >= 3 (MpiBuffer transport minimum)");
  }
  // DRAT proof emission (milestone 0): CaDiCaL-only, and incompatible with the
  // speed levers -- enumeration adds non-entailed blocking clauses, sharing/SLS
  // break a worker's self-contained proof. See utilities/cube/PROOF_SCOPE.md.
  if (command_line_arguments.proof_filename != NULL) {
    if (backend != BACKEND_CADICAL)
      throw BadParameterException("--proof is currently only supported with the cadical backend");
    if (share || sls)
      throw BadParameterException("--proof does not compose with --share or --sls (proof needs self-contained per-worker solves)");
  }

  // enter into the respective execution path
  vector<Message*> solutions;
  if (share) {
    solutions = share_execute(master_implementation, backend);  // cadical + clause hub
  } else if (strengthen && sls) {
    solutions = mode_2_execute(master_implementation);          // tinisat + SLS + strengthener
  } else if (strengthen) {
    solutions = mode_3_execute(master_implementation);          // tinisat + strengthener
  } else if (sls) {
    solutions = sls_execute(master_implementation, backend);    // any backend + SLS helpers
  } else {
    solutions = simple_execute(master_implementation, backend); // any backend, no helpers
  }
  
  // dump found solutions to output file
  for (auto it = solutions.begin(); it != solutions.end(); it++) {
    VLOG(1) << "SOLUTION: " << **it;
    process_solution(dag, *it);
  }
  // print message counts
  if (world_rank == 0) {
    if (VLOG_IS_ON(1)) {
      VLOG(0) << "MASTER: number of solutions at each node:";
      for (int i = 0; i < dag->no_nodes; i++)
        VLOG(0) << "node " << i << ": incoming " << master_implementation->get_incoming_message_count(i) << " outgoing " << master_implementation->get_outgoing_message_count(i);
      VLOG(0) << "MASTER: number of nodes in each bdd:";
      master_implementation->print_stats(false, true);
    }
    delete master_implementation;
  }
  // print exit message and cleanup
  VLOG(2) << "process " << world_rank << " exiting";
  
  delete dag;
  MPI_Finalize();
}

