/*************************
Copyright 2020 Mark Burgess, Marhsall Cliffton, Josh Milthorpe

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


#include "SatSolver.h"
#include <algorithm>
#include <math.h>
#include <stdio.h>
#include <stdlib.h>

#include "mpi_global.h"
#include "SlsChannel.h"
#include <algorithm>
#include <functional>
#include <glog/logging.h>
#include <mpi.h>
#include <ostream>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include "utilities.h"
#include "exceptions.h"
#include "Arguments.h"
#include "CnfHolder.h"

extern Arguments command_line_arguments;
extern CnfHolder* cnf_holder;

#define HALFLIFE 128 // the halflife number of cycles of the variable scores used in VSIDS variable selection rule.
#define _DT 32 // RSAT phase selection threshold


// function used for sorting variables in order of their score 
struct compScores : public binary_function<unsigned, unsigned, bool> {
  Variable *vars;
  compScores(Variable *myVars) : vars(myVars) {}
  bool operator()(unsigned a, unsigned b) const {
    return SCORE(a) > SCORE(b);
  }
};

// selects a decision literal through a cascade of mechanisms
int SatSolver::selectLiteral() {
  int x = 0;
  if (short_stopping)
    if (verifySolution()) // if CNF is already satisfied return 0
      return 0;
//#ifndef GEOMETRIC_RESTARTING_SCHEME
if (command_line_arguments.opportunity_modulo!=0) {
  if (heuristic_rotation == suggestion_first) // The first thing to try is to select a variable using an SLS process. if suggestion_first flag set
    x = get_suggestion();
}
//#endif
  if (x == 0)
    x = selectLiteral__conflict(); // choose a variable from a conflict clause
  if ((x == 0) && (heuristic_rotation == cdcl_first))
    x = get_suggestion();
  if (x == 0) // USE VSIDS
    x = selectLiteral__vsids();
  return x;
}

// pick best var in unsatisfied conflict clause nearest to top of stack
// but only search 256 clauses
int SatSolver::selectLiteral__conflict() {
  int x = 0;
  int lastClause = nextClause > 256 ? (nextClause - 256) : 0;
  for (int i = nextClause; i >= lastClause; i--) {
    int *p = clauses[nextClause = i];
    // skip satisfied clauses
    bool sat = false;
    for (; (*p); p++)
      if (SET(*p)) {
        sat = true;
        break;
      }
    if (sat)
      continue;
    // traverse again, find best variable of clause
    int score = -1;
    for (p = clauses[i]; (*p); p++)
      if (FREE(*p) && ((int)SCORE(VAR(*p))) > score) {
        x = VAR(*p);
        score = SCORE(x);
      }
    // RSAT phase selection
    int d = vars[x].activity[_POSI] - vars[x].activity[_NEGA];
    if (d > _DT)
      return x;
    else if (-d > _DT)
      return -x;
    else
      return (vars[x].phase == _POSI) ? (x) : -(int)(x);
  }
  return x;
}

// select decision litteral from VSIDS heuristic
int SatSolver::selectLiteral__vsids() {
  int x = 0;
  for (unsigned i = nextVar; i < nVars; i++) {
    if (vars[varOrder[i]].value == _FREE) {
      x = varOrder[i];
      nextVar = i + 1;
      // RSAT phase selection
      int d = vars[x].activity[_POSI] - vars[x].activity[_NEGA];
      if (d > _DT)
        return x;
      else if (-d > _DT)
        return -x;
      else
        return (vars[x].phase == _POSI) ? (x) : -(int)(x);
    }
  }
  return 0;
}


// int run()
//   The primary loop of the CDCL (SAT solving) process, returning 0 if contradiction detected, 1 if SAT, or 2 if paused
int SatSolver::run(Message* m) {
  if (solver_unit_contradiction == true) // if unit clause conflict detected return immediate UNSAT
    return false;
  if (dLevel == 0)
    return false; // assertUnitClauses() has failed
  int solution = sls__get_solutions(); // check all gnovelties if they have solution to report
  if (solution) {
    if ((command_line_arguments.solution_trimming==1) && (!verify_and_trim_Solution()))
      throw ConsistencyException("Gnovelty somehow generated bad SAT result");
    return solution;
  }
  int literals_selected = 0;
  for (int lit; (lit = selectLiteral());) { // pick decision literal
    literals_selected++;
    if (literals_selected == command_line_arguments.sat_reporting_time) {
      return 2; // return back up the stack to check whether master has reassignment every sat_reporting_time literals decided.
    }
    if ((literals_selected % command_line_arguments.gnovelty_solution_checking_time) == 0) {
      int solution = sls__get_solutions(); // check all gnovelties if they have solution to report
      if (solution) {
        if ((command_line_arguments.solution_trimming==1) && (!verify_and_trim_Solution()))
          throw ConsistencyException("Gnovelty somehow generated bad SAT result");
        return solution;
      }
    }
    bool decision = decide(lit);
    this->decisions++;
    if (!decision)
      do { // decision/conflict
        // conflict has occurred in dLevel 1, unsat
        if (aLevel == 0)
          return false;
        // score decay
        if (nConflicts == nextDecay) {
          nextDecay += HALFLIFE;
          scoreDecay();
        }
        // rewind to top of clause stack
        nextClause = clauses.size() - 1;
        // restart at dLevel 1
        bool backtrack = false;
if (command_line_arguments.opportunity_modulo!=0) {
	opportunity_counter++;
	if (! (opportunity_counter % command_line_arguments.opportunity_modulo) ){ // CONSIDER RESTART
	  
	  if ( (rand() / (RAND_MAX + 1.)) >= probability_of_not_restarting ){
	    backtrack = true;
	    //for (auto ppp=0; ppp< 5 ; ppp++)std::cerr<<"*********************RESET   ";
	  }
	  probability_of_not_restarting *= command_line_arguments.discount_factor;
	}
	
} else {
        if ( (command_line_arguments.tinisat_restarting==1)
	     && (nConflicts == nextRestart)) {
          nextRestart += luby.next() * lubyUnit;
          backtrack = true;
        } else if (command_line_arguments.tinisat_restarting==2) {
          backtrack = true;
        }
}
        if (backtrack == true) {
          backtrack_func(1);
          if (dLevel != aLevel)
            break;
        } else { // partial restart at aLevel
          backtrack_func(aLevel);
        }
      } while (!assertCL()); // assert conflict literal
  }
  if ((command_line_arguments.solution_trimming==1) && (!verify_and_trim_Solution()))
    throw ConsistencyException("SatSolver somehow generated bad SAT result");
  if (command_line_arguments.solution_trimming==2)
  	purge_negative_literals();
  return true;
}

// removes all negative literals from the solution (WARNING: possibly UNSAFE configuraiton)
void SatSolver::purge_negative_literals() {
  for (int i=1; i<=vc; i++)
    if (vars[i].value == _NEGA)
      vars[i].value = _FREE;
}

// returning TRUE/FALSE, if current assignment actually satisfies whole CNF
bool SatSolver::verifySolution() {
  for (int i=0; i<cnf->cc; i++) {
    bool satisfied = false;
    for (int j=0; j<cnf->cl[i]; j++) {
      int lit = cnf->clauses[i][j];
      if (SET(lit)) {
        satisfied = true;
        break;
      }
    }
    if (!satisfied)
      return false;
  }
  return true;
}

// returning TRUE/FALSE, if current assignment actually satisfies whole CNF, and trims the litterals to only thoes satisfying the CNF
bool SatSolver::verify_and_trim_Solution() {
//#ifdef GEOMETRIC_RESTARTING_SCHEME
  probability_of_not_restarting = 1.0; // reset probability if a solution is found...
//#endif
  // initially mark all the variables false
  for (int i=1; i<=vc; i++)
    vars[i].mark2 = false;
  // scan through original CNF clauses, marking variables that satisfy clauses true
  for (int i=0; i<cnf->cc; i++) {
    bool satisfied = false;
    int min_satisfying_var = -1;
    int min_satisfying_lit = -1;
    for (int j=0; j<cnf->cl[i]; j++) {
      int lit = cnf->clauses[i][j];
      if (SET(lit)) {
        satisfied = true;
        int varlit = VAR(lit);
        if (vars[varlit].mark2 == true) {
          min_satisfying_var = varlit;
          min_satisfying_lit = lit;
          break;
        }
        if ((min_satisfying_var==-1)||(
        	((lit>0) && (min_satisfying_lit<0)) || // priority towards positive literals
        	(varlit<min_satisfying_var)
        	))
          min_satisfying_var = varlit;
      }
    }
    if (!satisfied) // if a clause is unsatisfied return false
      return false;
    vars[min_satisfying_var].mark2 = true;
  }
  // scan through clauses that are the negation of added solution conflict clauses, marking variables which satisfy thoes clauses true
  for (int i=0; i<solution_conflict_indices.size(); i++) {
    int clause_index = solution_conflict_indices[i];
    bool satisfied = false;
    int min_satisfying_var = -1;
    int min_satisfying_lit = -1;
    for (int j=0; clauses[clause_index][j]!=0; j++) {
      int lit = clauses[clause_index][j];
      if (SET(lit)) {
        satisfied = true;
        int varlit = VAR(lit);
        if (vars[varlit].mark2 == true) {
          min_satisfying_var = varlit;
          min_satisfying_lit = lit;
          break;
        }
        if ((min_satisfying_var==-1)||(
        	((lit>0) && (min_satisfying_lit<0)) || // priority towards positive literals
        	(varlit<min_satisfying_var)
        	))
          min_satisfying_var = varlit;
      }
    }
    if (!satisfied) // in the unlikely event that one of thoes variables is unsat, return false
      return false;
    vars[min_satisfying_var].mark2 = true;
  }
  // by default add all unit-conflict clauses as nessisary
  for (auto it = unit_conflicts.begin(); it != unit_conflicts.end(); it++)
    vars[VAR(*it)].mark2 = true;
  // all variables not marked are set to free
  for (int i=1; i<=vc; i++)
    if (!(vars[i].mark2))
      vars[i].value = _FREE;
  return true;
}






void SatSolver::push_to_reducer(deque<int> *toPushClause, int inClauseLitPool, int inClauseLitPoolPos) {
  int metric = toPushClause->size(); // TODO allow other metrics
  // Set up array of clause, including its litPool and litPoolPos
  const int clauseLengthWithPos = toPushClause->size() + 2;
  int *clauseArrayWithPos = new int[clauseLengthWithPos];
  // Convert to array
  for (int litIndex = 0; litIndex < clauseLengthWithPos - 2; litIndex++)
    clauseArrayWithPos[litIndex] = (*toPushClause)[litIndex];
  clauseArrayWithPos[clauseLengthWithPos - 2] = inClauseLitPool;
  clauseArrayWithPos[clauseLengthWithPos - 1] = inClauseLitPoolPos;
  // Generate structure containing extra information
  WorkWithPosArrayWithLength *workWithPosArrayWithLength = new WorkWithPosArrayWithLength; // TODO this is not getting deallocated
  workWithPosArrayWithLength->clauseArrayWithPos = clauseArrayWithPos;
  workWithPosArrayWithLength->length = clauseLengthWithPos;
  // Push to the workset
  work.insert(workWithPosArrayWithLength, metric);
  // Check if ready to MPI send another batch
  if (mpi_buffer->readyToSend()) {
    // Send in batches to get the best from the workset, not just the most recent
    int workMetric;
    tie(workWithPosArrayWithLength, workMetric) = work.get();
    while (workWithPosArrayWithLength->length + 1 <= mpi_buffer->getRemainingOutSpace()) {
      mpi_buffer->pushClauseNoAutoSend(workWithPosArrayWithLength->clauseArrayWithPos, workWithPosArrayWithLength->length);
      delete[] workWithPosArrayWithLength->clauseArrayWithPos; // TODO deleting the underlying clause array
      delete workWithPosArrayWithLength;
      if (work.available())
        tie(workWithPosArrayWithLength, workMetric) = work.get();
      else {
        workWithPosArrayWithLength = NULL;
        break;
      }
    }
    // This one couldn't fit in the out buffer so put back in the workset
    if (workWithPosArrayWithLength)
      if (mpi_buffer->getWillThisClauseEverFit(workWithPosArrayWithLength->length)) // TODO review off by one errors
        work.insert(workWithPosArrayWithLength, workMetric);
  }
}



void SatSolver::add_arbitrary_clause(int *inClauseArray, int inClauseLength, int inClauseLitPool, int inClauseLitPoolPos) {
  if ((inClauseLitPool == -1) && (inClauseLitPoolPos == -1))
    return; // unimplemented
  if (inClauseLength < 2)
    return; // Do not handle unit clauses
  // existing clause amendment
  // First try remove any lit in the litpool, that isn't in the newfound reduced clause, BUT, only from the 3rd lit forward
  int *originalConflictClause = litPools[inClauseLitPool] + inClauseLitPoolPos;
  int originalLength;
  for (originalLength=0; originalConflictClause[originalLength]; originalLength++);
  assert(isSubset(inClauseArray, inClauseLength, originalConflictClause, originalLength));
  int newLength = 2;
  for (int i = 2; i < originalLength; i++)
    if (litInClause(originalConflictClause[i], inClauseArray, inClauseLength)) {
      originalConflictClause[newLength] = originalConflictClause[i];
      newLength++;
    }
  originalConflictClause[newLength] = 0;
  assert(originalConflictClause[newLength - 1]);
  assert(!originalConflictClause[newLength]);
  // Try reduce the first 2 literals
  // for each of the first 2 lits, IF one is to be removed AND there is an elibible candidate elsewhere in the clause, push the eligible candidate down
  for (int zeroOne = 0; zeroOne < 2; zeroOne++) { // for each of the first 2 literals
    if (!litInClause(originalConflictClause[zeroOne], inClauseArray, inClauseLength)) {
      // First or second one should be removed if it can be, try find an elligible replacement
      for (int prospectiveReplacementIndex = 2; prospectiveReplacementIndex < newLength; prospectiveReplacementIndex++) {
        int lit = originalConflictClause[prospectiveReplacementIndex];
        if (((vars[VAR(lit)].value == _FREE) && (VAR(lit) != VAR(*stackTop))) || SET(lit)) {
          // Found a replacement, Change Watchlists accordingly
          vector<int *> &watchlist = WATCHLIST(originalConflictClause[zeroOne]); // Get watchlist of the lit being replaced
          int index = 0;
          while (watchlist[index] != originalConflictClause) index++;  // Find in that watchlist, the index of THIS clause
          watchlist.erase(watchlist.begin() + index);                  // then erase it
          // set up a new watch
          WATCHLIST(lit).push_back(originalConflictClause);
          // Moving lits around
          originalConflictClause[zeroOne] = lit;       // Move found replacement to beginning
          originalConflictClause[prospectiveReplacementIndex] = originalConflictClause[newLength - 1]; // move whatever was at the end to replace the lit that was just moved
          originalConflictClause[newLength - 1] = 0;                                                   // replace with 0 as new end
          newLength--;
          break;
        }
      }
    }
  }
}

void SatSolver::get_from_reducer() {
  ClauseWithPos *clauseWithPos;
  while ((clauseWithPos = mpi_buffer->getClause())) {
    VLOG(4) << "got clause: " << clauseWithPos->litPool << " " << clauseWithPos->litPoolPos << std::endl;
    add_arbitrary_clause(clauseWithPos->clause.data(), clauseWithPos->clause.size(), clauseWithPos->litPool, clauseWithPos->litPoolPos);
    delete clauseWithPos;
  }
}




SatSolver::SatSolver(
    Cnf* cnf,
    int decision_interval,
    int suggestion_size,
    MPI_Comm *communicator_sls,
    MPI_Comm *communicator_strengthener,
    bool pure_literal_assertion,
    bool short_stopping,
    string &heuristic_rotation_scheme,
    int phase)
    : CnfManager(cnf) {
this->phase = phase;
this->decisions = 0;
  this->communicator_sls = communicator_sls;
  this->communicator_strengthener = communicator_strengthener;
  this->decision_interval = decision_interval;
  this->suggestion_size = suggestion_size;
  this->short_stopping = short_stopping;
  this->sls_solution_count = 0;
  this->sls_novel_solution_count = 0;
  this->sls_channel = NULL;
  nVars = 0;
  nextVar = 0;
  nextClause = clauses.size() - 1;

  heuristic_rotation = suggestion_first;
  if(0 == strcmp("slsfirst", heuristic_rotation_scheme.c_str())){
    heuristic_rotation = suggestion_first;
  } else if (0 == strcmp("cdclfirst", heuristic_rotation_scheme.c_str())) {
    heuristic_rotation = cdcl_first;
  } else if (0 == strcmp("", heuristic_rotation_scheme.c_str())) {
    heuristic_rotation = cdcl_first;
  } else {
    throw ConsistencyException("SatSolver has invalid heuristic_rotation scheme");
  }

  // initialize parameters
  nextRestart = luby.next() * (lubyUnit = 512);
  nextDecay = HALFLIFE;

  // assertUnitClauses has not failed
  if (dLevel != 0) {
    if (pure_literal_assertion) // assert pure literals
      for (int i = 1; i <= (int)vc; i++)
        if (vars[i].value == _FREE) {
          if (vars[i].activity[_POSI] == 0 && vars[i].activity[_NEGA] > 0)
            // ante is NULL, as opposed to empty clause for implied literals
            assertLiteral(-i, NULL);
          else if (vars[i].activity[_NEGA] == 0 && vars[i].activity[_POSI] > 0)
            assertLiteral(i, NULL);
        }
    // initialize varOrder
    for (unsigned i = 1; i <= vc; i++)
      if (vars[i].value == _FREE && SCORE(i) > 0) {
        varOrder[nVars++] = i;
        vars[i].phase = (vars[i].activity[_POSI] > vars[i].activity[_NEGA]) ? _POSI : _NEGA;
      }
    sort(varOrder, varOrder + nVars, compScores(vars));
    for (unsigned i = 0; i < nVars; i++)
      varPosition[varOrder[i]] = i;
  }

  if (communicator_strengthener != NULL) {
    mpi_buffer = new MpiBuffer(communicator_strengthener, phase, REDUCER_RANK, REDUCER_TO_TINISAT_CLAUSE_TAG, TINISAT_TO_REDUCER_CLAUSE_TAG);
  } else {
    mpi_buffer = NULL;
  }
  if (communicator_sls != NULL) {
    sls_solution_buffer_size = cnf_holder->max_vc+2;
    TEST_NOT_NULL(sls_solution_buffer = (int *)malloc(sls_solution_buffer_size * sizeof(int)))
  } else {
    sls_solution_buffer_size = 0;
    sls_solution_buffer = NULL;
  }
  unit_conflicts.clear();

  // allocate the prefix block built from vars[] and handed to the channel
  TEST_NOT_NULL(prefix = (int *)calloc(vc + 1, sizeof(int)))

  // COLLECTIVE: construct the SLS channel in lockstep with the gnovelty helpers
  // (its constructor performs the collective MPI_Win_allocate). The channel owns
  // the window, suggestion double-buffer, per-helper depths and solution receive.
  if (communicator_sls != NULL)
    sls_channel = new SlsChannel(communicator_sls, suggestion_size, cnf_holder->max_vc + 2);
  else
    sls_channel = NULL;
}





SatSolver::~SatSolver() {
  // The channel's destructor sends the length-0 completion prefix to each
  // gnovelty helper, closes any open RMA epoch, frees the collective window and
  // cancels its solution receive (all previously inline here).
  if (sls_channel != NULL)
    delete sls_channel;
  if (sls_solution_count > 0)
    VLOG(1) << " SLS found " << sls_solution_count << " solutions (" << sls_novel_solution_count << " novel)";
  free(sls_solution_buffer);
  free(prefix);
  if (mpi_buffer != NULL)
    delete mpi_buffer;
}





/**
 * Reset any SLS helpers that were working on a prefix greater than the level
 * to which backtracking occurred.
 */
void SatSolver::backtrack_func(int level) {
  backtrack(level);
  if (sls_channel != NULL)
    sls_channel->backtrack_reset(level);
}

int SatSolver::sls__get_solutions() {
  if (sls_channel == NULL)
    return 0;
  VLOG(4) << "checking for solutions from SLS";
  // The channel does the (phase-checked) non-blocking receive; it returns the
  // number of literals of a matching-phase solution (0-terminated in the buffer),
  // or 0 if nothing arrived. The novelty check + loading stay here because they
  // touch tinisat's clauses[]/vars[].
  int recv_size = sls_channel->poll_solution(sls_solution_buffer, (int)sls_solution_buffer_size, phase);
  if (recv_size <= 0)
    return false;
  // check whether the solution is novel (compatible with all clauses, original
  // and learnt, that this tinisat knows about)
  bool new_solution = true;
  for (int c = 0; (c < clauses.size()) && (new_solution); c++) {
    int size = 0;
    while (clauses[c][size] != 0) size++;
    new_solution = new_solution && isClauseOverlap(clauses[c], size, sls_solution_buffer, recv_size);
  }
  sls_solution_count++;
  if (new_solution) { // if novel, load tinisat with the solution and propagate up
    VLOG(2) << "received novel solution from SLS";
    for (int i = 0; i <= vc; i++) // hard reset the SAT solver's variables
      vars[i].value = _FREE;
    for (int i = 0; sls_solution_buffer[i] != 0; i++) { // load in from gnovelty
      int v = sls_solution_buffer[i];
      if (v > 0) {
        vars[VAR(v)].value = _POSI;
      } else if (v < 0) {
        vars[VAR(v)].value = _NEGA;
      }
    }
    sls_novel_solution_count++;
    return true; // found solution
  }
  VLOG(3) << "received solution from SLS is NOT novel";
  return false;
}



/**
 * Get a suggestion for the next literal to set from the SLS instance which
 * is working off the highest-level prefix below the current decision level.
 */
int SatSolver::get_suggestion() {
  if (sls_channel == NULL)
    return 0;
  // number of decisions
  int decisions = dLevel - 1;
  // the decision 'tier' we are on, depth divided into decision_intervals
  int tier = decisions / decision_interval;
  // the depth that marks the beginning of the tier we are on
  int dLevelIndex = tier * decision_interval;
  if ((decisions % decision_interval == 0) &&
      (sls_channel->processDLevel[sls_channel->currentSLS] != dLevelIndex)) {
    // On a tier marker with the next helper not yet working it: build the prefix
    // (the currently-assigned literals) and hand it to that helper. send_prefix
    // records the depth (even for an empty prefix, so this guard advances) and
    // only transmits + rotates when there is something to send.
    int prefixLength = 0;
    for (int i = 1; i <= vc; i++)
      if (vars[VAR(i)].value != _FREE)
        prefix[prefixLength++] = (vars[VAR(i)].value == _POSI ? 1 : -1) * i;
    sls_channel->send_prefix(prefix, prefixLength, dLevelIndex);
    return 0;
  }

  // otherwise return the next suggested literal that is still free. The channel
  // manages the RMA double-buffer refill from the best helper internally.
  int s;
  while ((s = sls_channel->next_suggestion(dLevel)) != 0) {
    if (VAR(s) <= vc && vars[VAR(s)].value == _FREE)
      return s;
  }
  return 0;
}


// add the conflicts as a clause to solver, return false if contradiction
// need to pass unit_conflicts by reference since we want to add to them
bool SatSolver::solver_add_conflict_clause(std::deque<int> conflicts) {
  conflictLits = conflicts;

  if (communicator_strengthener != NULL)
    get_from_reducer();
  int clause_index = addClause();
  if (communicator_strengthener != NULL)
    push_to_reducer(&conflictLits, litPools.size()-1, conflictClause - litPool);

  // special handling is required for unit conflict clauses, as Tiny sat ignores them...
  if (conflictLits.size() == 1) {
    unit_conflicts.push_back(conflictLits[0]);
  }
  if (clause_index!=-1) {
    solution_conflict_indices.push_back(clause_index);
  }
  conflictLits.clear();
  return true;
}

// full, hard clear of the solver's state
bool SatSolver::reset_solver() {
  while (stackTop[-1] != 0) { // clear the SAT solver's stack
    stackTop--;
    stackTop[0] = 0;
  }
  for (int i = 0; i <= vc; i++) { // reset the SAT solver's variables
    vars[i].value = _FREE;
    vars[i].dLevel = 0;
  }
  dLevel = 1; // misc reset stuff
  nextVar = 0;
  nextClause = clauses.size() - 1;

  //add in the unit conflict clauses
  int *zero = &(stackTop[-1]);
  for (auto it = unit_conflicts.begin(); it != unit_conflicts.end(); it++) {
    //** if unset add to the top of the stack, with the zero literal as ancdeant
    if (vars[VAR(*it)].value == _FREE) {
      setLiteral(*it, zero);
      *(stackTop) = *it;
      stackTop++;
    } else if (vars[VAR(*it)].value == SIGN(NEG(*it))) {
      return false;
    }
  }
  // after a reset, the SAT keeps the binary implication lists and 3+ length clausees but discards the unitary clauses
  // need to re-add them and assert them.
  for (int i = 0; i < cnf->cc; i++) {
    if (cnf->clauses[i][1] == 0) {  // unit clause
      int lit = cnf->clauses[i][0]; //** if unset add to the top of the stack, with the zero literal as ancdeant
      if (vars[VAR(lit)].value == _FREE) {
        setLiteral(lit, zero);
        *(stackTop) = lit;
        stackTop++;
      } else if (vars[VAR(lit)].value == SIGN(NEG(lit))) {
        return false;
        //printf(" contradictory unit clauses %d, %d\n", lit, -lit);
        //printf(" UNSATISFIABLE message\n");
        //exit(20);
      }
    }
  }
  assertUnitClauses();
  return true;
}



void SatSolver::printSolution(FILE *ofp){
  for(unsigned i = 1; i <= vc; i++)
    if(vars[i].value == _POSI) fprintf(ofp, "%d ", i);
    else if(vars[i].value == _NEGA) fprintf(ofp, "-%d ", i);
  fprintf(ofp, "0\n");
}



