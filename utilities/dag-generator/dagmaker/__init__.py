"""dagmaker -- a structure-aware DAG generator for the Dagster distributed SAT solver.

The package produces a Dagster ``.dag`` decomposition file from a DIMACS CNF.
Its design goal is to maximise *useful* parallelism subject to a vertex-separator
width budget, because Dagster's execution cost is exponential in the number of
variables passed along a DAG edge (see ``scorer`` and the project plan).

The core (CNF parsing, interval compaction, the DAG model, the validity gate and
the min-degree elimination backend) depends only on the Python standard library.
Optional backends (``decompose.rcm``, ``decompose.external``) use numpy/scipy/
pymetis/kahypar when installed and are skipped gracefully otherwise.
"""

__all__ = [
    "intervals",
    "cnf",
    "dagmodel",
    "validate",
]
