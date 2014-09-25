#!/usr/bin/env python

""" 
This example shows how to use the EnsembleMD Toolkit ``SimulationAnalysis`` 
pattern to execute 64 iterations of a simulation analysis loop. Each
``simulation_step`` generates 16 files with a random value between [0..100]. 
The ``analysis_step`` checks whether any of the values equals 42.

Run this example with ``RADICAL_ENMD_VERBOSE`` set to ``info`` if you want to 
see log messages about plug-in invocation and simulation progress::

    RADICAL_ENMD_VERBOSE=info python simulation_analysis_loop.py
"""

__author__       = "Ole Weider <ole.weidner@rutgers.edu>"
__copyright__    = "Copyright 2014, http://radical.rutgers.edu"
__license__      = "MIT"
__example_name__ = "A Simulation-Analysis-Loop Example"


from radical.ensemblemd import Kernel
from radical.ensemblemd import SimulationAnalysisLoop
from radical.ensemblemd import EnsemblemdError
from radical.ensemblemd import SimulationAnalysisLoop
from radical.ensemblemd import SingleClusterEnvironment


# ------------------------------------------------------------------------------
#
class RandomSA(SimulationAnalysisLoop):
    # 'RandomSA' implements the simulation-analysis loop described above. It
    # inherits from radical.ensemblemd.Pipeline, the abstract base class 
    # for all pipelines. 

    def __init__(self, maxiterations, simulation_instances=1, analysis_instances=1):
        SimulationAnalysisLoop.__init__(self, maxiterations, simulation_instances, analysis_instances)

    def pre_loop(self):
        # Pre_loop is executed before the main simulation-analysis loop is 
        # started. In this example we don't need this.
        pass

    def simulation_step(self, iteration, instance):
        k = Kernel(name="misc.randval") 
        k.arguments = ["--upperlimit=100", "--outputfile=simulation-iter%{0}-%{1}.dat".format(iteration, instance)]
        return k

    def analysis_step(self, iteration, instance):
        k = Kernel(name="misc.checkvalues")
        k.arguments = ["--inputfiles=simulation-iter%{0}-*.dat", "--value=42", "--outputfile=result-iter%{0}.dat".format(iteration)]
        k.download_output_data = "result-iter%{0}.dat"
        return k

    def post_loop(self):
        # post_loop is executed after the main simulation-analysis loop has
        # finished. In this example we don't need this.
        pass


# ------------------------------------------------------------------------------
#
if __name__ == "__main__":

    try:
        # Create a new static execution context with one resource and a fixed
        # number of cores and runtime.
        cluster = SingleClusterEnvironment(
            resource="localhost", 
            cores=1, 
            walltime=15
        )

        # We set the 'instances' of the simulation step to 16. This means that 16 
        # instances of the simulation are executed every iteration.
        # We set the 'instances' of the analysis step to 1. This means that only 
        # one instance of the analysis is executed for each iteration
        randomsa = RandomSA(maxiterations=64, simulation_instances=16, analysis_instances=1)

        cluster.run(randomsa)

    except EnsemblemdError, er:

        print "EnsembleMD Error: {0}".format(str(er))
        raise # Just raise the execption again to get the backtrace
