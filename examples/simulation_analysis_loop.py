#!/usr/bin/env python


__author__       = "Ole Weider <ole.weidner@rutgers.edu>"
__copyright__    = "Copyright 2014, http://radical.rutgers.edu"
__license__      = "MIT"
__example_name__ = "Simulation-Analysis Example (generic)"


import sys
import os
import json

from radical.ensemblemd import Kernel
from radical.ensemblemd import SimulationAnalysisLoop
from radical.ensemblemd import EnsemblemdError
from radical.ensemblemd import SimulationAnalysisLoop
from radical.ensemblemd import SingleClusterEnvironment

# ------------------------------------------------------------------------------
#
class RandomSA(SimulationAnalysisLoop):
    """RandomSA implements the simulation-analysis loop described above. It
       inherits from radical.ensemblemd.SimulationAnalysisLoop, the abstract
       base class for all Simulation-Analysis applications.
    """
    def __init__(self, maxiterations, simulation_instances=1, analysis_instances=1):
        SimulationAnalysisLoop.__init__(self, maxiterations, simulation_instances, analysis_instances)

    def pre_loop(self):
        """pre_loop is executed before the main simulation-analysis loop is
           started. In this example we create an initial 1 kB random ASCII file
           that we use as the reference for all analysis steps.
        """
        k = Kernel(name="misc.mkfile")
        k.arguments = ["--size=1000", "--filename=reference.dat"]
	k.upload_input_data = ['levenshtein.py']
        return k

    def simulation_step(self, iteration, instance):
        """The simulation step generates a 1 kB file containing random ASCII
           characters that is compared against the 'reference' file in the
           subsequent analysis step.
        """
        k = Kernel(name="misc.mkfile")
        k.arguments = ["--size=1000", "--filename=simulation-{0}-{1}.dat".format(iteration, instance)]
        return k

    def analysis_step(self, iteration, instance):
        """In the analysis step, we take the previously generated simulation
           output and perform a Levenshtein distance calculation between it
           and the 'reference' file.

           ..note:: The placeholder ``$PRE_LOOP`` used in ``link_input_data`` is
                    a reference to the working directory of pre_loop.
                    The placeholder ``$PREV_SIMULATION`` used in ``link_input_data``
                    is a reference to the working directory of the previous
                    simulation step.

                    It is also possible to reference a specific
                    simulation step using ``$SIMULATION_N`` or all simulations
                    via ``$SIMULATIONS``. Analogous placeholders exist for
                    ``ANALYSIS``.
        """
        input_filename  = "simulation-{0}-{1}.dat".format(iteration, instance)
        output_filename = "analysis-{0}-{1}.dat".format(iteration, instance)

        k = Kernel(name="misc.levenshtein")
        k.link_input_data      = ["$PRE_LOOP/reference.dat", "$SIMULATION_ITERATION_{1}_INSTANCE_{2}/{0}".format(input_filename,iteration,instance),"$PRE_LOOP/levenshtein.py"]
        k.arguments            = ["--inputfile1=reference.dat",
                                  "--inputfile2={0}".format(input_filename),
                                  "--outputfile={0}".format(output_filename)]
        k.download_output_data = output_filename
        return k

    def post_loop(self):
        # post_loop is executed after the main simulation-analysis loop has
        # finished. In this example we don't do anything here.
        pass


# ------------------------------------------------------------------------------
#
if __name__ == "__main__":

    # use the resource specified as argument, fall back to localhost
    try:
        # Create a new static execution context with one resource and a fixed
        # number of cores and runtime.
        cluster = SingleClusterEnvironment(
                        resource='local.localhost',
                        cores=1,
                        walltime=15,

                        #username=None,
                        #project=None,
                        #queue = None,
                        #database_url='',
                        #database_name='',
        )

        # Allocate the resources.
        cluster.allocate()

        # We set both the the simulation and the analysis step 'instances' to 16.
        # This means that 16 instances of the simulation step and 16 instances of
        # the analysis step are executed every iteration.
        randomsa = RandomSA(maxiterations=1, simulation_instances=16, analysis_instances=16)

        cluster.run(randomsa)
        

        cluster.deallocate()


        # After execution has finished, we print some statistical information
        # extracted from the analysis results that were transferred back.
        for it in range(1, randomsa.iterations+1):
            print "\nIteration {0}".format(it)
            ldists = []
            for an in range(1, randomsa.analysis_instances+1):
                ldists.append(int(open("analysis-{0}-{1}.dat".format(it, an), "r").readline()))
            print "   * Levenshtein Distances: {0}".format(ldists)
            print "   * Mean Levenshtein Distance: {0}".format(sum(ldists) / len(ldists))

    except EnsemblemdError, er:

        print "Ensemble MD Toolkit Error: {0}".format(str(er))
        raise # Just raise the execption again to get the backtrace
