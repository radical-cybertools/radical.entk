#!/usr/bin/env python



__author__       = "Vivek <vivek.balasubramanian@rutgers.edu>"
__copyright__    = "Copyright 2014, http://radical.rutgers.edu"
__license__      = "MIT"
__example_name__ = "Multiple Simulations Instances, Single Analysis Instance Example (MSSA)"


import sys
import os
import json

from radical.ensemblemd import Kernel
from radical.ensemblemd import SimulationAnalysisLoop
from radical.ensemblemd import EnsemblemdError
from radical.ensemblemd import SingleClusterEnvironment

# ------------------------------------------------------------------------------
#
class MSSA(SimulationAnalysisLoop):
    """MSMA exemplifies how the MSMA (Multiple-Simulations / Multiple-Analsysis)
       scheme can be implemented with the SimulationAnalysisLoop pattern.
    """
    def __init__(self, iterations, simulation_instances, analysis_instances, adaptive_simulation):
        SimulationAnalysisLoop.__init__(self, iterations, simulation_instances, analysis_instances, adaptive_simulation)


    def simulation_step(self, iteration, instance):
        """In the simulation step we
        """
        k = Kernel(name="misc.mkfile")
        k.arguments = ["--size=1000", "--filename=asciifile-{0}.dat".format(instance)]
        k.download_output_data = ['asciifile-{0}.dat > iter{1}/asciifile-{0}.dat'.format(instance,iteration)]
        return [k]

    def analysis_step(self, iteration, instance):
        """In the analysis step we use the ``$PREV_SIMULATION`` data reference
           to refer to the previous simulation. The same
           instance is picked implicitly, i.e., if this is instance 5, the
           previous simulation with instance 5 is referenced.
        """
        k = Kernel(name="misc.randval")
        k.arguments            = ["--upperlimit=16"]
        return [k]


# ------------------------------------------------------------------------------
#
if __name__ == "__main__":

    try:

        # Create a new static execution context with one resource and a fixed
        # number of cores and runtime.
        cluster = SingleClusterEnvironment(
                        resource="xsede.stampede",
                        cores=16,
                        walltime=5,
                        username='vivek91',

                        project = 'TG-MCB090174',
                        queue = 'development',

                        database_url='mongodb://vivek:hawkie@ds037145.mongolab.com:37145/rp',
                        #database_name='myexps',
        )

        # Allocate the resources.
        cluster.allocate()

        # We set both the the simulation and the analysis step 'instances' to 16.
        # If they
        mssa = MSSA(iterations=2, simulation_instances=16, analysis_instances=1, adaptive_simulation=True)

        cluster.run(mssa)

        cluster.deallocate()

    except EnsemblemdError, er:

        print "Ensemble MD Toolkit Error: {0}".format(str(er))
        raise # Just raise the execption again to get the backtrace
