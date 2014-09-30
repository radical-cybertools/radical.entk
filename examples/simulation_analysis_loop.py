#!/usr/bin/env python

""" 
This example shows how to use the EnsembleMD Toolkit ``SimulationAnalysis`` 
pattern to execute 64 iterations of a simulation analysis loop. In the 
``pre_loop`` step, a global configuration for the simulation step is uploaded.
Each ``simulation_step`` generates 16 files with a random value between [0..100]. 
The ``analysis_step`` checks whether any of the values equals 42.

.. figure:: images/simulation_analysis_pattern.*
   :width: 300pt
   :align: center
   :alt: Simulation-Analysis Pattern

   Fig.: `The Simulation-Analysis Pattern.`

Run this example with ``RADICAL_ENMD_VERBOSE`` set to ``info`` if you want to 
see log messages about plug-in invocation and simulation progress::

    RADICAL_ENMD_VERBOSE=info python simulation_analysis_loop.py

By default, simulation and analysis steps run on one core your local machine:: 

    SingleClusterEnvironment(
        resource="localhost", 
        cores=1, 
        walltime=30,
        username=None,
        allocation=None
    )

You can change the script to use a remote HPC cluster and increase the number 
of cores to see how this affects the runtime of the script as the individual
pipeline instances can run in parallel::

    SingleClusterEnvironment(
        resource="stampede.tacc.utexas.edu", 
        cores=16, 
        walltime=30,
        username=None,  # add your username here 
        allocation=None # add your allocation or project id here if required
    )

"""

__author__       = "Ole Weider <ole.weidner@rutgers.edu>"
__copyright__    = "Copyright 2014, http://radical.rutgers.edu"
__license__      = "MIT"
__example_name__ = "Simulation-Analysis (generic)"

import os

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
           started. In this example we create an initial 100 kB random ASCII file 
           that we use as the reference for all analysis steps.
        """
        k = Kernel(name="misc.mkfile") 
        k.arguments = ["--size=10000", "--filename=reference.dat"]
        return k

    def simulation_step(self, iteration, instance):
        """The simulation step generates a 1 MB file containing random ASCII
           characters that is compared against the 'reference' file in the 
           subsequent analysis step.
        """
        k = Kernel(name="misc.mkfile") 
        k.arguments = ["--size=10000", "--filename=simulation-{0}-{1}.dat".format(iteration, instance)]
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
        k.link_input_data      = ["$PRE_LOOP/reference.dat", "$PREV_SIMULATION/{0}".format(input_filename)]
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

    try:
        # Create a new static execution context with one resource and a fixed
        # number of cores and runtime.
        cluster = SingleClusterEnvironment(
            resource="localhost", 
            cores=1, 
            walltime=30,
            username=None,
            allocation=None
        )

        # We set both the the simulation and the analysis step 'instances' to 16. 
        # This means that 16 instances of the simulation step and 16 instances of
        # the analysis step are executed every iteration.
        randomsa = RandomSA(maxiterations=8, simulation_instances=4, analysis_instances=4)

        cluster.run(randomsa)

    except EnsemblemdError, er:

        print "EnsembleMD Error: {0}".format(str(er))
        raise # Just raise the execption again to get the backtrace
