#!/usr/bin/env python

"""  

Explain...

.. code-block:: bash

    [S]    [S]    [S]    [S]    [S]    [S]    [S]    [S]
     |      |      |      |      |      |      |      |
    [A]    [A]    [A]    [A]    [A]    [A]    [A]    [A]
     |      |      |      |      |      |      |      |
    [S]    [S]    [S]    [S]    [S]    [S]    [S]    [S]
     |      |      |      |      |      |      |      |
    [A]    [A]    [A]    [A]    [A]    [A]    [A]    [A]
     :      :      :      :      :      :      :      :

.. _multiple_simulations_multiple_analysis:

Example Source 
^^^^^^^^^^^^^^
"""

__author__       = "Ole Weider <ole.weidner@rutgers.edu>"
__copyright__    = "Copyright 2014, http://radical.rutgers.edu"
__license__      = "MIT"
__example_name__ = "Multiple Simulations Instances, Multiple Analysis Instances (MSMA)"

from radical.ensemblemd import Kernel
from radical.ensemblemd import SimulationAnalysisLoop
from radical.ensemblemd import EnsemblemdError
from radical.ensemblemd import SingleClusterEnvironment

# ------------------------------------------------------------------------------
#
class MSMA(SimulationAnalysisLoop):
    """MSMA exemplifies how the MSMA (Multiple-Simulations / Multiple-Analsysis)
       scheme can be implemented with the SimulationAnalysisLoop pattern.
    """
    def __init__(self, iterations, simulation_instances, analysis_instances):
        SimulationAnalysisLoop.__init__(self, iterations, simulation_instances, analysis_instances)


    def simulation_step(self, iteration, instance):
        """TODO
        """
        k = Kernel(name="misc.mkfile") 
        k.arguments = ["--size=1000", "--filename=asciifile.dat"]
        return k

    def analysis_step(self, iteration, instance):
        """In the analysis step, we simply use the ``$PREV_SIMULATION``
           placeholder to references the previous simulation. The same 
           instance is picked implicitly, i.e., if this is instance 5, the
           previous simulation with instance 5 is referenced. 
        """
        k = Kernel(name="misc.ccount")
        k.arguments            = ["--inputfile=asciifile.dat", "--outputfile=cfreqs.dat"]
        k.link_input_data      = "$PREV_SIMULATION/asciifile.dat".format(instance)
        k.download_output_data = "cfreqs.dat > cfreqs-{iteration}-{instance}".format(instance=instance, iteration=iteration)
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
        # If they
        msma = MSMA(iterations=4, simulation_instances=16, analysis_instances=16)

        cluster.run(msma)

    except EnsemblemdError, er:

        print "EnsembleMD Error: {0}".format(str(er))
        raise # Just raise the execption again to get the backtrace
