#!/usr/bin/env python

"""  

Explain...

.. code-block:: none

    [S]    [S]    [S]    [S]    [S]    [S]    [S] 
     |      |      |      |      |      |      |  
     \-----------------------------------------/
                          |
                         [A]
                          |
     /-----------------------------------------\
     |      |      |      |      |      |      |    
    [S]    [S]    [S]    [S]    [S]    [S]    [S] 
     |      |      |      |      |      |      |  
     \-----------------------------------------/
                          |
                         [A]
                          :  

.. _multiple_simulations_single_analysis:

Example Source 
^^^^^^^^^^^^^^
"""

__author__       = "Vivek <vivek.balasubramanian@rutgers.edu>"
__copyright__    = "Copyright 2014, http://radical.rutgers.edu"
__license__      = "MIT"
__example_name__ = "Multiple Simulations Instances, Single Analysis Instance (MSSA)"

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
    def __init__(self, iterations, simulation_instances, analysis_instances):
        SimulationAnalysisLoop.__init__(self, iterations, simulation_instances, analysis_instances)


    def simulation_step(self, iteration, instance):
        """In the simulation step we
        """
        k = Kernel(name="misc.mkfile") 
        k.arguments = ["--size=1000", "--filename=asciifile.dat"]
        return k

    def analysis_step(self, iteration, instance):
        """In the analysis step we use the ``$PREV_SIMULATION`` data reference
           to refer to the previous simulation. The same 
           instance is picked implicitly, i.e., if this is instance 5, the
           previous simulation with instance 5 is referenced. 
        """
        link_input_data = []
        for i in range(0, self.simlation_instances):
            link_input_data.append("$PREV_SIMULATION_INSTANCE_{instance}/asciifile.dat > asciifile-{instance}.dat".format(instance=i))

        k = Kernel(name="misc.ccount")
        k.arguments            = ["--inputfile=asciifile-*.dat", "--outputfile=cfreqs.dat"]
        k.link_input_data      = link_input_data
        k.download_output_data = "cfreqs.dat > cfreqs-{iteration}.dat".format(iteration=iteration)
        return k


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
        mssa = MSSA(iterations=2, simulation_instances=8, analysis_instances=1)

        cluster.run(mssa)

    except EnsemblemdError, er:

        print "EnsembleMD Error: {0}".format(str(er))
        raise # Just raise the execption again to get the backtrace
