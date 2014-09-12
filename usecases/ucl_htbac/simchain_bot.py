#!/usr/bin/env python

""" 
'HT-BAC simchain' bag-of-task proof-of-concept (UCL).
"""

__author__        = "Ole Weider <ole.weidner@rutgers.edu>"
__copyright__     = "Copyright 2014, http://radical.rutgers.edu"
__license__       = "MIT"
__use_case_name__ = "'HT-BAC simchain' bag-of-task proof-of-concept (UCL)."


from radical.ensemblemd import Kernel
from radical.ensemblemd import Pipeline
from radical.ensemblemd import EnsemblemdError
from radical.ensemblemd import SimulationAnalysisLoop
from radical.ensemblemd import SingleClusterEnvironment


# ------------------------------------------------------------------------------
#
class UCL_BAC_SimChain(Pipeline):

    def __init__(self, width):
        Pipeline.__init__(self, width)

    def step_01(self, instance):
        """This steps calculates the trajectories. 
        """
        k = Kernel(name="md.namd")
        k.set_args(["eq%{0}.inp".format(instance)])
        k.download_input_data(
            ["http://testing.saga-project.org/cybertools/sampledata/BAC-SIMCHAIN/simchain-sample-data/complex.pdb > complex.pdb",
             "http://testing.saga-project.org/cybertools/sampledata/BAC-SIMCHAIN/simchain-sample-data/complex.top > complex.top",
             "http://testing.saga-project.org/cybertools/sampledata/BAC-SIMCHAIN/simchain-sample-data/cons.pdb > cons.pdb",
             "http://testing.saga-project.org/cybertools/sampledata/BAC-SIMCHAIN/simchain-sample-data/eq0.inp > eq%{0}.inp".format(instance)])

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
            walltime=15
        )

        # According to the use-case, about 50 trajectories are simulated in a 
        # production run. Hence, we set the pipeline width to 50.  
        simchain = UCL_BAC_SimChain(width=50)

        cluster.run(simchain)

    except EnsemblemdError, er:

        print "EnsembleMD Error: {0}".format(str(er))
        raise # Just raise the execption again to get the backtrace
