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

    def __init__(self, instances):
        Pipeline.__init__(self, instances)

    def step_01(self, instance):
        """This steps does the trajectory simulation. First, the input files 
           are downloaded from a remote HTTP server. Next, NAMD reads the 'inp' 
           ("input") file and simulates the trajectory. Finally, the output is 
           transferred back to the machine on which this script is executed. 
        """
        namd = Kernel(name="md.namd")
        namd.core                 = 4
        namd.arguments            = ["eq{0}.inp".format(instance)]
        namd.download_input_data  = ["http://testing.saga-project.org/cybertools/sampledata/BAC-SIMCHAIN/simchain-sample-data/complex.pdb > complex.pdb",
                                     "http://testing.saga-project.org/cybertools/sampledata/BAC-SIMCHAIN/simchain-sample-data/complex.top > complex.top",
                                     "http://testing.saga-project.org/cybertools/sampledata/BAC-SIMCHAIN/simchain-sample-data/cons.pdb > cons.pdb",
                                     "http://testing.saga-project.org/cybertools/sampledata/BAC-SIMCHAIN/simchain-sample-data/eq0.inp > eq{0}.inp".format(instance)]
        namd.download_output_data =  "STDOUT > eq{0}.out".format(instance)
        return namd

# ------------------------------------------------------------------------------
#
if __name__ == "__main__":

    try:
        # Create a new single cluster environment with a fixed umber of 
        # cores and runtime.
        cluster = SingleClusterEnvironment(
            resource="stampede.tacc.utexas.edu", 
            cores=256,   # (4 cores * 64 tasks) 
            walltime=30  # minutes
        )

        # According to the use-case, about 50 trajectories are simulated in a 
        # production run. We set the pipeline instances to 64.  
        simchain = UCL_BAC_SimChain(instances=64)

        cluster.run(simchain)

    except EnsemblemdError, er:

        print "Ensemble MD Error: {0}".format(str(er))
        raise # Just raise the execption again to get the backtrace
