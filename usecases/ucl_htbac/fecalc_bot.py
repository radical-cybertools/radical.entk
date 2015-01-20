#!/usr/bin/env python

""" 
'HT-BAC free energy calculation' bag-of-task proof-of-concept (UCL).
"""

__author__        = "Ole Weider <ole.weidner@rutgers.edu>"
__copyright__     = "Copyright 2014, http://radical.rutgers.edu"
__license__       = "MIT"
__use_case_name__ = "'HT-BAC free energy calculation' bag-of-task proof-of-concept (UCL)."


from radical.ensemblemd import Kernel
from radical.ensemblemd import Pipeline
from radical.ensemblemd import EnsemblemdError
from radical.ensemblemd import SimulationAnalysisLoop
from radical.ensemblemd import SingleClusterEnvironment


# ------------------------------------------------------------------------------
#
class UCL_BAC_FreeEnergy(Pipeline):

    def __init__(self, instances):
        Pipeline.__init__(self, instances)

    def step_01(self, instance):
        """This steps does the trajectory analysis, a.k.a free energy 
           calculation. First, the input files are downloaded from a remote 
           HTTP server. Next, MMPBSA reads the 'traj' ("trajectory") file and 
           carries out the analysis. Finally, the output is transferred back to 
           the machine on which this script is executed. 
        """
        mmpbsa = Kernel(name="md.mmpbsa")
        mmpbsa.core                 = 4
        mmpbsa.arguments            = ["-i nmode.5h.py", "-cp com.top.2", "-rp rec.top.2", "-lp lig.top", "-y rep{0}.traj".format(instance)]
        mmpbsa.download_input_data  = ["http://testing.saga-project.org/cybertools/sampledata/BAC-MMBPSA/com.top.2 > com.top.2",
                                       "http://testing.saga-project.org/cybertools/sampledata/BAC-MMBPSA/rec.top.2 > rec.top.2",
                                       "http://testing.saga-project.org/cybertools/sampledata/BAC-MMBPSA/lig.top > lig.top",
                                       "http://testing.saga-project.org/cybertools/sampledata/BAC-MMBPSA/nmode.5h.py > nmode.5h.py",
                                       "http://testing.saga-project.org/cybertools/sampledata/BAC-MMBPSA/trajectories/rep1.traj > rep{0}.traj".format(instance)]
        mmpbsa.download_output_data =  "STDOUT > eq{0}.out".format(instance)
        return mmpbsa

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
        # production run, so 50 analysis runs have to be performed.
        # We set the pipeline instances to 64.  
        freenrg = UCL_BAC_FreeEnergy(instances=64)

        cluster.run(freenrg)

    except EnsemblemdError, er:

        print "Ensemble MD Error: {0}".format(str(er))
        raise # Just raise the execption again to get the backtrace
