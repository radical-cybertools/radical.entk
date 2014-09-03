#!/usr/bin/env python

""" 
Extasy project: 'GROMACS/LSDMap' STATIC simulation-analysis loop proof-of-concept (Rice).
"""

__author__        = "Ole Weider <ole.weidner@rutgers.edu>"
__copyright__     = "Copyright 2014, http://radical.rutgers.edu"
__license__       = "MIT"
__use_case_name__ = "Extasy project: 'GROMACS/LSDMap' STATIC simulation-analysis loop proof-of-concept (Rice)."


from radical.ensemblemd import Kernel
from radical.ensemblemd import EnsemblemdError
from radical.ensemblemd import SimulationAnalysisLoop
from radical.ensemblemd import SingleClusterEnvironment


# ------------------------------------------------------------------------------
#
class Extasy_CocoAmber_Static(SimulationAnalysisLoop):

    def __init__(self, maxiterations, simulation_width, analysis_width):
        SimulationAnalysisLoop.__init__(self, maxiterations, simulation_width, analysis_width)

    def pre_loop(self):
        pass

    def simulation_step(self, iteration, instance):
        pass

    def analysis_step(self, iteration, instance):
        pass

    def post_loop(self):
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

        coco_amber_static = Extasy_CocoAmber_Static(maxiterations=1, simulation_width=1, analysis_width=1)
        cluster.run(coco_amber_static)

    except EnsemblemdError, er:

        print "EnsembleMD Error: {0}".format(str(er))
        raise # Just raise the execption again to get the backtrace
