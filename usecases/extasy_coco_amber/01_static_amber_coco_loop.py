#!/usr/bin/env python

""" 
Extasy project: 'Coco/Amber' STATIC simulation-analysis loop proof-of-concept (Nottingham).
"""

__author__        = "Ole Weider <ole.weidner@rutgers.edu>"
__copyright__     = "Copyright 2014, http://radical.rutgers.edu"
__license__       = "MIT"
__use_case_name__ = "Extasy project: 'Coco/Amber' STATIC simulation-analysis loop proof-of-concept (Nottingham)."


from radical.ensemblemd import Kernel
from radical.ensemblemd import EnsemblemdError
from radical.ensemblemd import SimulationAnalysisLoop
from radical.ensemblemd import SingleClusterEnvironment


# ------------------------------------------------------------------------------
#
class Extasy_CocoAmber_Static(SimulationAnalysisLoop):

    SimulationAnalysisLoop.__init__(self, maxiterations, simulation_width, analysis_width)

    def pre_loop(self):
        pass

    def simulation_step(self, iteration, instance):
        k = Kernel(name="misc.nop") 
        k.set_args(["--duration=10")
        return k

    def analysis_step(self, iteration, instance):
        k = Kernel(name="misc.nop")
        k.set_args(["--duration=10")
        return k

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
