#!/usr/bin/env python


import math

from radical.ensemblemd import Kernel
from radical.ensemblemd import SimulationAnalysisLoop
from radical.ensemblemd import EnsemblemdError
from radical.ensemblemd import SimulationAnalysisLoop
from radical.ensemblemd import SingleClusterEnvironment

config = {
    "cores": [1,2,4,8],
    "instances": [1,2,4,8,16,32,64],
    "iterations": [2,4,8]
 }

# ------------------------------------------------------------------------------
#
class NopSA(SimulationAnalysisLoop):

    def __init__(self, maxiterations, simulation_instances=1, analysis_instances=1):
        SimulationAnalysisLoop.__init__(self, maxiterations, simulation_instances, analysis_instances)

    def pre_loop(self):
        pass

    def simulation_step(self, iteration, instance):
        k = Kernel(name="misc.nop")
        return k

    def analysis_step(self, iteration, instance):
        k = Kernel(name="misc.nop")
        return k

    def post_loop(self):
        pass


# ------------------------------------------------------------------------------
#
if __name__ == "__main__":

    try:
        # iterate over cores
        for cfg_core in config["cores"]:

            # iterate over instances
            for cfg_inst in config["instances"]:

                for cfg_iter in config["iterations"]:

                    print "\n\ncores: %s instances: %s iterations: %s" % (cfg_core, cfg_inst, cfg_iter)

                    cluster = SingleClusterEnvironment(
                        resource="localhost",
                        cores=cfg_core,
                        walltime=30,
                        username=None,
                        allocation=None
                    )
                    # wait=True waits for the pilot to become active
                    # before the call returns. This is not useful when
                    # you want to take advantage of the queueing time /
                    # file-transfer overlap, but it's useful for baseline
                    # performance profiling of a specific pattern.
                    cluster.allocate(wait=True)

                    nopsa = NopSA(
                        maxiterations=cfg_iter,
                        simulation_instances=cfg_inst,
                        analysis_instances=cfg_inst
                    )
                    cluster.run(nopsa)

                    print nopsa.execution_profile

    except EnsemblemdError, er:
        print "Ensemble MD Toolkit Error: {0}".format(str(er))
