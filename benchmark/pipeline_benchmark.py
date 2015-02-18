#!/usr/bin/env python


import math

from radical.ensemblemd import Kernel
from radical.ensemblemd import SimulationAnalysisLoop
from radical.ensemblemd import EnsemblemdError
from radical.ensemblemd import Pipeline
from radical.ensemblemd import SingleClusterEnvironment

# ------------------------------------------------------------------------------
# BENCHMARK PARAMETERS
#
config = {
    "idletime":   10,
    "cores":      [1],
    "instances":  [2],
 }

# ------------------------------------------------------------------------------
#
class NopP(Pipeline):

    def __init__(self, instances, idle_time=0):
        self.idle_time = idle_time
        Pipeline.__init__(self, instances)

    def step_1(self, instance):
        k = Kernel(name="misc.idle")
        k.arguments = ["--duration={0}".format(self.idle_time)]
        return k

# ------------------------------------------------------------------------------
#
if __name__ == "__main__":

    try:
        # iterate over cores
        for cfg_core in config["cores"]:

            # iterate over instances
            for cfg_inst in config["instances"]:

                print "\n\ncores: %s instances: %s" % (cfg_core, cfg_inst)

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

                nopsa = NopP(
                    instances = cfg_inst,
                    idle_time = config["idletime"]
                )
                cluster.run(nopsa)

                print nopsa.execution_profile

    except EnsemblemdError, er:
        print "Ensemble MD Toolkit Error: {0}".format(str(er))
