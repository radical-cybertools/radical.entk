#!/usr/bin/env python


import math
import pprint

from radical.ensemblemd import Kernel
from radical.ensemblemd import SimulationAnalysisLoop
from radical.ensemblemd import EnsemblemdError
from radical.ensemblemd import SimulationAnalysisLoop
from radical.ensemblemd import SingleClusterEnvironment

# ------------------------------------------------------------------------------
# BENCHMARK PARAMETERS
#
config = {
    "idletime":   10,
    "cores":      [1],
    "instances":  [1],
    "iterations": [2]
 }

# ------------------------------------------------------------------------------
#
class NopSA(SimulationAnalysisLoop):

    def __init__(self, maxiterations, simulation_instances=1, analysis_instances=1, idle_time=0):
        self.idle_time = idle_time
        SimulationAnalysisLoop.__init__(self, maxiterations, simulation_instances, analysis_instances)

    def pre_loop(self):
        pass

    def simulation_step(self, iteration, instance):
        k = Kernel(name="misc.idle")
        k.arguments = ["--duration={0}".format(self.idle_time)]
        k.upload_input_data = "/etc/passwd"
        #k.download_output_data = "/etc/passwd"
        return k

    def analysis_step(self, iteration, instance):
        k = Kernel(name="misc.idle")
        k.arguments = ["--duration={0}".format(self.idle_time)]
        k.upload_input_data = "/etc/passwd"
        #k.download_output_data = "/etc/passwd"
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
                        analysis_instances=cfg_inst,
                        idle_time = config["idletime"]
                    )
                    cluster.run(nopsa)

                    pp = pprint.PrettyPrinter()
                    pp.pprint(nopsa.execution_profile_dict)

                    print nopsa.execution_profile_dataframe

    except EnsemblemdError, er:
        print "Ensemble MD Toolkit Error: {0}".format(str(er))
