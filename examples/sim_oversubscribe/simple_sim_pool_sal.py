import sys
import os
import json

from radical.ensemblemd import Kernel
from radical.ensemblemd import SimulationAnalysisLoop
from radical.ensemblemd import EnsemblemdError
from radical.ensemblemd import SingleClusterEnvironment
from radical.ensemblemd.engine import get_engine

from kernel_defs.sleep_mkfile import kernel_sleep_mkfile
get_engine().add_kernel_plugin(kernel_sleep_mkfile)

from kernel_defs.concat import kernel_concat
get_engine().add_kernel_plugin(kernel_concat)

# ------------------------------------------------------------------------------
#
class SAL_1(SimulationAnalysisLoop):

    def __init__(self, iterations, simulation_instances, analysis_instances):
        SimulationAnalysisLoop.__init__(self, iterations, simulation_instances, analysis_instances)


    def simulation_step(self, iteration, instance):
        """In the simulation step we
        """
        k = Kernel(name="custom.sleep_mkfile")
        k.arguments = ["--maxsleep=10","--upperlimit=1000", "--filename=asciifile.dat".format(instance)]
        return [k]

    def analysis_step(self, iteration, instance):

        k1 = Kernel(name="custom.concat")
        k1.arguments            = ["--format=dat","--filename=output.dat"]
        k1.download_output_data = "output.dat"

        link_input_data = []
        for i in range(1, self.simulation_instances+1):
            link_input_data.append("$PREV_SIMULATION_INSTANCE_{instance}/asciifile.dat > asciifile-{instance}.dat".format(instance=i))

        k1.link_input_data = link_input_data
        return [k1]


if __name__ == "__main__":

    try:

        # Create a new static execution context with one resource and a fixed
        # number of cores and runtime.
        cluster = SingleClusterEnvironment(
                        resource='local.localhost',
                        cores=2,
                        walltime=15,
                        
                        #username=None,
                        #project=None,
                        #queue = None,
                        #database_url='',
                        #database_name='',
        )

        # Allocate the resources.
        cluster.allocate()

        # We set both the the simulation and the analysis step 'instances' to 16.
        # If they
        sal_1 = SAL_1(iterations=1, simulation_instances=8, analysis_instances=1)
        cluster.run(sal_1)

        cluster.deallocate()

    except EnsemblemdError, er:

        print "Ensemble MD Toolkit Error: {0}".format(str(er))
        raise # Just raise the execption again to get the backtrace