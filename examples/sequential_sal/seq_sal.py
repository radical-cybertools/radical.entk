
__author__       = "Vivek <vivek.balasubramanian@rutgers.edu>"
__copyright__    = "Copyright 2016, http://radical.rutgers.edu"
__license__      = "MIT"
__example_name__ = "Sequential SAL"


import sys
import os
import json

from radical.ensemblemd import Kernel
from radical.ensemblemd import SimulationAnalysisLoop
from radical.ensemblemd import EnsemblemdError
from radical.ensemblemd import SingleClusterEnvironment

# ------------------------------------------------------------------------------
#
class SAL_1(SimulationAnalysisLoop):

    def __init__(self, iterations, simulation_instances, analysis_instances):
        SimulationAnalysisLoop.__init__(self, iterations, simulation_instances, analysis_instances)


    def simulation_step(self, iteration, instance):
        """In the simulation step we
        """
        k = Kernel(name="misc.mkfile")
        k.arguments = ["--size=1000", "--filename=asciifile-{0}.dat".format(instance)]
        k.download_output_data = ['asciifile-{0}.dat'.format(instance)]
        return [k]

    def analysis_step(self, iteration, instance):

        k1 = Kernel(name="misc.randval")
        k1.arguments            = ["--upperlimit=3", "--filename=iters.dat"]
        k1.download_output_data = "iters.dat"

        k2 = Kernel(name="misc.randval")
        k2.arguments = ["--upperlimit=16","--filename=sims.dat"]
        k2.download_output_data = "sims.dat"

        return [k1,k2]

class SAL_2(SimulationAnalysisLoop):

    def __init__(self, iterations, simulation_instances, analysis_instances):
        SimulationAnalysisLoop.__init__(self, iterations, simulation_instances, analysis_instances)


    def simulation_step(self, iteration, instance):
        """In the simulation step we
        """
        k = Kernel(name="misc.mkfile")
        k.arguments = ["--size=1000", "--filename=asciifile.dat"]
        return [k]

    def analysis_step(self, iteration, instance):

        link_input_data = []
        for i in range(1, self.simulation_instances+1):
            link_input_data.append("$PREV_SIMULATION_INSTANCE_{instance}/asciifile.dat > asciifile-{instance}.dat".format(instance=i))

        k = Kernel(name="misc.ccount")
        k.arguments            = ["--inputfile=asciifile-*.dat", "--outputfile=cfreqs.dat"]
        k.link_input_data      = link_input_data
        k.download_output_data = "cfreqs.dat > cfreqs-{iteration}.dat".format(iteration=iteration)
        return [k]


# ------------------------------------------------------------------------------
#
if __name__ == "__main__":

    try:

        # Create a new static execution context with one resource and a fixed
        # number of cores and runtime.
        cluster = SingleClusterEnvironment(
                        resource='local.localhost',
                        cores=1,
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

        while( not ((os.path.isfile('iters.dat')) and(os.path.isfile('sims.dat')))):
            continue


        f1 = open('iters.dat','r')
        new_iters = int(f1.readline().strip())
        f1.close()

        f2 = open('sims.dat','r')
        new_sims = int(f2.readline().strip())
        f2.close()

        print 'Commencing second SAL workload'
        sal_2 = SAL_2(iterations=new_iters, simulation_instances=new_sims, analysis_instances=1)
        cluster.run(sal_2)

        cluster.deallocate()

    except EnsemblemdError, er:

        print "Ensemble MD Toolkit Error: {0}".format(str(er))
        raise # Just raise the execption again to get the backtrace