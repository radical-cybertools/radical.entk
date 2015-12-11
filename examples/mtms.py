#!/usr/bin/env python

__author__       = "Vivek Balasubramanian <vivek.balasubramanian@rutgers.edu>"
__copyright__    = "Copyright 2015, http://radical.rutgers.edu"
__license__      = "MIT"
__example_name__ = "MTMS Example (generic)"

from radical.ensemblemd import Kernel
from radical.ensemblemd import MTMS
from radical.ensemblemd import EnsemblemdError
from radical.ensemblemd import SingleClusterEnvironment



# ------------------------------------------------------------------------------
#
class CalculateChecksums(MTMS):

    def __init__(self, stages, tasks):
        MTMS.__init__(self, stages, tasks)

    def stage_1(self, instance):
        k = Kernel(name="misc.mkfile")
        k.arguments = ["--size=1000000", "--filename=asciifile-{0}.dat".format(instance)]
        return k

    def stage_2(self, instance):
        k = Kernel(name="misc.ccount")
        k.arguments            = ["--inputfile=asciifile-{0}.dat".format(instance), "--outputfile=cfreqs-{0}.dat".format(instance)]
        k.copy_input_data      = "$STAGE_1/asciifile-{0}.dat".format(instance)
        k.download_output_data = "cfreqs-{0}.dat".format(instance)
        return k



# ------------------------------------------------------------------------------
#
if __name__ == "__main__":
    try:

        cluster = SingleClusterEnvironment(
                        resource="local.localhost",
                        cores=1,
                        walltime=15,
#			username='vivek91',
#			project='TG-MCB090174',
#			queue='development',
                        database_url='mongodb://ec2-54-221-194-147.compute-1.amazonaws.com:24242',
                        database_name='myexps',
        )

        # Allocate the resources.
        cluster.allocate()



        ccount = CalculateChecksums(stages=2,tasks=16)
        cluster.run(ccount)
        cluster.deallocate()

    except EnsemblemdError, er:

        print "Ensemble MD Toolkit Error: {0}".format(str(er))
        raise # Just raise the execption again to get the backtrace
