#!/usr/bin/env python



__author__       = "Ole Weider <ole.weidner@rutgers.edu>"
__copyright__    = "Copyright 2014, http://radical.rutgers.edu"
__license__      = "MIT"
__example_name__ = "Bag of Tasks Example (generic)"

import sys
import os
import json

from radical.ensemblemd import Kernel
from radical.ensemblemd import BagofTasks
from radical.ensemblemd import EnsemblemdError
from radical.ensemblemd import SingleClusterEnvironment


# ------------------------------------------------------------------------------
#
class CalculateChecksums(BagofTasks):
    """The CalculateChecksums class implements a Bag of Tasks. Since there
        is no explicit "Bag of Tasks" pattern template, we inherit from the
        radical.ensemblemd.Pipeline pattern and define just one step.
    """

    def __init__(self, steps, instances):
        BagofTasks.__init__(self, steps, instances)

    def step_1(self, instance):
        """This step downloads a sample UTF-8 file from a remote websever and
           calculates the SHA1 checksum of that file. The checksum is written
           to an output file and tranferred back to the host running this
           script.
        """
        k = Kernel(name="misc.chksum")
        k.arguments            = ["--inputfile=UTF-8-demo.txt", "--outputfile=checksum{0}.sha1".format(instance)]
        k.upload_input_data  = "UTF-8-demo.txt"
        k.download_output_data = "checksum{0}.sha1".format(instance)
        k.post_exec = ['if [ -f "checksu{0}.sha1" ]; then exit 0; else echo "File checksum{0}.sha1 does not exist" >&2; exit 1; fi'.format(instance)]
		
        return k


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
                        #username = None,
                        #project = None,
                        #queue = None,

        #                database_url='mongodb://ec2-54-221-194-147.compute-1.amazonaws.com:24242',
        #                database_name='myexps',
        )

        # Allocate the resources.
        cluster.allocate()

        # Set the 'instances' of the pipeline to 16. This means that 16 instances
        # of each pipeline step are executed.
        #
        # Execution of the 16 pipeline instances can happen concurrently or
        # sequentially, depending on the resources (cores) available in the
        # SingleClusterEnvironment.
        ccount = CalculateChecksums(steps=1,instances=16)

        os.system('wget -q -o UTF-8-demo.txt http://gist.githubusercontent.com/oleweidner/6084b9d56b04389717b9/raw/611dd0c184be5f35d75f876b13604c86c470872f/gistfile1.txt')

        cluster.run(ccount)

        # Print the checksums
        print "\nResulting checksums:"
        import glob
        for result in glob.glob("checksum*.sha1"):
            print "  * {0}".format(open(result, "r").readline().strip())

        cluster.deallocate()

    except EnsemblemdError, er:

        print "Ensemble MD Toolkit Error: {0}".format(str(er))
        raise # Just raise the execption again to get the backtrace
