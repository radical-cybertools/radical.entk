#!/usr/bin/env python

""" 
This example shows how to use the EnsembleMD Toolkit ``Pipeline`` pattern
to execute a single "bag of tasks". A bag of tasks is modeled as a pipeline 
with just one step. The "width" of the pipeline corresponds to the number of 
tasks in the bag.

Run this example with ``RADICAL_ENMD_VERBOSE`` set to ``info`` if you want to 
see log messages about plug-in invocation and simulation progress::

    RADICAL_ENMD_VERBOSE=info python bag_of_tasks.py
"""

__author__       = "Ole Weider <ole.weidner@rutgers.edu>"
__copyright__    = "Copyright 2014, http://radical.rutgers.edu"
__license__      = "MIT"
__example_name__ = "A Pipeline Example"


from radical.ensemblemd import Kernel
from radical.ensemblemd import Pipeline
from radical.ensemblemd import EnsemblemdError
from radical.ensemblemd import SingleClusterEnvironment


# ------------------------------------------------------------------------------
#
class CalculateChecksums(Pipeline):
    """The CalculateChecksums class implements a bag of task. Since there is 
        no explicit "bag of tasks" pattern template, we inherit from the
        radical.ensemblemd.Pipeline pattern and define just one step. 
    """ 

    def __init__(self, width):
        Pipeline.__init__(self, width)

    def step_1(self, instance):
        """This step downloads a sample UTF-8 file from a remote websever and 
           calculates the SHA1 checksum of that file. The checksum is written
           to an output file and tranferred back to the host running this 
           script.
        """
        k = Kernel(name="misc.chksum")
        k.arguments            = ["--inputfile=UTF-8-demo.txt", "--outputfile=checksum{0}.sha1".format(instance)]
        k.download_input_data  = "http://www.cl.cam.ac.uk/~mgk25/ucs/examples/UTF-8-demo.txt"
        k.download_output_data = "checksum{0}.sha1".format(instance)
        return k


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

        # Set the 'width' of the pipeline to 32. This means that 32 instances
        # of each pipeline step are executed. 
        # 
        # Execution of the 32 pipeline instances can happen concurrently or 
        # sequentially, depending on the resources (cores) available in the 
        # SingleClusterEnvironment. 
        ccount = CalculateChecksums(width=32)

        cluster.run(ccount)

    except EnsemblemdError, er:

        print "EnsembleMD Error: {0}".format(str(er))
        raise # Just raise the execption again to get the backtrace
