#!/usr/bin/env python

""" 
This example shows how to use the EnsembleMD Toolkit ``Pipeline`` pattern
to execute 16 concurrent pipeline of sequential tasks. In the first step of 
each pipeline ``step_1``, a 10 MB input file is generated and filled with 
ASCII charaters. In the second step ``step_2``, a character frequency analysis 
if performed on this file. In the last step ``step_3``, an SHA1 checksum is 
calculated for the analysis result.

The results of the frequency analysis and the SHA1 checksums are copied
back to the machine on which this script executes. 

Run this example with ``RADICAL_ENMD_VERBOSE`` set to ``info`` if you want to 
see log messages about plug-in invocation and simulation progress::

    RADICAL_ENMD_VERBOSE=info python pipeline.py
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
class CharCount(Pipeline):
    """The CharCount class implements a three-step pipeline. It inherits from 
        radical.ensemblemd.Pipeline, the abstract base class for all pipelines.
    """

    def __init__(self, instances):
        Pipeline.__init__(self, instances)

    def step_1(self, instance):
        """The first step of the pipeline creates a 10 MB ASCI file.
        """
        k = Kernel(name="misc.mkfile")
        k.arguments = ["--size=10000000", "--filename=asciifile-{0}.dat".format(instance)]
        return k

    def step_2(self, instance):
        """The second step of the pipeline does a character frequency analysis
           on the file generated the first step. The result is transferred back
           to the host running this script.

           ..note:: The variable ``$STEP_1`` used in ``link_input_data`` is 
                    a reference to the working directory of step 1. ``$STEP_``
                    can be used analogous to refernce other steps, i.e., ``$STEP_X``.
        """
        k = Kernel(name="misc.ccount")
        k.arguments            = ["--inputfile=asciifile-{0}.dat".format(instance), "--outputfile=cfreqs-{0}.dat".format(instance)]
        k.link_input_data      = "$STEP_1/asciifile-{0}.dat".format(instance)
        k.download_output_data = "cfreqs-{0}.dat".format(instance)
        return k

    def step_3(self, instance):
        """The third step of the pipeline creates a checksum of the output file
           of the second step. The result is transferred back to the host 
           running this script.
        """
        k = Kernel(name="misc.chksum")
        k.arguments            = ["--inputfile=cfreqs-{0}.dat".format(instance), "--outputfile=cfreqs-{0}.sum".format(instance)]
        k.link_input_data      = "$STEP_2/cfreqs-{0}.dat".format(instance)
        k.download_output_data = "cfreqs-{0}.sum".format(instance)
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
            walltime=30
        )

        # Set the 'instances' of the pipeline to 16. This means that 16 instances
        # of each pipeline step are executed. 
        # 
        # Execution of the 16 pipeline instances can happen concurrently or 
        # sequentially, depending on the resources (cores) available in the 
        # SingleClusterEnvironment. 
        ccount = CharCount(instances=1)

        cluster.run(ccount)

    except EnsemblemdError, er:

        print "EnsembleMD Error: {0}".format(str(er))
        raise # Just raise the execption again to get the backtrace
