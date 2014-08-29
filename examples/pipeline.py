#!/usr/bin/env python

""" 
This example shows how to use the EnsembleMD Toolkit ``Pipeline`` pattern
to execute 16 concurrent pipeline of sequential tasks. In the first step of 
each pipeline ``step_01``, a 10 MB input file is generated and filled with 
ASCII charaters. In the second step ``step_02``, a character frequency analysis 
if performed on this file. In the last step ``step_03``, an SHA1 checksum is 
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
    # 'CharCount' implements the three-step pipeline described above. It
    # inherits from radical.ensemblemd.Pipeline, the abstract base class 
    # for all pipelines. 

    def __init__(self, width):
        Pipeline.__init__(self, width)

    def step_01(self, instance):
        k = Kernel(name="misc.mkfile")
        k.set_args(["--size=10000000", "--filename=asciifile-%{0}.dat" % instance])
        return k

    def step_02(self, instance):
        k = Kernel(name="misc.ccount")
        k.set_args(["--inputfile=asciifile-%{0}.dat", "--outputfile=cfreqs-%{0}.dat" % instance])
        k.set_download_output(files="cfreqs-%{0}.dat")
        return k

    def step_03(self, instance):
        k = Kernel(name="misc.ccount")
        k.set_args(["--inputfile=cfreqs-%{0}.dat", "--outputfile=cfreqs-%{0}.sum" % instance])
        k.set_download_output(files="cfreqs-%{0}.sum")
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

        # Set the 'width' of the pipeline to 16. This means that 16 instances
        # of the pipeline are executed. 
        # Execution of the 16 pipeline instances can happen concurrently or 
        # sequentially, depending on the resources (cores) available in the 
        # SingleClusterEnvironment. 
        ccount = CharCount(width=16)

        cluster.run(ccount)

    except EnsemblemdError, er:

        print "EnsembleMD Error: {0}".format(str(er))
        raise # Just raise the execption again to get the backtrace
