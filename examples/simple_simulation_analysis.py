#!/usr/bin/env python

""" 
This example shows how to use the EnsembleMD Toolkit ``SimulationAnalysis`` 
pattern to execute up to 64 iterations of a simulation analysis loop. Each
``simulation_step`` generates 16 files with a random value between [0..100]. 
The ``analysis_step`` checks whether any of the values equals 42.

Run this example with ``RADICAL_ENMD_VERBOSE`` set to ``info`` if you want to 
see log messages about plug-in invocation and simulation progress::

    RADICAL_ENMD_VERBOSE=info python simple_pipeline.py
"""

__author__       = "Ole Weider <ole.weidner@rutgers.edu>"
__copyright__    = "Copyright 2014, http://radical.rutgers.edu"
__license__      = "MIT"
__example_name__ = "A Pipeline of Tasks"


from radical.ensemblemd import Kernel
from radical.ensemblemd import Pipeline
from radical.ensemblemd import EnsemblemdError
from radical.ensemblemd import SingleClusterEnvironment


# ------------------------------------------------------------------------------
#
class RandomSA(Pipeline):
    # 'CharCount' implements the three-step pipeline described above. It
    # inherits from radical.ensemblemd.Pipeline, the abstract base class 
    # for all pipelines. 

    def __init__(self, maxloops, simulation_width=1, analysis_width=1):
        Pipeline.__init__(self, width)

    def simulation_step(self, iteration, instance):
        k = Kernel(name="misc.randval") 
        k.set_args(["--upperlimit=100", "--outputfile=simulation-step%{0}-%{1}.dat".format(iteration, instance)])
        return k

    def analysis_step(self, iteration, instance):
        k = Kernel(name="misc.checkvalue")
        k.set_args(["--inputfile=simulation-%{0}.dat", "--outputfile=cfreqs-%{0}.dat" % column])
        k.set_download_output(files="cfreqs-%{0}.dat")
        return k

    def step_03(self, column):
        k = Kernel(name="misc.ccount")
        k.set_args(["--inputfile=cfreqs-%{0}.dat", "--outputfile=cfreqs-%{0}.sum" % column])
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
        randomsa = RandomSA(simulation_width=16, analysis_width=1)

        cluster.run(randomsa)

    except EnsemblemdError, er:

        print "EnsembleMD Error: {0}".format(str(er))
        raise # Just raise the execption again to get the backtrace
