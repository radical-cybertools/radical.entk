#!/usr/bin/env python

"""This example shows how to use the SimulationAnalysisPattern with a fixed 
(static) number of three iterations.

Run this example with RADICAL_ENSEMBLEMD_VERBOSE set to info if you want to see 
log messages about plug-in invocation and simulation progress:

    RADICAL_ENSEMBLEMD_VERBOSE=info python simulation-analysis-loop-static.py
"""

__author__    = "Ole Weider <ole.weidner@rutgers.edu>"
__copyright__ = "Copyright 2014, http://radical.rutgers.edu"
__license__   = "MIT"

import sys

from radical.ensemblemd import EnsemblemdError
from radical.ensemblemd import StaticExecutionContext
from radical.ensemblemd import SimulationAnalysisPattern

# ------------------------------------------------------------------------------
#
if __name__ == "__main__":

    try:
        # Create a new static execution context with one resource and a fixed
        # number of cores and runtime.
        sec = StaticExecutionContext()

        # Instantiate a new simulation-analysis pattern 
        loop = SimulationAnalysisPattern()

        pre = Preprocessing()
        pre.set_kernel("base64 make text")                # base64 /dev/urandom | head -c 10000000 > file.txt
        pre_out = proc.add_output("file.txt")


        proc = Processing()
        proc.add_input(pre_out, label="pre_out")          # Takes the output of the preproc step as input
        proc_log = proc.add_output(filename="log.dat")    # returns "Port"
        proc_out = proc.add_output(filename="output.dat") # returns "Port"

        post = Postprocessing()
        post.input(proc_out)
        post_out = post.output(filename="output_processed.dat")

        # s1 = loop.add_simulation_stage("S1", after=None)
        # a1 = loop.add_analysis_stage("A1", after=s1)
        # s2 = loop.add_simulation_stage("S2", after=a2)
        # a2 = loop.add_analysis_stage("A2", after=s2)
        # s3 = loop.add_simulation_stage("S3", after=a2)
        # a3 = loop.add_analysis_stage("A3", after=s2)

        # Pass the simulation-analysis pattern definition to the static 
        # execution context for execution.
        sec.execute(loop)

    except EnsemblemdError, er:

        print "EnsembleMD Error: {0}".format(str(er))
        sys.exit(1)
