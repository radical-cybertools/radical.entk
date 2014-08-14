#!/usr/bin/env python

"""This example shows how to use EnsembleMD Toolkit to execute a single 
task, including an integrated pre-processing and post-processing step.

Run this example with RADICAL_ENSEMBLEMD_VERBOSE set to info if you want to see 
log messages about plug-in invocation and simulation progress:

    RADICAL_ENSEMBLEMD_VERBOSE=info python single-task.py
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

        ##################
        # THIS IS A STEP #
        ##################
 
        pre = Preprocessing()                                               # Preprocessing (sub-)step
        pre.set_kernel(Kernel(kernel="base64 make text"))                   # base64 /dev/urandom | head -c 10000000 > file.txt
        pre_out = proc.add_output(output_filename="file.txt")               # expects the kernel to generate a file "file.txt", fails otherwise

        proc = Processing()                                                 # Processing (sub-)step
        proc.set_kernel(Kernel(kernel="do_whatever", args="-f {pre_out}"))
        proc.add_input(pre_out, label="pre_out")                            # Takes the output of the preproc step as input
        proc_out = proc.add_output(output_filename="output.txt")            # returns "Port"

        post = Postprocessing()                                             # Postprocessing (sub-)step
        post.set_kernel("mv {sim_out} output-6-6-2014.dat")
        post.add_input(proc_out, label="sim_out")
        post.output(filename="output-6-6-2014.dat")

        sec.execute(loop)

    except EnsemblemdError, er:

        print "EnsembleMD Error: {0}".format(str(er))
        sys.exit(1)
