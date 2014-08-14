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

from radical.ensemblemd import Task
from radical.ensemblemd import Kernel
from radical.ensemblemd import Subtask
from radical.ensemblemd import EnsemblemdError
from radical.ensemblemd import StaticExecutionContext

# ------------------------------------------------------------------------------
#
if __name__ == "__main__":

    try:
        # Create a new static execution context with one resource and a fixed
        # number of cores and runtime.
        sec = StaticExecutionContext()

        # Create a new, emtpy task instance.
        task = Task()
 
        # Create a new preprocessing operation.
        pre = Subtask()
        pre.set_kernel(Kernel(kernel="util.mkfile", args=["10M"]))          # base64 /dev/urandom | head -c 10000000 > file.txt
        pre_out = pre.add_output(filename="file.txt")                              # expects the kernel to generate a file "file.txt", fails otherwise

        # Create a new processing operation.
        proc = Subtask()                                                     
        proc.set_kernel(Kernel(kernel="do_whatever", args=["-f {pre_out}"]))
        proc.add_input(pre_out, label="pre_out")                                   # Takes the output of the preproc step as input
        proc_out = proc.add_output(filename="output.txt")                          # returns "Port"

        # Create a new postprocessing operation.
        post = Subtask()                                                  
        post.set_kernel(Kernel(kernel="util.move", args=["{sim_out}", "output-6-6-2014.dat"]))
        post.add_input(proc_out, label="sim_out")
        post.add_output(filename="output-6-6-2014.dat")

        # Add the three individual steps to the task.
        task.set_preprocessing_subtask(pre)
        task.set_processing_subtask(proc)
        task.set_postprocessing_subtask(post)

        sec.execute(task)

    except EnsemblemdError, er:

        print "EnsembleMD Error: {0}".format(str(er))
        sys.exit(1)
