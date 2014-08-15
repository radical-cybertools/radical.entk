#!/usr/bin/env python

"""This example shows how to use EnsembleMD Toolkit to execute a single 
task, including an integrated pre-processing and post-processing step.

Run this example with RADICAL_ENMD_VERBOSE set to info if you want to see 
log messages about plug-in invocation and simulation progress:

    RADICAL_ENMD_VERBOSE=info python single-task.py
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
 
        # Create a new preprocessing operation.
        pre = Subtask()
        pre.set_kernel(Kernel(kernel="misc.mkfile", args=["--size=10000000", "--filename=asciifile.dat"])) 
        pre_out = pre.add_output(filename="asciifile.dat")

        # Create a new processing operation.
        proc = Subtask()                                                     
        proc.add_input(pre_out, label="pre_out")                                   
        proc.set_kernel(Kernel(kernel="misc.ccount", args=["--inputfile={pre_out}", "--outputfile=cfreqs.dat"]))
        proc_out = proc.add_output(filename="output.txt")                        

        # Create a new postprocessing operation.
        #post = Subtask()                                                  
        #post.set_kernel(Kernel(kernel="util.move", args=["{sim_out}", "output-6-6-2014.dat"]))
        #post.add_input(proc_out, label="sim_out")
        #post.add_output(filename="output-6-6-2014.dat")

        # Create a new task instance and add the three subprocesses.
        task = Task(preprocessing=pre, processing=proc)#, postprocessing=post)

        sec.execute(task)

    except EnsemblemdError, er:

        print "EnsembleMD Error: {0}".format(str(er))
        raise
        sys.exit(1)
