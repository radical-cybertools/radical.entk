#!/usr/bin/env python

"""This example shows how to use EnsembleMD Toolkit to execute a simple 
   pipeline of sequential tasks. In the first step 'pre', a 10 MB input file
   is generated and filled with ASCII charaters. In the second step 'proc',
   a character frequency analysis if performed on this file. In the last step
   'post', an SH1 checksum is calculated for the result.

Run this example with RADICAL_ENMD_VERBOSE set to info if you want to see 
log messages about plug-in invocation and simulation progress:

    RADICAL_ENMD_VERBOSE=info python single-pipeline.py
"""

__author__    = "Ole Weider <ole.weidner@rutgers.edu>"
__copyright__ = "Copyright 2014, http://radical.rutgers.edu"
__license__   = "MIT"

import sys

from radical.ensemblemd import Task
from radical.ensemblemd import Kernel
from radical.ensemblemd import Pipeline
from radical.ensemblemd import EnsemblemdError
from radical.ensemblemd import StaticExecutionContext

# ------------------------------------------------------------------------------
#
if __name__ == "__main__":

    try:
        # Create a new static execution context with one resource and a fixed
        # number of cores and runtime.
        sec = StaticExecutionContext()
 
        # Create a new File object referencing a local file.
        data = File("/etc/passwd")

        # Create a new preprocessing step: generate a 10MB ASCII file.
        pre = Task()
        pre.set_input(input_data, label="input_data")
        pre.set_kernel(Kernel(kernel="misc.chksum", args=["--inputfile=%{input_data}", "--filename=CHECKSUM"])) 

        # Create a new task instance and add the three subprocesses.
        pipeline = Pipeline()
        pipeline.add_steps([pre, proc, post])

        sec.execute(pipeline)

    except EnsemblemdError, er:

        print "EnsembleMD Error: {0}".format(str(er))
        raise # Just raise the execption again to get the backtrace
