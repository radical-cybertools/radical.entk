#!/usr/bin/env python

"""This example shows how to use EnsembleMD Toolkit to execute a simple 
   batch of N homogeneous tasks.

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
 
        # Create a new Batch object. A batch object behaves essentially like 
        # a task (and can be used in any pattern in lieu of a task).
        batch = Batch(size=16)
        proc.add_input(["file1.dat, file2.dat"])
        batch.set_kernel(Kernel(kernel="misc.mkfile", args=["--size=10000000", "--filename=asciifile.dat"])) 
        output = pre.add_output(["asciifile-%{task}.dat")


        sec.execute(pipeline)

    except EnsemblemdError, er:

        print "EnsembleMD Error: {0}".format(str(er))
        raise
