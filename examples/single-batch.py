#!/usr/bin/env python

""" This example shows how to use EnsembleMD Toolkit to execute a single 
    batch of tasks. 

    Run this example with RADICAL_ENMD_VERBOSE set to info if you want to see 
    log messages about plug-in invocation and simulation progress:

        RADICAL_ENMD_VERBOSE=info python single-batch.py
"""

__author__    = "Ole Weider <ole.weidner@rutgers.edu>"
__copyright__ = "Copyright 2014, http://radical.rutgers.edu"
__license__   = "MIT"

import sys

from radical.ensemblemd import Batch
from radical.ensemblemd import Kernel
from radical.ensemblemd import EnsemblemdError
from radical.ensemblemd import StaticExecutionContext

# ------------------------------------------------------------------------------
#
if __name__ == "__main__":

    try:
        # Create a new static execution context with one resource and a fixed
        # number of cores and runtime.
        sec = StaticExecutionContext()
 
        # Create a new preprocessing step: generate a 10MB ASCII file.
        batch = Batch(size=16)
        batch.set_kernel(Kernel(kernel="misc.mkfile", args=["--size=10000000", "--filename=asciifile.dat"])) 
        #pre_out = pre.add_output(filename="asciifile.dat")

        # A batch can be passed directly to an execution context. 
        sec.execute(batch)

    except EnsemblemdError, er:

        print "EnsembleMD Error: {0}".format(str(er))
        raise # Just raise the execption again to get the backtrace
