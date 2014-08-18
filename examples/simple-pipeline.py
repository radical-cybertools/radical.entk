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
 
        # Create a new preprocessing step: generate a 10MB ASCII file.
        pre = Task()
        pre.set_kernel(Kernel(kernel="misc.mkfile", args=["--size=10000000", "--filename=asciifile.dat"])) 
        pre.assert_output(filename="asciifile.dat")

        # Create a new processing step: count the character frequencies. 
        proc = Task()                                                     
        proc.set_kernel(Kernel(kernel="misc.ccount", args=["--inputfile=asciifile.dat", "--outputfile=cfreqs.dat"]))
        proc.assert_output(filename="cfreqs.dat")

        # Create a new postprocessing step: create a checksum for the result.
        post = Task()                                                  
        post.set_kernel(Kernel(kernel="misc.chksum", args=["--inputfile=cfreqs.dat", "--outputfile=cfreqs.sum"]))
        post.assert_output(filename="cfreqs.sha1")

        # Create a new task instance and add the three subprocesses.
        pipeline = Pipeline()
        pipeline.add_steps([pre, proc, post])

        sec.execute(pipeline)

    except EnsemblemdError, er:

        print "EnsembleMD Error: {0}".format(str(er))
        raise
