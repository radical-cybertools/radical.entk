#!/usr/bin/env python

""" This example shows how to use EnsembleMD Toolkit to execute a simple 
    pipeline of sequential batches. In the first step 'pre', 16 10 MB input 
    files are generated and filled with ASCII charaters. In the second step 
    'proc', a character frequency analysis if performed on these file. In the 
    last step 'post', an SHA1 checksum is calculated for each analysis result.

    The results of the frequency analysis and the SHA1 checksums are copied
    back to the machine on which this script executes. 

    Run this example with RADICAL_ENMD_VERBOSE set to info if you want to see 
    log messages about plug-in invocation and simulation progress:

        RADICAL_ENMD_VERBOSE=info python batch-pipeline.py
"""

__author__    = "Ole Weider <ole.weidner@rutgers.edu>"
__copyright__ = "Copyright 2014, http://radical.rutgers.edu"
__license__   = "MIT"

import sys

from radical.ensemblemd import Batch
from radical.ensemblemd import Kernel
from radical.ensemblemd import Pipeline
from radical.ensemblemd import EnsemblemdError
from radical.ensemblemd import StaticExecutionContext

BATCH_SIZE = 16

# ------------------------------------------------------------------------------
#
if __name__ == "__main__":

    try:
        # Create a new static execution context with one resource and a fixed
        # number of cores and runtime.
        sec = StaticExecutionContext(
            resource="localhost", 
            cores=1, 
            walltime=15
        )

        # Create a new batch preprocessing step: generate 16 10MB ASCII files.
        pre = Batch(size=BATCH_SIZE)
        pre.set_kernel(Kernel(kernel="misc.mkfile", args=["--size=10000000", "--filename=asciifile.dat"])) 
        pre_out = pre.add_output(filename="asciifile-%{task-id}.dat")

        # Create a new batch processing step: count the character frequencies. 
        proc = Batch(size=BATCH_SIZE)
        proc.add_input(files=pre_out, labels="pre_out")                                                 
        proc.set_kernel(Kernel(kernel="misc.ccount", args=["--inputfile=%{pre_out}", "--outputfile=cfreqs.dat"]))
        proc_out = proc.add_output(filename="cfreqs-%{task-id}.dat", download="./cfreqs.dat")

        # Create a new postprocessing step: create a checksum for the result.
        post = Batch(size=BATCH_SIZE)
        post.add_input(files=proc_out, labels="proc_out")                                              
        post.set_kernel(Kernel(kernel="misc.chksum", args=["--inputfile=%{proc_out}", "--outputfile=cfreqs.sum"]))
        post.add_output(filename="cfreqs-%{task-id}.sha1", download="./cfreqs.sha1")

        # Add all three batch steps to the pipeline for sequential execution.
        pipeline = Pipeline()
        pipeline.add_steps([pre, proc, post])

        # Run the pipeline. 
        sec.execute(pipeline)

    except EnsemblemdError, er:

        print "EnsembleMD Error: {0}".format(str(er))
        raise # Just raise the execption again to get the backtrace
