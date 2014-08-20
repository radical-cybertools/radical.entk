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

from radical.ensemblemd import File
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
        sec = StaticExecutionContext(
            resource="localhost", 
            cores=1, 
            walltime=15
        )

        # These are the shared files required for all tasks in the batch. The 
        # 'from_local_path' class method denotes that the file is on the local
        # machine and tells EnMD that it eventually needs to be uploaded.
        nmode = File.from_local_path("MMBPSA/nmode.5h.py")
        com   = File.from_local_path("MMBPSA/com.top.2")
        rec   = File.from_local_path("MMBPSA/rec.top.2")
        lig   = File.from_local_path("MMBPSA/lig.top")

        # In 'trajectories' we list the files that we want to be processed  and 
        # analyzed by the individual batch tasks.
        trajectories = list()
        for traj_no in range(0, 16):
            filename = "MMBPSA/trajectories/rep-{0}.traj".format(traj_no)
            trajectories.append(File.from_local_path(filename))
 
        # Create the batch via the 'from_input_files()' class method, which
        # creates a set of tasks based on the provided list of input files. For 
        # each file in 'files' a new task is created. The files are referenced 
        # via the provided 'label' (see kernel aruments).
        batch = Batch.from_input_files(files=trajectories, label="trajectory")
        batch.add_input(files=[nmode, com, rec, lig], labels=["nmode", "com", "rec", "lig"])

        batch.set_kernel(Kernel(kernel="md.mmpbsa", args=["-i %{nmode} -cp %{com} -rp %{rec} -lp %{lig} -y %{trajectory}"])) 

        # A batch can be passed directly to an execution context.
        sec.execute(batch)

    except EnsemblemdError, er:

        print "EnsembleMD Error: {0}".format(str(er))
        raise # Just raise the execption again to get the backtrace
