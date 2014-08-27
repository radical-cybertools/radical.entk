#!/usr/bin/env python

"""
This example shows how to use EnsembleMD Toolkit to execute a single 
ensemble of tasks.

You can find a gzipped archive (~ 20 MB) with the input data for this example at 
http://testing.saga-project.org/cybertools/sampledata/BAC-MMBPSA/mmpbsa-sample-data.tgz

Run this example with ``RADICAL_ENMD_VERBOSE`` set to ``info`` if you want to 
see  log messages about plug-in invocation and simulation progress::

    RADICAL_ENMD_VERBOSE=info python single_ensemble.py
"""

__author__       = "Ole Weider <ole.weidner@rutgers.edu>"
__copyright__    = "Copyright 2014, http://radical.rutgers.edu"
__license__      = "MIT"
__example_name__ = "A Single Ensemble"

import sys

from radical.ensemblemd import File
from radical.ensemblemd import Ensemble
from radical.ensemblemd import Kernel
from radical.ensemblemd import EnsemblemdError
from radical.ensemblemd import SingleClusterEnvironment

# ------------------------------------------------------------------------------
#
if __name__ == "__main__":

    try:
        # Create a new static execution context with one resource and a fixed
        # number of cores and runtime.
        sec = SingleClusterEnvironment(
            resource="localhost", 
            cores=1, 
            walltime=15
        )

        # These are the shared files required for all tasks in the ensemble. The 
        # 'from_local_path' class method denotes that the file is on the local
        # machine and tells EnMD that it eventually needs to be uploaded.
        nmode = File.from_local_path("./mmpbsa-sample-data/nmode.5h.py")
        com   = File.from_local_path("./mmpbsa-sample-data/com.top.2")
        rec   = File.from_local_path("./mmpbsa-sample-data/rec.top.2")
        lig   = File.from_local_path("./mmpbsa-sample-data/lig.top")

        # In 'trajectories' we list the files that we want to be processed  and 
        # analyzed by the individual ensemble tasks.
        trajectories = list()
        for traj_no in range(0, 6):
            filename = "./mmpbsa-sample-data/trajectories/rep{0}.traj".format(traj_no)
            trajectories.append(File.from_local_path(filename))
 
        # Create the ensemble via the 'from_input_files()' class method, which
        # creates a set of tasks based on the provided list of input files. For 
        # each file in 'files' a new task is created. The files are referenced 
        # via the provided 'labels' (see kernel aruments).
        ensemble = Ensemble.from_input_files(files=trajectories, label="trajectory")
        ensemble.add_input(files=[nmode, com, rec, lig], labels=["nmode", "com", "rec", "lig"])
        ensemble.set_kernel(Kernel(kernel="md.mmpbsa", args=["-i %{nmode}", "-cp %{com}", "-rp %{rec}", "-lp %{lig}", "-y %{trajectory}"])) 

        # A ensemble can be passed directly to an execution context, just like
        # any other pattern.
        sec.execute(ensemble)

    except EnsemblemdError, er:

        print "EnsembleMD Error: {0}".format(str(er))
        raise # Just raise the execption again to get the backtrace
