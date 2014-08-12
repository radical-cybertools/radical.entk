#!/usr/bin/env python

"""This example shows how to use the SimulationAnalysisPattern with a fixed 
(static) number of loop iterations.

Run this example with RADICAL_ENSEMBLEMD_VERBOSE set to info if you want to see 
log messages about plug-in invocation and simulation progress:

    RADICAL_ENSEMBLEMD_VERBOSE=info python simulation-analysis-loop-static.py
"""

__author__    = "Ole Weider <ole.weidner@rutgers.edu>"
__copyright__ = "Copyright 2014, http://radical.rutgers.edu"
__license__   = "MIT"

import sys

from radical.ensemblemd import StaticExecutionContext
from radical.ensemblemd import SimulationAnalysisPattern

# ------------------------------------------------------------------------------
#
if __name__ == "__main__":

    try:
        # Create a new static execution context with one resource and a fixed
        # number of cores and runtime.
        sec = StaticExecutionContext()

        # Instantiate a new Simulation-Analysis pattern 
        loop = SimulationAnalysisPattern()

    except EnsemblemdError, er:

        print "Exception in EnsembleMD: {0}".format(str(er))
        sys.exit(1)
