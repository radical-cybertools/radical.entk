#!/usr/bin/env python

""" 
Extasy project: 'Coco/Amber' DYNAMIC simulation-analysis loop proof-of-concept (Nottingham).
"""

__author__        = "Ole Weider <ole.weidner@rutgers.edu>"
__copyright__     = "Copyright 2014, http://radical.rutgers.edu"
__license__       = "MIT"
__use_case_name__ = "Extasy project: 'Coco/Amber' DYNAMIC simulation-analysis loop proof-of-concept (Nottingham)."


from radical.ensemblemd import Kernel
from radical.ensemblemd import EnsemblemdError
from radical.ensemblemd import SingleClusterEnvironment


# ------------------------------------------------------------------------------
#
if __name__ == "__main__":

    try:
        print "NOT IMPLEMENTED"

    except EnsemblemdError, er:

        print "EnsembleMD Error: {0}".format(str(er))
        raise # Just raise the execption again to get the backtrace
