#!/usr/bin/env python

"""TODO: Docstring.
"""

__author__    = "Ole Weider <ole.weidner@rutgers.edu>"
__copyright__ = "Copyright 2014, http://radical.rutgers.edu"
__license__   = "MIT"

from radical.ensemblemd.execution_pattern import ExecutionPattern

PATTERN_NAME = "SimulationAnalysis"


#-------------------------------------------------------------------------------
#
class SimulationAnalysisPattern(ExecutionPattern):
    """SimulationAnalysisPattern provides an execution pattern that allows the 
    implementation of static (fixed) and dynamic simulation-analysis loops.
    """

    #-------------------------------------------------------------------------------
    #
    def __init__(self):
        """Creates a new SimulationAnalysisPattern instance.
        """
        super(SimulationAnalysisPattern, self).__init__()


    #-------------------------------------------------------------------------------
    #
    def get_name(self):
        """Implements base class ExecutionPattern.get_name().
        """
        return PATTERN_NAME
