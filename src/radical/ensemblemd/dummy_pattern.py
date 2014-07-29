#!/usr/bin/env python

"""TODO: Docstring.
"""

__author__    = "Ole Weider <ole.weidner@rutgers.edu>"
__copyright__ = "Copyright 2014, http://radical.rutgers.edu"
__license__   = "MIT"

from radical.ensemblemd.execution_pattern import ExecutionPattern

PATTERN_NAME = "dummy"


#-------------------------------------------------------------------------------
#
class DummyPattern(ExecutionPattern):
    """DummyPattern provides an execution pattern that is solely used for 
    internal purposes by the ensemblemd testing framework. 
    """

    #-------------------------------------------------------------------------------
    #
    def __init__(self):
        """Creates a new DummyPattern instance.
        """
        super(DummyPattern, self).__init__()


    #-------------------------------------------------------------------------------
    #
    def get_name(self):
        """Implements base class ExecutionPattern.get_name().
        """
        return PATTERN_NAME
