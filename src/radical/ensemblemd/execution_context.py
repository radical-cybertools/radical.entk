#!/usr/bin/env python

"""TODO: Docstring.
"""

__author__    = "Ole Weider <ole.weidner@rutgers.edu>"
__copyright__ = "Copyright 2014, http://radical.rutgers.edu"
__license__   = "MIT"

from radical.ensemblemd.exceptions import *
from radical.ensemblemd.engine import Engine
from radical.ensemblemd.execution_pattern import ExecutionPattern

#-------------------------------------------------------------------------------
#
class ExecutionContext(object):
    """An execution context represents the computing infrastructure on which 
       an ensemblemd application will run. ExecutionContext is the abstract
       base-class and usually not used directly. To create an execution context,
       use either of the following derived classes:

         * :class:`radical.ensemblemd.StaticExecutionContext`
         * :class:`radical.ensemblemd.DynamicExecutionContext`
    """

    #---------------------------------------------------------------------------
    #
    def __init__(self):
        """Creates a new ExecutionContext instance.
        """

        self._engine = Engine()

    #---------------------------------------------------------------------------
    #
    def run(self, pattern):
        """Creates a new ExecutionContext instance.
        """

        # Some basic type checks.g
        if type(pattern) != ExecutionPattern:
            raise TypeError(
              expected_type=ExecutionPattern, 
              actual_type=type(pattern))

        self._engine = Engine()

