#!/usr/bin/env python

"""TODO: Docstring.
"""

__author__    = "Ole Weider <ole.weidner@rutgers.edu>"
__copyright__ = "Copyright 2014, http://radical.rutgers.edu"
__license__   = "MIT"

from radical.ensemblemd.engine import Engine
from radical.ensemblemd.exceptions import NotImplementedError
from radical.ensemblemd.execution_pattern import ExecutionPattern

#-------------------------------------------------------------------------------
#
class ExecutionContext(object):
    """An execution context represents the computing infrastructure on which
       an ensemblemd application will run. ExecutionContext is the abstract
       base-class and usually not used directly. To create an execution context,
       use either of the following derived classes:

         * :class:`radical.ensemblemd.SingleClusterEnvironment`
         * :class:`radical.ensemblemd.MultiClusterEnvironment`
    """

    #---------------------------------------------------------------------------
    #
    def __init__(self):
        """Creates a new ExecutionContext instance.
        """
        self._engine = Engine()

    #---------------------------------------------------------------------------
    #
    def get_name(self):
        """Returns the name of the execution pattern.
        """
        raise NotImplementedError(
          method_name="get_name",
          class_name=type(self))

    #---------------------------------------------------------------------------
    #
    def allocate(self):
        """Allocates the requested resources.
        """
        raise NotImplementedError(
          method_name="allocate",
          class_name=type(self))

    #---------------------------------------------------------------------------
    #
    def run(self, pattern, force_plugin=None):
        """Creates a new ExecutionContext instance.
        """
        raise NotImplementedError(
          method_name="execute",
          class_name=type(self))
