#!/usr/bin/env python

"""TODO: Docstring.
"""

__author__    = "Ole Weider <ole.weidner@rutgers.edu>"
__copyright__ = "Copyright 2014, http://radical.rutgers.edu"
__license__   = "MIT"

from radical.ensemblemd.engine import Engine
from radical.ensemblemd.exceptions import TypeError
from radical.ensemblemd.execution_pattern import ExecutionPattern
from radical.ensemblemd.execution_context import ExecutionContext

CONTEXT_NAME = "Static"


#-------------------------------------------------------------------------------
#
class SingleClusterEnvironment(ExecutionContext):
    """A static execution context provides a fixed set of computational 
       resources. 
    """

    #---------------------------------------------------------------------------
    #
    def __init__(self, resource, cores, walltime):
        """Creates a new ExecutionContext instance.
        """
        super(SingleClusterEnvironment, self).__init__()

    #---------------------------------------------------------------------------
    #
    def get_name(self):
        """Returns the name of the execution context.
        """
        return CONTEXT_NAME

    #---------------------------------------------------------------------------
    #
    def execute(self, pattern, force_plugin=None):
        """Creates a new SingleClusterEnvironment instance.
        """

        # Some basic type checks.
        if not isinstance(pattern, ExecutionPattern):
            raise TypeError(
              expected_type=ExecutionPattern, 
              actual_type=type(pattern))

        self._engine = Engine()
        plugin = self._engine.get_execution_plugin_for_pattern(
            pattern_name=pattern.get_name(),
            context_name=self.get_name(),
            plugin_name=force_plugin)

        plugin.verify_pattern(pattern)
        plugin.execute_pattern(pattern)
