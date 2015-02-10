#!/usr/bin/env python

"""TODO: Docstring.
"""

__author__    = "Ole Weider <ole.weidner@rutgers.edu>"
__copyright__ = "Copyright 2014, http://radical.rutgers.edu"
__license__   = "MIT"

from radical.ensemblemd.exceptions import *
from radical.ensemblemd.engine import Engine

#-------------------------------------------------------------------------------
#
class ExecutionPattern(object):
    """An execution pattern represents a specific molecular dynamics workflow
       pattern. Pattern is the abstract base-class and not used directly.
       To create an execution pattern, use either of the following derived
       classes:

         * :class:`radical.ensemblemd.DummyPattern`
         * :class:`radical.ensemblemd.SimulationAnalysisPattern
         * ...`
    """

    #---------------------------------------------------------------------------
    #
    def __init__(self):
        """Creates a new ExecutionPattern instance.
        """
        self._workload = None
        self._engine = Engine()

    #---------------------------------------------------------------------------
    #
    @property
    def name(self):
        """Returns the name of the execution pattern.
        """
        raise NotImplementedError(
          method_name="get_name",
          class_name=type(self))

    #---------------------------------------------------------------------------
    #
    @property
    def execution_profile(self):
        """Returns the execution profile after the pattern has finished
           running, 'None' otheriwse.
        """
        raise NotImplementedError(
          method_name="execution_profile",
          class_name=type(self))

    #---------------------------------------------------------------------------
    #
    def _get_workload(self):
      """Returns the workload description (tasks) of the execution pattern
         instance.
      """
      return self._workload
