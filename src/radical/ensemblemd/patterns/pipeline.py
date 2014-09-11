#!/usr/bin/env python

"""This module defines and implements the Pipeline class.
"""

__author__    = "Ole Weider <ole.weidner@rutgers.edu>"
__copyright__ = "Copyright 2014, http://radical.rutgers.edu"
__license__   = "MIT"

from radical.ensemblemd.exceptions import NotImplementedError
from radical.ensemblemd.execution_pattern import ExecutionPattern

PATTERN_NAME = "Pipeline"


# ------------------------------------------------------------------------------
#
class Pipeline(ExecutionPattern):
    
    #---------------------------------------------------------------------------
    #
    def __init__(self, width=1):
        """Creates a new Pipeline instance.
        """
        self._width = width
        super(Pipeline, self).__init__()

    #---------------------------------------------------------------------------
    #
    def get_width(self):
        """Returns the width of the pipeline.
        """
        return self._width
        
    #---------------------------------------------------------------------------
    #
    def get_name(self):
        """Implements base class ExecutionPattern.get_name().
        """
        return PATTERN_NAME

    #---------------------------------------------------------------------------
    #
    def step_01(self, column):
        """The first step of the pipeline.
        """
        raise NotImplementedError(
          method_name="step_01",
          class_name=type(self))

    #---------------------------------------------------------------------------
    #
    def step_02(self, column):
        """The second step of the pipeline.
        """
        raise NotImplementedError(
          method_name="step_02",
          class_name=type(self))

    #---------------------------------------------------------------------------
    #
    def step_03(self, column):
        """The third step of the pipeline.
        """
        raise NotImplementedError(
          method_name="step_03",
          class_name=type(self))

    #---------------------------------------------------------------------------
    #
    def step_04(self, column):
        """The fourth step of the pipeline.
        """
        raise NotImplementedError(
          method_name="step_04",
          class_name=type(self))
