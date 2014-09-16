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
    def step_1(self, column):
        """The first step of the pipeline.
        """
        raise NotImplementedError(
          method_name="step_1",
          class_name=type(self))

    #---------------------------------------------------------------------------
    #
    def step_2(self, column):
        """The second step of the pipeline.
        """
        raise NotImplementedError(
          method_name="step_2",
          class_name=type(self))

    #---------------------------------------------------------------------------
    #
    def step_3(self, column):
        """The third step of the pipeline.
        """
        raise NotImplementedError(
          method_name="step_3",
          class_name=type(self))

    #---------------------------------------------------------------------------
    #
    def step_4(self, column):
        """The fourth step of the pipeline.
        """
        raise NotImplementedError(
          method_name="step_4",
          class_name=type(self))
