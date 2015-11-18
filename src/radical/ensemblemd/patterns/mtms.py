#!/usr/bin/env python

"""This module defines and implements the Pipeline class.
"""

__author__    = "Vivek Balasubramnian <vivek.balasubramanian@rutgers.edu>"
__copyright__ = "Copyright 2015, http://radical.rutgers.edu"
__license__   = "MIT"

from radical.ensemblemd.exceptions import NotImplementedError
from radical.ensemblemd.execution_pattern import ExecutionPattern

PATTERN_NAME = "MTMS"


# ------------------------------------------------------------------------------
#
class MTMS(ExecutionPattern):

    #---------------------------------------------------------------------------
    #
    def __init__(self, stages=1,tasks=1):
        """Creates a new MTMS instance.
        """
        self._tasks = tasks
        self._stages = stages

        super(MTMS, self).__init__()


    #---------------------------------------------------------------------------
    #
    @property
    def tasks(self):
        """Returns the number of tasks in MTMS.
        """
        return self._tasks

    #---------------------------------------------------------------------------
    #
    @property
    def stages(self):
        """Returns the number of stages in each task of the MTMS.
        """
        return self._stages

    #---------------------------------------------------------------------------
    #
    @property
    def name(self):
        """Returns the name of the pattern.
        """
        return PATTERN_NAME


    '''
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

    #---------------------------------------------------------------------------
    #
    def step_5(self, column):
        """The fifth step of the pipeline.
        """
        raise NotImplementedError(
          method_name="step_5",
          class_name=type(self))

    #---------------------------------------------------------------------------
    #
    def step_6(self, column):
        """The sixth step of the pipeline.
        """
        raise NotImplementedError(
          method_name="step_6",
          class_name=type(self))

    #---------------------------------------------------------------------------
    #
    def step_7(self, column):
        """The seventh step of the pipeline.
        """
        raise NotImplementedError(
          method_name="step_7",
          class_name=type(self))

    #---------------------------------------------------------------------------
    #
    def step_8(self, column):
        """The eighth step of the pipeline.
        """
        raise NotImplementedError(
          method_name="step_8",
          class_name=type(self))

    #---------------------------------------------------------------------------
    #
    def step_9(self, column):
        """The ninth step of the pipeline.
        """
        raise NotImplementedError(
          method_name="step_9",
          class_name=type(self))

    #---------------------------------------------------------------------------
    #
    def step_10(self, column):
        """The tenth step of the pipeline.
        """
        raise NotImplementedError(
          method_name="step_10",
          class_name=type(self))
    '''