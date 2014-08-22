#!/usr/bin/env python

"""This module defines and implements the Pipeline class.
"""

__author__    = "Ole Weider <ole.weidner@rutgers.edu>"
__copyright__ = "Copyright 2014, http://radical.rutgers.edu"
__license__   = "MIT"

from radical.ensemblemd.task import Task
from radical.ensemblemd.ensemble import Ensemble
from radical.ensemblemd.exceptions import TypeError
from radical.ensemblemd.execution_pattern import ExecutionPattern

PATTERN_NAME = "Pipeline"


# ------------------------------------------------------------------------------
#
class Pipeline(ExecutionPattern):
    
    #---------------------------------------------------------------------------
    #
    def __init__(self, steps=[]):
        """Creates a new Task instance.
        """
        super(Pipeline, self).__init__()

        # self._steps contains the list of tasks in this pipeline.
        self._steps = list()
        self.add_steps(steps)
        
    #-------------------------------------------------------------------------------
    #
    def get_name(self):
        """Implements base class ExecutionPattern.get_name().
        """
        return PATTERN_NAME

    #-------------------------------------------------------------------------------
    #
    def add_steps(self, steps):
        """Implements base class ExecutionPattern.get_name().
        """
        if type(steps) != list:
            raise TypeError(
                expected_type=list, 
                actual_type=type(steps))

        for step in steps:
            if type(step) != Task and type(step) != Ensemble:
                raise TypeError(
                    expected_type=[Task, Ensemble], 
                    actual_type=type(step))
            else:
                self._steps.append(step)

    #---------------------------------------------------------------------------
    #
    def _get_pattern_workload(self):
        """Returns a structured description of the tasks in the pipeline.
        """
        return self._steps
