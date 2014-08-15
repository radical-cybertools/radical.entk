#!/usr/bin/env python

"""This module defines and implements the Task class.
"""

__author__    = "Ole Weider <ole.weidner@rutgers.edu>"
__copyright__ = "Copyright 2014, http://radical.rutgers.edu"
__license__   = "MIT"

from radical.ensemblemd.subtask import Subtask
from radical.ensemblemd.exceptions import TypeError
from radical.ensemblemd.execution_pattern import ExecutionPattern

PATTERN_NAME = "Task"


# ------------------------------------------------------------------------------
#
class Task(ExecutionPattern):
    
    #---------------------------------------------------------------------------
    #
    def __init__(self, preprocessing, processing, postprocessing):
        """Creates a new Task instance.
        """
        super(Task, self).__init__()

        if type(preprocessing) != Subtask:
            raise TypeError(
                expected_type=Subtask, 
                actual_type=type(preprocessing))

        if type(processing) != Subtask:
            raise TypeError(
                expected_type=Subtask, 
                actual_type=type(processing))

        if type(postprocessing) != Subtask:
            raise TypeError(
                expected_type=Subtask, 
                actual_type=type(postprocessing))

    #-------------------------------------------------------------------------------
    #
    def get_name(self):
        """Implements base class ExecutionPattern.get_name().
        """
        return PATTERN_NAME

    #---------------------------------------------------------------------------
    #
    def set_preprocessing_subtask(self, subtask):
        """Sets the :class:`radical.ensemblemd.Subtask` that gets executed as 
           the preprocessing step.
        """
        if type(subtask) != Subtask:
            raise TypeError(
                expected_type=Subtask, 
                actual_type=type(subtask))

    #---------------------------------------------------------------------------
    #
    def set_processing_subtask(self, subtask):
        """Sets the :class:`radical.ensemblemd.Subtask` that gets executed as 
           the main processing step.
        """
        if type(subtask) != Subtask:
            raise TypeError(
                expected_type=Subtask, 
                actual_type=type(subtask))

    #---------------------------------------------------------------------------
    #
    def set_postprocessing_subtask(self, subtask):
        """Sets the :class:`radical.ensemblemd.Subtask` that gets executed as
           the postprocessing step.
        """
        if type(subtask) != Subtask:
            raise TypeError(
                expected_type=Subtask, 
                actual_type=type(subtask))
