#!/usr/bin/env python

"""This module defines and implements the Pipeline class.
"""

__author__    = "Ole Weider <ole.weidner@rutgers.edu>"
__copyright__ = "Copyright 2014, http://radical.rutgers.edu"
__license__   = "MIT"

from radical.ensemblemd.task import Task
from radical.ensemblemd.exceptions import TypeError
from radical.ensemblemd.execution_pattern import ExecutionPattern

PATTERN_NAME = "Pipeline"


# ------------------------------------------------------------------------------
#
class Pipeline(ExecutionPattern):
    
    #---------------------------------------------------------------------------
    #
    def __init__(self, preprocessing=None, processing=None, postprocessing=None):
        """Creates a new Task instance.
        """
        super(Pipeline, self).__init__()

        if preprocessing is not None and type(preprocessing) != Subtask:
            raise TypeError(
                expected_type=Subtask, 
                actual_type=type(preprocessing))

        if processing is not None and type(processing) != Subtask:
            raise TypeError(
                expected_type=Subtask, 
                actual_type=type(processing))

        if postprocessing is not None and type(postprocessing) != Subtask:
            raise TypeError(
                expected_type=Subtask, 
                actual_type=type(postprocessing))

        self._preprocessing = preprocessing
        self._processing = processing
        self._postprocessing = postprocessing

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
        self._preprocessing = preprocessing

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
        self._processing = processing

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
        self._postprocessing = postprocessing

    #---------------------------------------------------------------------------
    #
    def _get_task_description(self):
        pass
