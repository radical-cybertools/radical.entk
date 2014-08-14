#!/usr/bin/env python

"""This module defines and implements the Task class.
"""

__author__    = "Ole Weider <ole.weidner@rutgers.edu>"
__copyright__ = "Copyright 2014, http://radical.rutgers.edu"
__license__   = "MIT"

from radical.ensemblemd.subtask import Subtask
from radical.ensemblemd.exceptions import TypeError

# ------------------------------------------------------------------------------
#
class Task(object):
    
    #---------------------------------------------------------------------------
    #
    def __init__(self):
        pass

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
