#!/usr/bin/env python

"""This module defines and implements the Task class.
"""

__author__    = "Ole Weider <ole.weidner@rutgers.edu>"
__copyright__ = "Copyright 2014, http://radical.rutgers.edu"
__license__   = "MIT"

import uuid
from radical.ensemblemd.exceptions import TypeError
from radical.ensemblemd.file import File

# ------------------------------------------------------------------------------
#
class Task(object):

    #---------------------------------------------------------------------------
    #
    def __init__(self):
        """ Creates a new Task object.
        """
        self._id = uuid.uuid4()
        self._kernel = None
        self._expected_output = list()
        self._requires_input = dict()

    #---------------------------------------------------------------------------
    #
    def size(self):
        """ Returns the size. If called for a Task, size() returns always '1'. 
            If called for a Ensemble, size() returns the number of tasks in the 
            Ensemble.
        """
        return 1

    #---------------------------------------------------------------------------
    #
    def set_kernel(self, kernel):
        """Sets the application kernel that gets executed in this Subtask.
        """
        self._kernel = kernel

    #---------------------------------------------------------------------------
    #
    def add_output(self, filename, download=None):
        """Asserts the existence of a specific file after the step has completed.
        """
        self._expected_output.append(filename)
        return File._create_from_task_output(task_id=self._id, filename=filename)
        
    #---------------------------------------------------------------------------
    #
    def assert_output(self, filename):
        """Asserts the existence of a specific file after the step has completed.
        """
        pass

    #---------------------------------------------------------------------------
    #
    def add_input(self, files, labels):
        """Creates a new OutputFile object referncing a physical output file 
           genereated by this Subtask.
        """
        if type(files) != list:
            files = [files]

        if type(labels) != list:
            labels = [labels]

        if len(files) != len(labels):
            raise LabelError(
                "File count ({0}) doesn't match label count ({1}).".format(
                    len(files),
                    len(labels)
                    )
                )

        for index in range(0, len(files)):
            if labels[index] in self._requires_input:
                raise LabelError("Duplicate label name '{0}'".format(
                    labels[index]
                    )
                )

            self._requires_input[labels[index]] = files[index]



    #---------------------------------------------------------------------------
    #
    def _get_task_description(self):
        """Returns the task description.
        """
        description = {
            "kernel"          : self._kernel._get_kernel_description(),
            "requires_input"  : self._requires_input,
            "expected_output" : self._expected_output
        }

        return description
