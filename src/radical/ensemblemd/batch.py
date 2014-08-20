#!/usr/bin/env python

"""This module defines and implements the Batch class.
"""

__author__    = "Ole Weider <ole.weidner@rutgers.edu>"
__copyright__ = "Copyright 2014, http://radical.rutgers.edu"
__license__   = "MIT"

import uuid

from radical.ensemblemd.file import File
from radical.ensemblemd.exceptions import LabelError
from radical.ensemblemd.execution_pattern import ExecutionPattern

PATTERN_NAME = "Batch"


# ------------------------------------------------------------------------------
#
class Batch(ExecutionPattern):

    #---------------------------------------------------------------------------
    #
    def __init__(self, size):
        """ Creates a new Batch object.
        """
        super(Batch, self).__init__()

        
        self._batch_id = uuid.uuid4()
        self._task_ids = list()
        for i in range(0, size):
            self._task_ids.append(uuid.uuid4())

        self._size = size

        self._kernel = None
        self._expected_output = list()
        self._requires_input = dict()

    #---------------------------------------------------------------------------
    #
    @classmethod
    def from_input_files(cls, files, label):
        """ The from_input_files() class method creates a new batch containing 
            a set of tasks based on the provided list of input files. For 
            each file in 'files' a new task is created. The files can be 
            referenced via the provided 'label'.
        """
        if type(files) != list:
            raise TypeError(
                expected_type=list, 
                actual_type=type(files))

        if type(label) != str:
            raise TypeError(
                expected_type=str, 
                actual_type=type(label))


        cls = Batch(size=len(files))
        return cls

    #-------------------------------------------------------------------------------
    #
    def get_name(self):
        """Implements base class ExecutionPattern.get_name().
        """
        return PATTERN_NAME

    #---------------------------------------------------------------------------
    #
    def size(self):
        """ Returns the size. If called for a Task, size() returns always '1'. 
            If called for a Batch, size() returns the number of tasks in the 
            Batch.
        """
        return self._size

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
        return File._create_from_task_output(task_id=self._batch_id, filename=filename)
        
    #---------------------------------------------------------------------------
    #
    def assert_output(self, filename):
        """Asserts the existence of a specific file after the step has completed.
        """
        pass

    #---------------------------------------------------------------------------
    #
    def add_input(self, files, labels, shared=True):
        """Adds one or more input files to the batch. Files added via 
           ``add_input()`` are added to all tasks of the batch. If the ``shared``
           parameter is set to ``True`` (default), the file(s) are copied once 
           per batch and shared amongst tasks. If set to ``False``, individual
           copies of the same file(s) are added to each batch task.
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
    def _get_batch_description(self):
        """Returns the task description.
        """
        tasks = list()

        for i in range(0, self.size()):
            tasks.append({
                "kernel"          : self._kernel,
                "requires_input"  : self._requires_input,
                "expected_output" : self._expected_output
            })

        return tasks
