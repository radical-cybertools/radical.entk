#!/usr/bin/env python

"""This module defines and implements the Batch class.
"""

__author__    = "Ole Weider <ole.weidner@rutgers.edu>"
__copyright__ = "Copyright 2014, http://radical.rutgers.edu"
__license__   = "MIT"

import uuid

from radical.ensemblemd.file import File

# ------------------------------------------------------------------------------
#
class Batch(object):

    #---------------------------------------------------------------------------
    #
    def __init__(self, size):
        
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
    def add_input(self, dataobject, label):
        """Creates a new OutputFile object referncing a physical output file 
           genereated by this Subtask.
        """
        self._requires_input[label] = file

    #---------------------------------------------------------------------------
    #
    def _get_batch_description(self):
        """Returns the task description.
        """
        tasks = list()

        for i in range(0, self.size()):
            tasks.append({
                "kernel"          : self._kernel._get_kernel_description(),
                "requires_input"  : self._requires_input,
                "expected_output" : self._expected_output
            })

        return tasks
