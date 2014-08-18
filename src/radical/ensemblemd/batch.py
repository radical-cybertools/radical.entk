#!/usr/bin/env python

"""This module defines and implements the Batch class.
"""

__author__    = "Ole Weider <ole.weidner@rutgers.edu>"
__copyright__ = "Copyright 2014, http://radical.rutgers.edu"
__license__   = "MIT"

from radical.ensemblemd.file import File

# ------------------------------------------------------------------------------
#
class Batch(object):

    #---------------------------------------------------------------------------
    #
    def __init__(self, size):
        
        self._size = size

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
        pass

    #---------------------------------------------------------------------------
    #
    def add_output(self, filename):
        """Asserts the existence of a specific file after the step has completed.
        """
        return File()
        
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
        pass