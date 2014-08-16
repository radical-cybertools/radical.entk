#!/usr/bin/env python

"""This module defines and implements the Subtask class.
"""

__author__    = "Ole Weider <ole.weidner@rutgers.edu>"
__copyright__ = "Copyright 2014, http://radical.rutgers.edu"
__license__   = "MIT"

# ------------------------------------------------------------------------------
#
class Subtask(object):

    #---------------------------------------------------------------------------
    #
    def __init__(self):
        pass

    #---------------------------------------------------------------------------
    #
    def set_kernel(self, kernel):
        """Sets the application kernel that gets executed in this Subtask.
        """
        pass

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