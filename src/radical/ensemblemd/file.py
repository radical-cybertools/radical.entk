#!/usr/bin/env python

"""This module defines and implements the File class.
"""

__author__    = "Ole Weider <ole.weidner@rutgers.edu>"
__copyright__ = "Copyright 2014, http://radical.rutgers.edu"
__license__   = "MIT"

# ------------------------------------------------------------------------------
#
class File(object):

    #---------------------------------------------------------------------------
    #
    def __init__(self):
        
        self._task_id = None
        self._filename = None

    #---------------------------------------------------------------------------
    #
    @classmethod
    def from_local_path(cls, path):
        """Creates a new File object from a path pointing to a local file.
           Example::

               from radical.ensemblemd import File
               f = File.from_local_path("/experiments/data/input.dat")

        """
        cls()
        cls._filename = path
        return cls

    #---------------------------------------------------------------------------
    #
    @classmethod
    def _create_from_task_output(cls, task_id, filename):
        cls()
        cls._task_id = task_id
        cls._filename = filename
        return cls

    #---------------------------------------------------------------------------
    #
    def _get_file_description(self):
        description = {
            "filename" : self._filename
        }

        return description
