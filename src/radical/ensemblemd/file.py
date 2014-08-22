#!/usr/bin/env python

"""This module defines and implements the File class.
"""

__author__    = "Ole Weider <ole.weidner@rutgers.edu>"
__copyright__ = "Copyright 2014, http://radical.rutgers.edu"
__license__   = "MIT"

import os

from radical.ensemblemd.exceptions import FileError

# ------------------------------------------------------------------------------
#
class File(object):

    #---------------------------------------------------------------------------
    #
    def __init__(self):
        
        self._filename = None
        self._size     = None
        self._source   = "local"

    #---------------------------------------------------------------------------
    #
    @classmethod
    def from_local_path(cls, path):
        """Creates a new File object from a path pointing to a local file.
           Example::

               from radical.ensemblemd import File
               f = File.from_local_path("/experiments/data/input.dat")

        """
        # Check if the file is accessible. 
        if os.path.isfile(path) is False:
            raise FileError("File {0} doesn't exist.".format(path))

        cls()
        cls._source   = "local"
        cls._filename = path
        cls._size     = os.stat(path).st_size

        return cls

    #---------------------------------------------------------------------------
    #
    @classmethod
    def _create_from_task_output(cls, task_id, filename):
        cls()
        cls._source   = task_id
        cls._filename = filename
        return cls

    #---------------------------------------------------------------------------
    #
    def _get_file_description(self):
        description = {
            "filename" : self._filename
        }

        return description
