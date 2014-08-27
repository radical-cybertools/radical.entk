#!/usr/bin/env python

"""This module defines and implements the Kernel class.
"""

__author__    = "Ole Weider <ole.weidner@rutgers.edu>"
__copyright__ = "Copyright 2014, http://radical.rutgers.edu"
__license__   = "MIT"

from radical.ensemblemd.engine import Engine
from radical.ensemblemd.exceptions import TypeError


# ------------------------------------------------------------------------------
#
class Kernel(object):
    
    #---------------------------------------------------------------------------
    #
    def __init__(self, kernel, args):
        """Create a new Kernel object.
        """
        if type(kernel) != str:
            raise TypeError(
                expected_type=str, 
                actual_type=type(kernel))

        if type(args) != list:
            raise TypeError(
                expected_type=list, 
                actual_type=type(args))

        self._engine = Engine()
        self._kernel = self._engine.get_kernel_plugin(kernel)

        # Call the validate_args() method of the plug-in.
        self._kernel.validate_args(args)

    #---------------------------------------------------------------------------
    #
    def get_raw_args(self):
        """Returns the arguments  passed to the kernel.
        """
        return self._kernel.get_arg(name)

    #---------------------------------------------------------------------------
    #
    def get_arg(self, name):
        """Returns the value of the kernel argument given by 'arg_name'.
        """
        return self._kernel.get_arg(name)

    #---------------------------------------------------------------------------
    #
    def _get_kernel_description(self):
        """Returns the kernel description as a dictionary that can be 
           translated into a CU description.
        """
        return self._kernel._get_kernel_description()
