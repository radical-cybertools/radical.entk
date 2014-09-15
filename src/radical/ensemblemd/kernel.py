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
    def __init__(self, name, args=None):
        """Create a new Kernel object.
        """
        if type(name) != str:
            raise TypeError(
                expected_type=str, 
                actual_type=type(name))

        self._engine = Engine()
        self._kernel = self._engine.get_kernel_plugin(name)

        if args is not None:
            self.set_args(args)

    #---------------------------------------------------------------------------
    #
    def arguments(self, args):
        pass

    #---------------------------------------------------------------------------
    #
    def set_args(self, args):
        """Sets the arguments for the kernel.
        """
        if type(args) != list:
            raise TypeError(
                expected_type=list, 
                actual_type=type(args))
        
        # Call the validate_args() method of the plug-in.
        self._kernel.validate_args(args)

    #---------------------------------------------------------------------------
    #
    def upload_input_data(self, data_directives):
        """Instructs the application to upload one or more files or directories 
           from the **machine the application is executing** into the kernel's 
           execution directory.
        """
        self._kernel._upload_input_data = data_directives

    #---------------------------------------------------------------------------
    #
    def download_input_data(self, data_directives):
        """Instructs the kernel to download one or more files or directories 
           from a **remote HTTP server** into the kernel's execution directory.
        """
        self._kernel._download_input_data = data_directives

    #---------------------------------------------------------------------------
    #
    def copy_input_data(self, data_directives):
        """Instructs the kernel to copy one or more files or directories from
           the **execution host's** filesystem into the kernel's execution 
           directory.
        """
        self._kernel._copy_input_data = data_directives

    #---------------------------------------------------------------------------
    #
    def link_input_data(self, data_directives):
        """Instructs the kernel to create a link to one or more files or 
           directories on the *execution host's** filesystem in the kernel's 
           execution directory.
        """
        self._kernel._link_input_data = data_directives

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
    def _get_kernel_description(self, resource_key):
        """Returns the kernel description as a dictionary that can be 
           translated into a CU description.
        """
        return self._kernel._get_kernel_description(resource_key)
