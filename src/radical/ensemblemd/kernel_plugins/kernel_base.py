#!/usr/bin/env python

"""Defines and implements the abstract kernel base class.
"""

__author__    = "Ole Weider <ole.weidner@rutgers.edu>"
__copyright__ = "Copyright 2014, http://radical.rutgers.edu"
__license__   = "MIT"

from copy import deepcopy
import radical.utils.logger  as rul

import radical.utils as ru

from radical.ensemblemd.exceptions import ArgumentError
from radical.ensemblemd.exceptions import NotImplementedError


# ------------------------------------------------------------------------------
# plugin base class
#
class KernelBase(object):

    #__metaclass__ = ru.Singleton

    # --------------------------------------------------------------------------
    #
    def __init__ (self, kernel_info) :

        self._info     = kernel_info
        self._name     = kernel_info['name']
        self._subname  = None

        #self._logger   = ru.get_logger ('radical.enmd.{0}'.format(self._name))
        self._args     = []
        self._raw_args = []

        self._pre_exec               = None
        self._post_exec              = None
        self._environment            = {}

        self._executable             = None
        self._arguments              = None
        self._uses_mpi               = None
        self._cores                  = 1


        self._upload_input_data      = None
        
        self._link_input_data        = None

        self._download_input_data    = None
        self._download_output_data   = None

        self._copy_input_data        = None
        self._copy_output_data       = None



    # --------------------------------------------------------------------------
    #
    def as_dict(self):
        """Returns a dictionary representation of the kernel.
        """
        d = {"pre_exec": self._pre_exec,
             "environment": self._environment,
             "executable": self._executable,
             "arguments": self._arguments,
             "uses_mpi": self._uses_mpi,
             "cores": self._cores}
        return d

    # --------------------------------------------------------------------------
    #
    def __str__(self):
        return str(self.as_dict())

    # --------------------------------------------------------------------------
    #
    def register(self) :
        """ Kernel registration function. The engine calls this during startup
            to retrieve the adaptor information.
        """
        return self._info

    # --------------------------------------------------------------------------
    #
    @staticmethod
    def get_name():
        return self._name

    # --------------------------------------------------------------------------
    #
    def get_info (self) :
        return self._info

    # --------------------------------------------------------------------------
    #
    def get_logger(self):
        return self._logger

    # --------------------------------------------------------------------------
    #
    def get_arg(self, arg_name):
        """Returns the value of the argument given by 'arg_name'.
        """
        return self._args[arg_name]["_value"]

    # --------------------------------------------------------------------------
    #
    def get_raw_args(self):
        """Returns all arguments as they were passed to the kernel.
        """
        return self._raw_args

    # --------------------------------------------------------------------------
    #
    def validate_args(self, args):
        """Validates if 'args' fulfill the argument requirements defined in the
           kernel info.
        """
        self._raw_args = args

        arg_config = deepcopy(self._info['arguments'])

        if arg_config == "*":
            self._args = args
            #self.get_logger().debug("Free-form argument validation ok: {0}.".format(args))
            return

        for (arg, arg_info) in arg_config.iteritems():
            arg_config[arg]["_is_set"] = False
            arg_config[arg]["_value"] = None

        # Check if only valid args are passed.
        for kernel_arg in args:
            kernel_arg_ok = False
            for (arg, arg_info) in arg_config.iteritems():
                if kernel_arg.startswith(arg):
                    kernel_arg_ok = True
                    arg_config[arg]["_is_set"] = True
                    arg_config[arg]["_value"] = kernel_arg.replace(arg, '')
                    break

            if kernel_arg_ok is False:
                raise ArgumentError(
                    kernel_name=self.get_name(),
                    message="Unknown / malformed argument '{0}'".format(kernel_arg),
                    valid_arguments_set=arg_config
                )

        # Check if mandatory args are set.
        for (arg, arg_info) in arg_config.iteritems():
            if (arg_info["mandatory"] == True) and (arg_info["_is_set"] == False):
                raise ArgumentError(
                    kernel_name=self.get_name(),
                    message="Mandatory argument '{0}' missing".format(arg),
                    valid_arguments_set=self._info['arguments']
                )

        #self.get_logger().debug("Arguments ok: {0}.".format(args))
        self._args = arg_config

    # --------------------------------------------------------------------------
    #
    def _bind_to_resource(self, resource_key, pattern_name=None):
        """Binds the kernel to a specific resource.
        """
        raise NotImplementedError(
          method_name="_get_kernel_description",
          class_name=type(self))
