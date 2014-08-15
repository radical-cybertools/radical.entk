#!/usr/bin/env python

"""Defines and implements the abstract kernel base class.
"""

__author__    = "Ole Weider <ole.weidner@rutgers.edu>"
__copyright__ = "Copyright 2014, http://radical.rutgers.edu"
__license__   = "MIT"

from copy import deepcopy
import radical.utils.logger  as rul

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

        self._info    = kernel_info
        self._name    = kernel_info['name']

        self._logger  = rul.getLogger ('radical.enmd.', self._name)
        self._args    = []

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
    def get_args(self):
        """Returns the arguments passed to the kernel during construction.
        """
        return self._args

    # --------------------------------------------------------------------------
    #
    def validate_args(self, args):
        """Validates if 'args' fulfill the argument requirements defined in the 
           kernel info.
        """
        self.get_logger().info("Validating arguments...")

        arg_config = deepcopy(self._info['arguments'])
        for (arg, arg_info) in arg_config.iteritems():
            arg_config[arg]["_is_set"] = False

        # Check if only valid args are passed.
        for kernel_arg in args:
            kernel_arg_ok = False
            for (arg, arg_info) in arg_config.iteritems():
                if kernel_arg.startswith(arg):
                    kernel_arg_ok = True
                    arg_config[arg]["_is_set"] = True
                    break

            if kernel_arg_ok is False:
                raise ArgumentError(
                    kernel_name=self.get_name(),
                    offending_argument=arg,
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

        self._args = args
