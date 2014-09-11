#!/usr/bin/env python

"""This module defines and implement all ensemblemd Exceptions.
"""

__author__    = "Ole Weider <ole.weidner@rutgers.edu>"
__copyright__ = "Copyright 2014, http://radical.rutgers.edu"
__license__   = "MIT"

# ------------------------------------------------------------------------------
#
class EnsemblemdError(Exception):
    """EnsemblemdError is the base exception thrown by the ensemblemd library.
    """
    def __init__ (self, msg):
        super(EnsemblemdError, self).__init__ (msg)

# ------------------------------------------------------------------------------
#
class NotImplementedError(EnsemblemdError):
    """NotImplementedError is thrown if a class method or function is not 
    implemented.
    """
    def __init__ (self, method_name, class_name):
        msg = "Method {0}() missing implementation in {1}.".format(
            method_name, 
            class_name
        )
        super(NotImplementedError, self).__init__ (msg)

# ------------------------------------------------------------------------------
#
class TypeError(EnsemblemdError):
    """TypeError is thrown if a parameter of a wrong type is passed to a method 
    or function.
    """
    def __init__ (self, expected_type, actual_type):
        msg = "Expected (base) type {0}, but got {1}.".format(
            str(expected_type), 
            str(actual_type)
        )
        super(TypeError, self).__init__ (msg)

# ------------------------------------------------------------------------------
#
class FileError(EnsemblemdError):
    """FileError is thrown if something goes wrong related to file operations, 
    i.e., if a file doesn't exist, cannot be copied and so on.
    """
    def __init__ (self, message):
        super(FileError, self).__init__ (message)

# ------------------------------------------------------------------------------
#
class ArgumentError(EnsemblemdError):
    """A BadArgumentError is thrown if a wrong set of arguments were passed 
       to a kernel.
    """
    def __init__ (self, kernel_name, message, valid_arguments_set):
        msg = "Invalid argument(s) for kernel '{0}': {1}. Valid arguments are {2}.".format(
            kernel_name,
            message,
            valid_arguments_set
        )
        super(ArgumentError, self).__init__ (msg)

# ------------------------------------------------------------------------------
#
class NoKernelPluginError(EnsemblemdError):
    """NoKernelPluginError is thrown if no kernel plug-in could be found for a 
       given kernel name.
    """
    def __init__ (self, kernel_name):
        msg = "Couldn't find a kernel plug-in named '{0}'".format(kernel_name)
        super(NoKernelPluginError, self).__init__ (msg)

# ------------------------------------------------------------------------------
#
class NoKernelConfigurationError(EnsemblemdError):
    """NoKernelConfigurationError is thrown if no kernel configuration could
       be found for the provided resource key.
    """
    def __init__ (self, kernel_name, resource_key):
        msg = "Kernel '{0}' doesn not have a configuration entry for resource key '{1}'.".format(kernel_name, resource_key)
        super(NoKernelConfigurationError, self).__init__ (msg)

# ------------------------------------------------------------------------------
#
class NoExecutionPluginError(EnsemblemdError):
    """NoExecutionPluginError is thrown if a patterns is passed to an execution
    context via execut() but no execution plugin for the pattern exist.
    """
    def __init__ (self, pattern_name, context_name, plugin_name):
        if plugin_name is None:
            msg = "Couldn't find an execution plug-in for pattern '{0}' and execution context '{1}'.".format(
                pattern_name, 
                context_name
            )
        else:
            msg = "Couldn't find an execution plug-in named '{0}' for pattern '{1}' and execution context '{2}'.".format(
                plugin_name,
                pattern_name, 
                context_name,
            )         
        super(NoExecutionPluginError, self).__init__ (msg)
