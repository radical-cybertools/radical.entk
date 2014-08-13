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
        msg = "Expected type {0}, but got {1}.".format(
            str(expected_type), 
            str(actual_type)
        )
        super(TypeError, self).__init__ (msg)

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
