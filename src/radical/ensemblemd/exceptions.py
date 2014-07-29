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
    def __init__ (self, msg):
        super(NotImplementedError, self).__init__ (msg)


# ------------------------------------------------------------------------------
#
class TypeError(EnsemblemdError):
    """TypeError is thrown if a parameter of a wrong type is passed to a method 
    or function.
    """
    def __init__ (self, expected_type, actual_type):
        msg = "Expected type {0}, but got {1}".format(str(expected_type), str(actual_type))
        super(TypeError, self).__init__ (msg)