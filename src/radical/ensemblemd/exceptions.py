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
    """NotImplementedError is thrown if a class method or function is 
    not implemented.
    """
    def __init__ (self, msg):
        super(NotImplementedError, self).__init__ (msg)
