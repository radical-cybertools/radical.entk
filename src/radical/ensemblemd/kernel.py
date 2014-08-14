#!/usr/bin/env python

"""This module defines and implements the Kernel class.
"""

__author__    = "Ole Weider <ole.weidner@rutgers.edu>"
__copyright__ = "Copyright 2014, http://radical.rutgers.edu"
__license__   = "MIT"

from radical.ensemblemd.exceptions import TypeError


# ------------------------------------------------------------------------------
#
class Kernel(object):
    
    #---------------------------------------------------------------------------
    #
    def __init__(self, kernel, args):

        if type(kernel) != str:
            raise TypeError(
                expected_type=str, 
                actual_type=type(kernel))

        if type(args) != list:
            raise TypeError(
                expected_type=list, 
                actual_type=type(args))