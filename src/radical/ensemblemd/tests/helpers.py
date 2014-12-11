#!/usr/bin/env python

"""Defines and implements some helper classes and functions for the unit tests.
"""

__author__    = "Ole Weider <ole.weidner@rutgers.edu>"
__copyright__ = "Copyright 2014, http://radical.rutgers.edu"
__license__   = "MIT"


def _exception_test_helper(exception, expected_type):
    """Test whether an exception has a specific type.
    """
    if type(exception) != expected_type:
        assert False, "Expected exception type {0} but got {1}".format(expected_type, type(exception))
    else:
        assert True
