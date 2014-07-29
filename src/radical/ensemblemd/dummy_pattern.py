#!/usr/bin/env python

"""TODO: Docstring.
"""

__author__    = "Ole Weider <ole.weidner@rutgers.edu>"
__copyright__ = "Copyright 2014, http://radical.rutgers.edu"
__license__   = "MIT"

from radical.ensemblemd.execution_pattern import ExecutionPattern


#-------------------------------------------------------------------------------
#
class DummyPattern(ExecutionPattern):
    """DummyPattern provides an execution pattern that is solely used for 
    internal purposes by the ensemblemd testing framework. 
    """

    def __init__(self):
        """Creates a new DummyPattern instance.
        """
