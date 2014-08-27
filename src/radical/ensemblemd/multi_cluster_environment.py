#!/usr/bin/env python

"""TODO: Docstring.
"""

__author__    = "Ole Weider <ole.weidner@rutgers.edu>"
__copyright__ = "Copyright 2014, http://radical.rutgers.edu"
__license__   = "MIT"

from radical.ensemblemd.engine import Engine
from radical.ensemblemd.exceptions import TypeError
from radical.ensemblemd.execution_pattern import ExecutionPattern
from radical.ensemblemd.execution_context import ExecutionContext

CONTEXT_NAME = "Dynamic"


#-------------------------------------------------------------------------------
#
class MultiClusterEnvironment(ExecutionContext):
    """A multi-cluster environment provides a dynamically managed set of 
       computational resources. 
    """

    #---------------------------------------------------------------------------
    #
    def __init__(self):
        """Creates a new ExecutionContext instance.
        """
        super(MultiClusterEnvironment, self).__init__()

    #---------------------------------------------------------------------------
    #
    def get_name(self):
        """Returns the name of the execution context.
        """
        return CONTEXT_NAME