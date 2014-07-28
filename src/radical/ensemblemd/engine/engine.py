#!/usr/bin/env python

"""TODO: Docstring.
"""

__author__    = "Ole Weider <ole.weidner@rutgers.edu>"
__copyright__ = "Copyright 2014, http://radical.rutgers.edu"
__license__   = "MIT"


from radical.utils import Singleton
from radical.ensemblemd.execution_context import ExecutionContext


#-------------------------------------------------------------------------------
#
class Engine(object):
    """The engine coordinates plug-in loading and other internal things.
    """

    def __init__(self):
        """Creates the Engine instance (singleton).
        """

        __metaclass__ = Singleton
