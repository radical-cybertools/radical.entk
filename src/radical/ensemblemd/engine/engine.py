#!/usr/bin/env python

"""TODO: Docstring.
"""

__author__    = "Ole Weider <ole.weidner@rutgers.edu>"
__copyright__ = "Copyright 2014, http://radical.rutgers.edu"
__license__   = "MIT"


import radical.utils.logger as rul
from radical.utils import Singleton

from radical.ensemblemd.engine.plugin_registry import plugin_registry

#-------------------------------------------------------------------------------
#
class Engine(object):
    """The engine coordinates plug-in loading and other internal things.
    """

    __metaclass__ = Singleton

    #---------------------------------------------------------------------------
    #
    def __init__(self):
        """Creates the Engine instance (singleton).
        """

        # Initialize the logging
        self._logger = rul.logger.getLogger('radical.ensemblemd', "Engine")

        # Load Execution Plugins
        self._load_plugins ()

    #---------------------------------------------------------------------------
    #
    def _load_plugins(self):
        """Loads the execution plugins.
        """
        
        # attempt to load all registered plugins
        for plugin_name in plugin_registry:

            self._logger.info ("Loading  plugin %s"  %  plugin_name)