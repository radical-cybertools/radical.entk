#!/usr/bin/env python

"""TODO: Docstring.
"""

__author__    = "Ole Weider <ole.weidner@rutgers.edu>"
__copyright__ = "Copyright 2014, http://radical.rutgers.edu"
__license__   = "MIT"


import radical.utils.logger as rul
from radical.utils import Singleton

from radical.ensemblemd.exceptions import *
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
        self._plugins = list()
        self._load_plugins()

    #---------------------------------------------------------------------------
    #
    def _load_plugins(self):
        """Loads the execution plugins.
        """
        
        # attempt to load all registered plugins
        for plugin_module_name in plugin_registry:

            # first, import the module
            adaptor_module = None
            try :
                adaptor_module = __import__ (plugin_module_name, fromlist=['Adaptor'])

            except Exception as e:
                self._logger.error("Skipping execution context plugin {0}: module loading failed: {1}".format(plugin_module_name, e))
                continue # skip to next adaptor

            # we expect the plugin module to have an 'Adaptor' class
            # implemented, which, on calling 'register()', returns
            # a info dict for all implemented adaptor classes.
            plugin_instance = None
            plugin_info     = None

            try: 
                plugin_instance = adaptor_module.Adaptor()
                plugin_info     = plugin_instance.register()

                self._logger.info("Loaded execution context plugin '{0}' from {1}".format(
                    plugin_instance.get_name(),
                    plugin_module_name))
                self._plugins.append(plugin_instance)

            except Exception as e:
                self._logger.error ("Skipping execution context plugin {0}: loading failed: '{1}'".format(plugin_module_name, e))

    #---------------------------------------------------------------------------
    #
    def get_plugin_for_pattern(self, pattern_name, context_name):
        """Returns an execution plug-in for a given pattern and context.
        """
        plugin = None

        for candidate_plugin in self._plugins:
            for_pattern = candidate_plugin.get_info()['pattern']
            for_context = candidate_plugin.get_info()['context_type']
            if (for_pattern == pattern_name) and (for_context == context_name):
                plugin = candidate_plugin
                break

        if plugin != None:
            self._logger.info("Selected execution plug-in '{0}' for pattern '{1}' and context type '{2}'.".format(
                plugin.get_name(),
                pattern_name,
                context_name)
            )
            return plugin
        else:
            error = NoExecutionPluginError(
                pattern_name=pattern_name,
                context_name=context_name)
            self._logger.error(str(error))
            raise error
