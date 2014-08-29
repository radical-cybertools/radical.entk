#!/usr/bin/env python

"""A static execution plugin for single tasks.
"""

__author__    = "Ole Weider <ole.weidner@rutgers.edu>"
__copyright__ = "Copyright 2014, http://radical.rutgers.edu"
__license__   = "MIT"

import os 
import radical.pilot

from radical.ensemblemd.execplugins.plugin_base import PluginBase

# ------------------------------------------------------------------------------
# 
_PLUGIN_INFO = {
    "name":         "pipeline.static.default",
    "pattern":      "Pipeline",
    "context_type": "Static"
}

_PLUGIN_OPTIONS = []


# ------------------------------------------------------------------------------
# 
class Plugin(PluginBase):

    # --------------------------------------------------------------------------
    #
    def __init__(self):
        super(Plugin, self).__init__(_PLUGIN_INFO, _PLUGIN_OPTIONS)

    # --------------------------------------------------------------------------
    #
    def verify_pattern(self, pattern):

        if True:
            self.get_logger().info("Pattern workload verification passed.")
        else:
            self.get_logger().error("Pattern workload verification failed.")

    # --------------------------------------------------------------------------
    #
    def execute_pattern(self, pattern):

        pass