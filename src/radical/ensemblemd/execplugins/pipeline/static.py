#!/usr/bin/env python

"""A static execution plugin for single tasks.
"""

__author__    = "Ole Weider <ole.weidner@rutgers.edu>"
__copyright__ = "Copyright 2014, http://radical.rutgers.edu"
__license__   = "MIT"

from radical.ensemblemd.task import Task
from radical.ensemblemd.batch import Batch

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
        workload = pattern._get_pattern_workload()

        # TODO: implement proper verification
        if True:
            self.get_logger().info("Pattern workload verification passed.")
        else:
            self.get_logger().error("Pattern workload verification failed.")

    # --------------------------------------------------------------------------
    #
    def execute_pattern(self, pattern):
        steps = pattern._get_pattern_workload()

        # each entry in 'workload' is a sequential 'step' that consists either
        # of a single task or a batch. 
        total_steps = len(steps)
        step_count = 1
        for step in steps:
            self.get_logger().info("Executing pipeline step {0}/{1} with {2} task(s)".format(
                step_count,
                total_steps,
                1))
            step_count += 1

            print str(steps)

        self.get_logger().info("Pattern workload: {0}".format(steps))
        self.get_logger().info("Executing pattern...")