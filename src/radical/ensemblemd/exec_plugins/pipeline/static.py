#!/usr/bin/env python

"""A static execution plugin for single tasks.
"""

__author__    = "Ole Weider <ole.weidner@rutgers.edu>"
__copyright__ = "Copyright 2014, http://radical.rutgers.edu"
__license__   = "MIT"

import os 
import radical.pilot 

from radical.ensemblemd.exec_plugins.plugin_base import PluginBase

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
        self.get_logger().info("Verifying pattern...")

    # --------------------------------------------------------------------------
    #
    def execute_pattern(self, pattern, resource):

        pipeline_width = pattern.get_width()

        self.get_logger().info("Executing pipeline of width {0} on {1} allocated core(s) on '{2}'".format(
            pipeline_width, resource._cores, resource._resource_key))

        step_01_cus = list()
        for instance in range(0, pipeline_width):

            kernel = pattern.step_01(instance)
            kernel._bind_to_resource(resource._resource_key)

            cu = radical.pilot.ComputeUnitDescription()

            cu.pre_exec   = kernel.pre_exec
            cu.executable = kernel.executable
            cu.arguments  = kernel.arguments

            step_01_cus.append(cu)

        session = radical.pilot.Session()
        pmgr = radical.pilot.PilotManager(session=session)

        pdesc = radical.pilot.ComputePilotDescription()
        pdesc.resource = "localhost"
        pdesc.runtime  = 5 # minutes
        pdesc.cores    = 1
        pdesc.cleanup  = True

        pilot = pmgr.submit_pilots(pdesc)

        umgr = radical.pilot.UnitManager(
            session=session,
            scheduler=radical.pilot.SCHED_DIRECT_SUBMISSION)

        umgr.add_pilots(pilot)

        units = umgr.submit_units(step_01_cus)
        umgr.wait_units()

        for unit in units:
            print "* Task %s (executed @ %s) state %s, exit code: %s, started: %s, finished: %s, stdout: %s" \
                % (unit.uid, unit.execution_locations, unit.state, unit.exit_code, unit.start_time, unit.stop_time, unit.stdout)

        session.close()