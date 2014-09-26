#!/usr/bin/env python

"""A static execution plugin for the 'simulation-analysis' pattern.
"""

__author__    = "Ole Weider <ole.weidner@rutgers.edu>"
__copyright__ = "Copyright 2014, http://radical.rutgers.edu"
__license__   = "MIT"

import saga
import radical.pilot 
from radical.ensemblemd.exec_plugins.plugin_base import PluginBase

# ------------------------------------------------------------------------------
# 
_PLUGIN_INFO = {
    "name":         "simulation_analysis_loop.static.default",
    "pattern":      "SimulationAnalysisLoop",
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

        self.get_logger().info("Executing simulation-analysis loop with {0} iterations on {1} allocated core(s) on '{2}'".format(
            pattern.maxiterations, resource._cores, resource._resource_key))

        session = radical.pilot.Session()

        if resource._username is not None:
            # Add an ssh identity to the session.
            c = rp.Context('ssh')
            c.user_id = resource._username
            session.add_context(c)

        pmgr = radical.pilot.PilotManager(session=session)

        pdesc = radical.pilot.ComputePilotDescription()
        pdesc.resource = resource._resource_key
        pdesc.runtime  = resource._walltime
        pdesc.cores    = resource._cores
        pdesc.cleanup  = True

        if resource._allocation is not None:
            pdesc.project = resource._allocation

        self.get_logger().info("Requesting resources on {0}...".format(resource._resource_key))

        pilot = pmgr.submit_pilots(pdesc)

        umgr = radical.pilot.UnitManager(
            session=session,
            scheduler=radical.pilot.SCHED_DIRECT_SUBMISSION)

        umgr.add_pilots(pilot)


        ########################################################################
        working_dirs = {}

        # execute pre_loop
        pre_loop = pattern.pre_loop()
        pre_loop._bind_to_resource(resource._resource_key)

        cu = radical.pilot.ComputeUnitDescription()

        cu.pre_exec       = pre_loop._cu_def_pre_exec
        cu.executable     = pre_loop._cu_def_executable
        cu.arguments      = pre_loop.arguments
        cu.mpi            = pre_loop.uses_mpi
        cu.input_staging  = pre_loop._cu_def_input_data
        cu.output_staging = pre_loop._cu_def_output_data

        unit = umgr.submit_units(cu)
        working_dirs["pre_loop"] = saga.Url(unit.working_directory).path

        self.get_logger().info("Submitted ComputeUnit(s) for pre_loop step.")
        self.get_logger().info("Waiting for ComputeUnit(s) in pre_loop step to complete.")
        umgr.wait_units()


        ########################################################################

        self.get_logger().info("Pattern execution finished finished.")
        session.close()