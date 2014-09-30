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

        #-----------------------------------------------------------------------
        #
        def pilot_state_cb (pilot, state) :
            self.get_logger().info("Resource {0} state has changed to {1}".format(
                resource._resource_key, state))

            if state == radical.pilot.FAILED:
                self.get_logger().error("Resource error: {0}".format(pilot.log))
                self.get_logger().error("Pattern execution FAILED.")

        #-----------------------------------------------------------------------
        #
        def unit_state_cb (unit, state) :

            if state == radical.pilot.FAILED:
                self.get_logger().error("ComputeUnit error: STDERR: {0}, STDOUT: {0}".format(unit.stderr, unit.stdout))
                self.get_logger().error("Pattern execution FAILED.") 


        self.get_logger().info("Executing simulation-analysis loop with {0} iterations on {1} allocated core(s) on '{2}'".format(
            pattern.maxiterations, resource._cores, resource._resource_key))

        try: 

            session = radical.pilot.Session()

            if resource._username is not None:
                # Add an ssh identity to the session.
                c = rp.Context('ssh')
                c.user_id = resource._username
                session.add_context(c)

            pmgr = radical.pilot.PilotManager(session=session)
            pmgr.register_callback(pilot_state_cb)

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
            umgr.register_callback(unit_state_cb)

            umgr.add_pilots(pilot)


            working_dirs = {}

            ########################################################################
            # execute pre_loop
            #
            pre_loop = pattern.pre_loop()
            pre_loop._bind_to_resource(resource._resource_key)

            cu = radical.pilot.ComputeUnitDescription()

            cu.pre_exec       = pre_loop._cu_def_pre_exec
            cu.executable     = pre_loop._cu_def_executable
            cu.arguments      = pre_loop.arguments
            cu.mpi            = pre_loop.uses_mpi
            cu.input_staging  = pre_loop._cu_def_input_data
            cu.output_staging = pre_loop._cu_def_output_data

            self.get_logger().debug("Created pre_loop CU: {0}.".format(cu.as_dict()))

            unit = umgr.submit_units(cu)
            working_dirs["pre_loop"] = saga.Url(unit.working_directory).path

            self.get_logger().info("Submitted ComputeUnit(s) for pre_loop step.")
            self.get_logger().info("Waiting for ComputeUnit(s) in pre_loop step to complete.")
            umgr.wait_units()

            ########################################################################
            # execute simulation analysis loop
            #
            for iteration in range(1, pattern.maxiterations+1):

                for s_instance in range(1, pattern._simulation_instances+1):
                    # EXECUTE SIMULATION STEPS
                    simulation_step = pattern.simulation_step(iteration=iteration, instance=s_instance)
                    simulation_step._bind_to_resource(resource._resource_key)

                    cu = radical.pilot.ComputeUnitDescription()
                    cu.pre_exec       = simulation_step._cu_def_pre_exec
                    cu.executable     = simulation_step._cu_def_executable
                    cu.arguments      = simulation_step.arguments
                    cu.mpi            = simulation_step.uses_mpi
                    cu.input_staging  = simulation_step._cu_def_input_data
                    cu.output_staging = simulation_step._cu_def_output_data

                    self.get_logger().debug("Created simulation CU: {0}.".format(cu.as_dict()))


                for a_instance in range(1, pattern._analysis_instances+1):
                    # EXECUTE ANALYSIS STEPS
                    analysis_step = pattern.analysis_step(iteration=iteration, instance=s_instance)
                    analysis_step._bind_to_resource(resource._resource_key)

                    cu = radical.pilot.ComputeUnitDescription()
                    cu.pre_exec       = analysis_step._cu_def_pre_exec
                    cu.executable     = analysis_step._cu_def_executable
                    cu.arguments      = analysis_step.arguments
                    cu.mpi            = analysis_step.uses_mpi
                    cu.input_staging  = analysis_step._cu_def_input_data
                    cu.output_staging = analysis_step._cu_def_output_data

                    self.get_logger().debug("Created analysis CU: {0}.".format(cu.as_dict()))


        except Exception, ex:
            self.get_logger().error("Fatal error during execution: {0}.".format(str(ex)))

        finally:
            self.get_logger().info("Deallocating resource.")
            session.close()