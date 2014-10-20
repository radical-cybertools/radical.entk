#!/usr/bin/env python

"""A static execution plugin for the 'simulation-analysis' pattern.
"""

__author__    = "Ole Weider <ole.weidner@rutgers.edu>"
__copyright__ = "Copyright 2014, http://radical.rutgers.edu"
__license__   = "MIT"

import time
import saga
import radical.pilot 
from radical.ensemblemd.exceptions import NotImplementedError
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
            pattern.iterations, resource._cores, resource._resource_key))

        try: 

            session = radical.pilot.Session()

            if resource._username is not None:
                # Add an ssh identity to the session.
                c = radical.pilot.Context('ssh')
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
            try: 
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

                self.get_logger().info("Submitted ComputeUnit(s) for pre_loop step.")
                self.get_logger().info("Waiting for ComputeUnit(s) in pre_loop step to complete.")
                umgr.wait_units()

                working_dirs["pre_loop"] = saga.Url(unit.working_directory).path

            except NotImplementedError:
                # Doesn't exist. That's fine as it is not mandatory. 
                self.get_logger().info("pre_loop() not defined. Skipping.")
                pass

            ########################################################################
            # execute simulation analysis loop
            #
            for iteration in range(1, pattern.iterations+1):

                working_dirs['iteration_{0}'.format(iteration)] = {}

                # EXECUTE SIMULATION STEPS
                s_units = []
                for s_instance in range(1, pattern._simulation_instances+1):

                    simulation_step = pattern.simulation_step(iteration=iteration, instance=s_instance)
                    simulation_step._bind_to_resource(resource._resource_key)

                    cud = radical.pilot.ComputeUnitDescription()
                    cud.pre_exec       = simulation_step._cu_def_pre_exec
                    cud.executable     = simulation_step._cu_def_executable
                    cud.arguments      = simulation_step.arguments
                    cud.mpi            = simulation_step.uses_mpi
                    cud.input_staging  = simulation_step._cu_def_input_data
                    cud.output_staging = simulation_step._cu_def_output_data
                    s_units.append(cud)
                    self.get_logger().debug("Created simulation CU: {0}.".format(cud.as_dict()))

                s_cus = umgr.submit_units(s_units)

                self.get_logger().info("Submitted tasks for simulation iteration {0}.".format(iteration))
                self.get_logger().info("Waiting for simulations in iteration {0} to complete.".format(iteration))
                umgr.wait_units()

                # TODO: ensure working_dir <-> instance mapping
                i = 0
                for cu in s_cus:
                    i += 1
                    working_dirs['iteration_{0}'.format(iteration)]['simulation_{0}'.format(i)] = saga.Url(cu.working_directory).path

                # EXECUTE ANALYSIS STEPS
                a_units = []
                for a_instance in range(1, pattern._analysis_instances+1):

                    analysis_step = pattern.analysis_step(iteration=iteration, instance=a_instance)
                    analysis_step._bind_to_resource(resource._resource_key)

                    a_cud = radical.pilot.ComputeUnitDescription()
                    a_cud.pre_exec = []

                    if "pre_loop" in working_dirs:
                        a_cud.pre_exec.append("export PRE_LOOP={0}".format(working_dirs["pre_loop"]))

                    a_cud.pre_exec.append("export PREV_SIMULATION={dir}".format(dir=working_dirs['iteration_{0}'.format(iteration)]['simulation_{0}'.format(a_instance)]))

                    for sim in range(1, pattern._simulation_instances+1):
                        a_cud.pre_exec.append("export PREV_SIMULATION_{inst}={dir}".format(
                            inst=sim,
                            dir=working_dirs['iteration_{0}'.format(iteration)]['simulation_{0}'.format(sim)]
                        ))

                    a_cud.pre_exec.extend(analysis_step._cu_def_pre_exec)

                    a_cud.executable     = analysis_step._cu_def_executable
                    a_cud.arguments      = analysis_step.arguments
                    a_cud.mpi            = analysis_step.uses_mpi
                    a_cud.input_staging  = analysis_step._cu_def_input_data
                    a_cud.output_staging = analysis_step._cu_def_output_data
                    a_units.append(a_cud)
                    self.get_logger().debug("Created simulation CU: {0}.".format(a_cud.as_dict()))

                a_cus = umgr.submit_units(a_units)

                self.get_logger().info("Submitted tasks for analysis iteration {0}.".format(iteration))
                self.get_logger().info("Waiting for analysis tasks in iteration {0} to complete.".format(iteration))
                umgr.wait_units()

                # TODO: ensure working_dir <-> instance mapping
                i = 0
                for cu in a_cus:
                    i += 1
                    working_dirs['iteration_{0}'.format(iteration)]['analysis_{0}'.format(i)] = saga.Url(cu.working_directory).path


        except Exception, ex:
            self.get_logger().error("Fatal error during execution: {0}.".format(str(ex)))

        finally:
            self.get_logger().info("Deallocating resource.")
            session.close()