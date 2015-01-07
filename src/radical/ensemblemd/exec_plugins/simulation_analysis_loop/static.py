#!/usr/bin/env python

"""A static execution plugin for the 'simulation-analysis' pattern.
"""

__author__    = "Ole Weider <ole.weidner@rutgers.edu>"
__copyright__ = "Copyright 2014, http://radical.rutgers.edu"
__license__   = "MIT"

import time
import saga
import radical.pilot
from radical.ensemblemd.exceptions import NotImplementedError, EnsemblemdError
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
def create_env_vars(working_dirs, instance, iteration, sim_width, ana_width, type):
    env_vars = dict()

    #  * ``$PRE_LOOP`` - References the pre_loop step.
    if "pre_loop" in working_dirs:
        env_vars["PRE_LOOP"] = working_dirs["pre_loop"]

    #  * ``$PREV_SIMULATION`` - References the previous simulation step with the same instance number.
    if sim_width == ana_width:
        if type == "analysis" and iteration > 1:
            env_vars["PREV_SIMULATION"] = working_dirs['iteration_{0}'.format(iteration)]['simulation_{0}'.format(instance)]

    #  * ``$PREV_SIMULATION_INSTANCE_Y`` - References instance Y of the previous simulation step.
    if type == "analysis" and iteration >= 1:
        for inst in range(1, sim_width+1):
            env_vars["PREV_SIMULATION_INSTANCE_{0}".format(inst)] = working_dirs['iteration_{0}'.format(iteration)]['simulation_{0}'.format(inst)]

    #  * ``$SIMULATION_ITERATION_X_INSTANCE_Y`` - Refernces instance Y of the simulation step of iteration number X.

    #  * ``$PREV_ANALYSIS`` - References the previous analysis step with the same instance number.
    if sim_width == ana_width:
        if type == "simulation" and iteration > 1:
            env_vars["PREV_ANALYSIS"] = working_dirs['iteration_{0}'.format(iteration-1)]['analysis_{0}'.format(instance)]

    #  * ``$PREV_ANALYSIS_INSTANCE_Y`` - References instance Y of the previous analysis step.
    if type == "simulation" and iteration > 1:
        for inst in range(1, ana_width+1):
            env_vars["PREV_ANALYSIS_INSTANCE_{0}".format(inst)] = working_dirs['iteration_{0}'.format(iteration-1)]['simulation_{0}'.format(inst)]

    #  * ``$ANALYSIS_ITERATION_X_INSTANCE_Y`` - Refernces instance Y of the analysis step of iteration number X.
    return env_vars


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

            if resource._queue is not None:
                pdesc.queue = resource._queue

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
                self.get_logger().info("Pre_loop completed.")

                if unit.state != radical.pilot.DONE:
                    raise EnsemblemdError("Pre-loop CU failed with error: {0}".format(unit.stdout))

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

                ################################################################
                # EXECUTE PRE-SIMULATION STEP
                s_units = []

                simulation_list = pattern.simulation_step(iteration=iteration, instance=1)

                pre_sim_step = simulation_list[0]
                pre_sim_step._bind_to_resource(resource._resource_key)

                cud = radical.pilot.ComputeUnitDescription()
                cud.pre_exec = []

                env_vars = create_env_vars(working_dirs, 1, iteration, pattern._simulation_instances, pattern._analysis_instances, type="simulation")
                for var, value in env_vars.iteritems():
                    cud.pre_exec.append("export {var}={value}".format(var=var, value=value))

                cud.pre_exec.extend(pre_sim_step._cu_def_pre_exec)

                cud.executable     = pre_sim_step._cu_def_executable
                cud.arguments      = pre_sim_step.arguments
                cud.mpi            = pre_sim_step.uses_mpi
                cud.input_staging  = pre_sim_step._cu_def_input_data
                cud.output_staging = pre_sim_step._cu_def_output_data
                s_units.append(cud)
                self.get_logger().debug("Created pre_simulation CU: {0}.".format(cud.as_dict()))

                s_cus = umgr.submit_units(s_units)

                self.get_logger().info("Submitted tasks for pre_simulation iteration {0}.".format(iteration))
                self.get_logger().info("Waiting for pre_simulations in iteration {0} to complete.".format(iteration))
                umgr.wait_units()
                self.get_logger().info("Pre_simulations in iteration {0} completed.".format(iteration))

                failed_units = ""
                pre_sim_wd = ""
                for unit in s_cus:
                    if unit.state != radical.pilot.DONE:
                        failed_units += " * Pre Simulation task {0} failed with an error: {1}\n".format(unit.uid, unit.stderr)
                    else:
                        pre_sim_wd = saga.Url(unit.working_directory).path

                ################################################################
                # EXECUTE SIMULATION STEPS

                s_units = []
                for s_instance in range(1, pattern._simulation_instances+1):

                    simulation_list = pattern.simulation_step(iteration=iteration, instance=s_instance)

                    sim_step = simulation_list[1]
                    sim_step._bind_to_resource(resource._resource_key)

                    cud = radical.pilot.ComputeUnitDescription()
                    cud.pre_exec = []

                    env_vars = create_env_vars(working_dirs, s_instance, iteration, pattern._simulation_instances, pattern._analysis_instances, type="simulation")
                    for var, value in env_vars.iteritems():
                        cud.pre_exec.append("export {var}={value}".format(var=var, value=value))

                    cud.pre_exec.extend(sim_step._cu_def_pre_exec)
                    cud.pre_exec.append('ln -s {0}/temp/start{1}.gro start.gro'.format(pre_sim_wd,s_instance-1))
                    cud.executable     = sim_step._cu_def_executable
                    cud.arguments      = sim_step.arguments
                    cud.mpi            = sim_step.uses_mpi
                    cud.input_staging  = sim_step._cu_def_input_data
                    cud.output_staging = sim_step._cu_def_output_data
                    s_units.append(cud)
                    self.get_logger().debug("Created simulation CU: {0}.".format(cud.as_dict()))

                s_cus = umgr.submit_units(s_units)

                self.get_logger().info("Submitted tasks for simulation iteration {0}.".format(iteration))
                self.get_logger().info("Waiting for simulations in iteration {0} to complete.".format(iteration))
                umgr.wait_units()
                self.get_logger().info("Simulations in iteration {0} completed.".format(iteration))

                failed_units = ""
                for unit in s_cus:
                    if unit.state != radical.pilot.DONE:
                        failed_units += " * Simulation task {0} failed with an error: {1}\n".format(unit.uid, unit.stderr)

                # TODO: ensure working_dir <-> instance mapping
                i = 0
                for cu in s_cus:
                    i += 1
                    working_dirs['iteration_{0}'.format(iteration)]['simulation_{0}'.format(i)] = saga.Url(cu.working_directory).path

                ################################################################
                # EXECUTE PRE-ANALYSIS STEPS

                a_units = []
                analysis_list = pattern.analysis_step(iteration=iteration, instance=1)
                pre_ana_step = analysis_list[0]
                pre_ana_step._bind_to_resource(resource._resource_key)

                cud = radical.pilot.ComputeUnitDescription()
                cud.pre_exec = []

                env_vars = create_env_vars(working_dirs, 1, iteration, pattern._simulation_instances, pattern._analysis_instances, type="analysis")
                for var, value in env_vars.iteritems():
                    cud.pre_exec.append("export {var}={value}".format(var=var, value=value))

                cud.pre_exec.extend(pre_ana_step._cu_def_pre_exec)

                cud.executable     = pre_ana_step._cu_def_executable
                cud.arguments      = pre_ana_step.arguments
                cud.mpi            = pre_ana_step.uses_mpi
                cud.input_staging  = pre_ana_step._cu_def_input_data
                cud.output_staging = pre_ana_step._cu_def_output_data

                a_units.append(cud)
                self.get_logger().debug("Created pre-analysis CU: {0}.".format(cud.as_dict()))

                a_cus = umgr.submit_units(a_units)

                self.get_logger().info("Submitted tasks for pre-analysis iteration {0}.".format(iteration))
                self.get_logger().info("Waiting for pre-analysis tasks in iteration {0} to complete.".format(iteration))
                umgr.wait_units()
                self.get_logger().info("Pre-Analysis in iteration {0} completed.".format(iteration))

                failed_units = ""
                pre_ana_wd = ""
                for unit in a_cus:
                    if unit.state != radical.pilot.DONE:
                        failed_units += " * Pre-Analysis task {0} failed with an error: {1}\n".format(unit.uid, unit.stderr)
                    else:
                        pre_ana_wd = saga.Url(unit.working_directory).path

                '''
                ################################################################
                # EXECUTE ANALYSIS STEPS
                a_units = []
                for a_instance in range(1, pattern._analysis_instances+1):

                    analysis_step = pattern.analysis_step(iteration=iteration, instance=a_instance)
                    analysis_step._bind_to_resource(resource._resource_key)

                    cud = radical.pilot.ComputeUnitDescription()
                    cud.pre_exec = []

                    env_vars = create_env_vars(working_dirs, a_instance, iteration, pattern._simulation_instances, pattern._analysis_instances, type="analysis")
                    for var, value in env_vars.iteritems():
                        cud.pre_exec.append("export {var}={value}".format(var=var, value=value))

                    cud.pre_exec.extend(analysis_step._cu_def_pre_exec)

                    cud.executable     = analysis_step._cu_def_executable
                    cud.arguments      = analysis_step.arguments
                    cud.mpi            = analysis_step.uses_mpi
                    cud.input_staging  = analysis_step._cu_def_input_data
                    cud.output_staging = analysis_step._cu_def_output_data
                    a_units.append(cud)
                    self.get_logger().debug("Created simulation CU: {0}.".format(cud.as_dict()))

                a_cus = umgr.submit_units(a_units)

                self.get_logger().info("Submitted tasks for analysis iteration {0}.".format(iteration))
                self.get_logger().info("Waiting for analysis tasks in iteration {0} to complete.".format(iteration))
                umgr.wait_units()

                failed_units = ""
                for unit in a_cus:
                    if unit.state != radical.pilot.DONE:
                        failed_units += " * Analysis task {0} failed with an error: {1}\n".format(unit.uid, unit.stderr)
                        
                if len(failed_units) > 0:
                    raise EnsemblemdError("One or more ComputeUnits failed in pipeline step {0}: \n{1}".format(step, failed_units))


                # TODO: ensure working_dir <-> instance mapping
                i = 0
                for cu in a_cus:
                    i += 1
                    working_dirs['iteration_{0}'.format(iteration)]['analysis_{0}'.format(i)] = saga.Url(cu.working_directory).path
                '''

        except Exception, ex:
            self.get_logger().error("Fatal error during execution: {0}.".format(str(ex)))
            raise

        finally:
            self.get_logger().info("Deallocating resource.")
            session.close()
