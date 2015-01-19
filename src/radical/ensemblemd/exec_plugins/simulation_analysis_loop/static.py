#!/usr/bin/env python

"""A static execution plugin for the 'simulation-analysis' pattern.
"""

__author__    = "Ole Weider <ole.weidner@rutgers.edu>"
__copyright__ = "Copyright 2014, http://radical.rutgers.edu"
__license__   = "MIT"

import os
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
        if type == "analysis":
            env_vars["PREV_SIMULATION"] = working_dirs['iteration_{0}'.format(iteration)]['simulation_{0}'.format(instance)]

    #  * ``$PREV_SIMULATION_INSTANCE_Y`` - References instance Y of the previous simulation step.
    if type == "analysis" and iteration >= 1:
        for inst in range(1, sim_width+1):
            env_vars["PREV_SIMULATION_INSTANCE_{0}".format(inst)] = working_dirs['iteration_{0}'.format(iteration)]['simulation_{0}'.format(inst)]

    #  * ``$SIMULATION_ITERATION_X_INSTANCE_Y`` - Refernces instance Y of the simulation step of iteration number X.
    if type == "analysis" and iteration >= 1:
        for iter in range(1,iteration+1):
            for inst in range(1,sim_width+1):
                env_vars["SIMULATION_ITERATION_{1}_INSTANCE_{0}".format(inst,iter)] = working_dirs['iteration_{0}'.format(iter)]['simulation_{0}'.format(inst)]

    #  * ``$PREV_ANALYSIS`` - References the previous analysis step with the same instance number.
    if sim_width == ana_width:
        if type == "simulation" and iteration > 1:
            env_vars["PREV_ANALYSIS"] = working_dirs['iteration_{0}'.format(iteration-1)]['analysis_{0}'.format(instance)]

    #  * ``$PREV_ANALYSIS_INSTANCE_Y`` - References instance Y of the previous analysis step.
    if (type == "simulation" or type == 'analysis') and iteration > 1:
        for inst in range(1, ana_width+1):
            env_vars["PREV_ANALYSIS_INSTANCE_{0}".format(inst)] = working_dirs['iteration_{0}'.format(iteration-1)]['analysis_{0}'.format(inst)]

    #  * ``$ANALYSIS_ITERATION_X_INSTANCE_Y`` - Refernces instance Y of the analysis step of iteration number X.
    if (type == "simulation" or type == 'analysis') and iteration > 1:
        for iter in range(1,iteration):
            for inst in range(1,ana_width+1):
                env_vars["ANALYSIS_ITERATION_{1}_INSTANCE_{0}".format(inst,iter)] = working_dirs['iteration_{0}'.format(iter)]['analysis_{0}'.format(inst)]

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
    def verify_pattern(self, pattern, resource):

        # THROW ERRROR IF PROFILING IS NOT IMPLEMENTED TO AVOID
        # FRUSTARTION AT THE NED
        do_profile = os.getenv('RADICAL_ENDM_PROFILING', '0')

        if do_profile != '0':
            # add profiling code here
            resource.deallocate()
            raise EnsemblemdError("RADICAL_ENDM_PROFILING set but profiling is not implemented for this pattern yet.")

    # --------------------------------------------------------------------------
    #
    def execute_pattern(self, pattern, resource):

        #-----------------------------------------------------------------------
        #
        def unit_state_cb (unit, state) :

            if state == radical.pilot.FAILED:
                self.get_logger().error("ComputeUnit error: STDERR: {0}, STDOUT: {0}".format(unit.stderr, unit.stdout))
                self.get_logger().error("Pattern execution FAILED.")


        self.get_logger().info("Executing simulation-analysis loop with {0} iterations on {1} allocated core(s) on '{2}'".format(pattern.iterations, resource._cores, resource._resource_key))

        try:
            resource._umgr.register_callback(unit_state_cb)

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

                unit = resource._umgr.submit_units(cu)

                self.get_logger().info("Submitted ComputeUnit(s) for pre_loop step.")
                self.get_logger().info("Waiting for ComputeUnit(s) in pre_loop step to complete.")
                resource._umgr.wait_units()
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
                # EXECUTE SIMULATION STEPS

                s_units = []
                for s_instance in range(1, pattern._simulation_instances+1):

                    sim_step = pattern.simulation_step(iteration=iteration, instance=s_instance)

                    sim_step._bind_to_resource(resource._resource_key)

                    cud = radical.pilot.ComputeUnitDescription()
                    cud.pre_exec = []

                    env_vars = create_env_vars(working_dirs, s_instance, iteration, pattern._simulation_instances, pattern._analysis_instances, type="simulation")
                    for var, value in env_vars.iteritems():
                        cud.pre_exec.append("export {var}={value}".format(var=var, value=value))

                    cud.pre_exec.extend(sim_step._cu_def_pre_exec)
                    cud.executable     = sim_step._cu_def_executable
                    cud.arguments      = sim_step.arguments
                    cud.mpi            = sim_step.uses_mpi
                    cud.input_staging  = sim_step._cu_def_input_data
                    cud.output_staging = sim_step._cu_def_output_data
                    s_units.append(cud)
                    self.get_logger().debug("Created simulation CU: {0}.".format(cud.as_dict()))

                s_cus = resource._umgr.submit_units(s_units)

                self.get_logger().info("Submitted tasks for simulation iteration {0}.".format(iteration))
                self.get_logger().info("Waiting for simulations in iteration {0} to complete.".format(iteration))
                resource._umgr.wait_units()
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
                # EXECUTE ANALYSIS STEPS

                a_units = []
                analysis_list = None
                for a_instance in range(1, pattern._analysis_instances+1):

                    analysis_list = pattern.analysis_step(iteration=iteration, instance=a_instance)

                    if not isinstance(analysis_list,list):
                        analysis_list = [analysis_list]

                    if len(analysis_list) > 1:

                        kernel_wd = ""
                        cur_kernel = 1

                        for ana_step in analysis_list:

                            a_units = []
                            ana_step._bind_to_resource(resource._resource_key)

                            cud = radical.pilot.ComputeUnitDescription()
                            cud.pre_exec = []

                            env_vars = create_env_vars(working_dirs, 1, iteration, pattern._simulation_instances, pattern._analysis_instances, type="analysis")
                            for var, value in env_vars.iteritems():
                                cud.pre_exec.append("export {var}={value}".format(var=var, value=value))

                            cud.pre_exec.extend(ana_step._cu_def_pre_exec)
                            if cur_kernel > 1:
                                cud.pre_exec.append('cp %s/*.* .'%kernel_wd)

                            cud.executable     = ana_step._cu_def_executable
                            cud.arguments      = ana_step.arguments
                            cud.mpi            = ana_step.uses_mpi
                            cud.input_staging  = ana_step._cu_def_input_data
                            cud.output_staging = ana_step._cu_def_output_data

                            a_units.append(cud)
                            self.get_logger().debug("Created analysis CU: {0}.".format(cud.as_dict()))

                            a_cus = resource._umgr.submit_units(a_units)

                            self.get_logger().info("Submitted tasks for analysis iteration {0}/ kernel {1}.".format(iteration,cur_kernel))
                            self.get_logger().info("Waiting for analysis tasks in iteration {0}/kernel {1} to complete.".format(iteration,cur_kernel))
                            resource._umgr.wait_units()
                            self.get_logger().info("Analysis in iteration {0}/kernel {1} completed.".format(iteration,cur_kernel))

                            failed_units = ""
                            for unit in a_cus:
                                if unit.state != radical.pilot.DONE:
                                    failed_units += " * Analysis task {0} failed with an error: {1}\n".format(unit.uid, unit.stderr)
                                else:
                                    kernel_wd = saga.Url(unit.working_directory).path
                                    cur_kernel += 1
                                    working_dirs['iteration_{0}'.format(iteration)]['analysis_1'] = saga.Url(unit.working_directory).path

                    else:
                        analysis_step = analysis_list[0]
                        analysis_step._bind_to_resource(resource._resource_key)

                        cud = radical.pilot.ComputeUnitDescription()
                        cud.pre_exec = []
                        env_vars = create_env_vars(working_dirs, a_instance, iteration, pattern._simulation_instances, pattern._analysis_instances, type="analysis")
                        for var, value in env_vars.iteritems():
                            cud.pre_exec.append("export {var}={value}".format(var=var, value=value))

                        cud.pre_exec.extend(analysis_step._cu_def_pre_exec)

                        cud.executable = analysis_step._cu_def_executable
                        cud.arguments = analysis_step.arguments
                        cud.mpi = analysis_step.uses_mpi
                        cud.input_staging = analysis_step._cu_def_input_data
                        cud.output_staging = analysis_step._cu_def_output_data
                        a_units.append(cud)

                        self.get_logger().debug("Created analysis CU: {0}.".format(cud.as_dict()))

                if len(analysis_list)==1:
                    a_cus = resource._umgr.submit_units(a_units)

                    self.get_logger().info("Submitted tasks for analysis iteration {0}.".format(iteration))
                    self.get_logger().info("Waiting for analysis tasks in iteration {0} to complete.".format(iteration))
                    resource._umgr.wait_units()
                    self.get_logger().info("Analysis in iteration {0} completed.".format(iteration))


                    failed_units = ""
                    for unit in a_cus:
                        if unit.state != radical.pilot.DONE:
                            failed_units += " * Analysis task {0} failed with an error: {1}\n".format(unit.uid, unit.stderr)

                        # TODO: ensure working_dir <-> instance mapping
                        i = 0
                        for cu in a_cus:
                            i += 1
                            working_dirs['iteration_{0}'.format(iteration)]['analysis_{0}'.format(i)] = saga.Url(cu.working_directory).path


        except Exception, ex:
            self.get_logger().error("Fatal error during execution: {0}.".format(str(ex)))
            raise

        finally:
            self.get_logger().info("Deallocating resource.")
            resource.deallocate()

        # -----------------------------------------------------------------
        # At this point, we have executed the pattern succesfully. Now,
        # if profiling is enabled, we can write the profiling data to
        # a file.
        do_profile = os.getenv('RADICAL_ENDM_PROFILING', '0')

        if do_profile != '0':
            # add profiling code here
            pass
