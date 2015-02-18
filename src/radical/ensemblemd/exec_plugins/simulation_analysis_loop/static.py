#!/usr/bin/env python

"""A static execution plugin for the 'simulation-analysis' pattern.
"""

__author__    = "Ole Weider <ole.weidner@rutgers.edu>"
__copyright__ = "Copyright 2014, http://radical.rutgers.edu"
__license__   = "MIT"

import os
import time
import saga
import datetime
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
def resolve_placeholder_vars(working_dirs, instance, iteration, sim_width, ana_width, type, path):

    # Extract placeholder from path
    if path.startswith('$'):
        placeholder = path.split('/')[0]

    # $PRE_LOOP
    if placeholder == "$PRE_LOOP":
        return path.replace(placeholder, working_dirs["pre_loop"])

    # $POST_LOOP
    elif placeholder == "$POST_LOOP":
        return path.replace(placeholder, working_dirs["post_loop"])

    # $PREV_SIMULATION
    elif placeholder == "$PREV_SIMULATION":
        if sim_width == ana_width:
            if type == "analysis":
                return path.replace(placeholder, working_dirs['iteration_{0}'.format(iteration)]['simulation_{0}'.format(instance)])
            else:
                raise Exception("$PREV_SIMULATION can only be referenced within analysis step. ")
        else:
            raise Exception("Simulation and analysis 'width' need to be identical for $PREV_SIMULATION to work.")

    # $PREV_ANALYSIS
    elif placeholder == "$PREV_ANALYSIS":
        if sim_width == ana_width:
            if type == "simulation":
                return path.replace(placeholder, working_dirs['iteration_{0}'.format(iteration-1)]['analysis_{0}'.format(instance)])
            else:
                raise Exception("$PREV_ANALYSIS can only be referenced within simulation step. ")
        else:
            raise Exception("Simulation and analysis 'width' need to be identical for $PREV_SIMULATION to work.")

    # $PREV_SIMULATION_INSTANCE_Y
    elif placeholder.startswith("$PREV_SIMULATION_INSTANCE_"):
        y = placeholder.split("$PREV_SIMULATION_INSTANCE_")[1]
        if type == "analysis" and iteration >= 1:
            return path.replace(placeholder, working_dirs['iteration_{0}'.format(iteration)]['simulation_{0}'.format(y)])
        else:
            raise Exception("$PREV_SIMULATION_INSTANCE_Y used in invalid context.")

    # $PREV_ANALYSIS_INSTANCE_Y
    elif placeholder.startswith("$PREV_ANALYSIS_INSTANCE_"):
        y = placeholder.split("$PREV_ANALYSIS_INSTANCE_")[1]
        if type == "simulation" and iteration > 1:
            return path.replace(placeholder, working_dirs['iteration_{0}'.format(iteration-1)]['analysis_{0}'.format(y)])
        else:
            raise Exception("$PREV_ANALYSIS_INSTANCE_Y used in invalid context.")

    # $SIMULATION_ITERATION_X_INSTANCE_Y
    elif placeholder.startswith("$SIMULATION_ITERATION_"):
        x = placeholder.split("_")[2]
        y = placeholder.split("_")[4]
        if type == "analysis" and iteration >= 1:
            return path.replace(placeholder, working_dirs['iteration_{0}'.format(x)]['simulation_{0}'.format(y)])
        else:
            raise Exception("$SIMULATION_ITERATION_X_INSTANCE_Y used in invalid context.")

    # $ANALYSIS_ITERATION_X_INSTANCE_Y
    elif placeholder.startswith("$ANALYSIS_ITERATION_"):
        x = placeholder.split("_")[2]
        y = placeholder.split("_")[4]
        if (type == 'simulation' or type == "analysis") and iteration >= 1:
            return path.replace(placeholder, working_dirs['iteration_{0}'.format(x)]['analysis_{0}'.format(y)])
        else:
            raise Exception("$ANALYSIS_ITERATION_X_INSTANCE_Y used in invalid context.")

    # Nothing to replace here...
    else:
        return path

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
        pass

    # --------------------------------------------------------------------------
    #
    def execute_pattern(self, pattern, resource):

        pattern_start_time = time.time()

        #-----------------------------------------------------------------------
        #
        def unit_state_cb (unit, state) :

            if state == radical.pilot.FAILED:
                self.get_logger().error("ComputeUnit error: STDERR: {0}, STDOUT: {0}".format(unit.stderr, unit.stdout))
                self.get_logger().error("Pattern execution FAILED.")


        self.get_logger().info("Executing simulation-analysis loop with {0} iterations on {1} allocated core(s) on '{2}'".format(pattern.iterations, resource._cores, resource._resource_key))

        working_dirs = {}
        all_cus = []

        pattern._execution_profile = {}

        try:
            resource._umgr.register_callback(unit_state_cb)

            ########################################################################
            # execute pre_loop
            #
            try:
                pre_loop = pattern.pre_loop()
                pre_loop._bind_to_resource(resource._resource_key)

                cu = radical.pilot.ComputeUnitDescription()
                cu.name = "pre_loop"

                cu.pre_exec       = pre_loop._cu_def_pre_exec
                cu.executable     = pre_loop._cu_def_executable
                cu.arguments      = pre_loop.arguments
                cu.mpi            = pre_loop.uses_mpi
                cu.input_staging  = pre_loop._cu_def_input_data
                cu.output_staging = pre_loop._cu_def_output_data

                self.get_logger().debug("Created pre_loop CU: {0}.".format(cu.as_dict()))

                unit = resource._umgr.submit_units(cu)
                all_cus.append(unit)

                self.get_logger().info("Submitted ComputeUnit(s) for pre_loop step.")
                self.get_logger().info("Waiting for ComputeUnit(s) in pre_loop step to complete.")
                resource._umgr.wait_units()
                self.get_logger().info("Pre_loop completed.")

                if unit.state != radical.pilot.DONE:
                    raise EnsemblemdError("Pre-loop CU failed with error: {0}".format(unit.stdout))

                working_dirs["pre_loop"] = saga.Url(unit.working_directory).path

            except Exception:
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
                pattern._execution_profile["sim_{0}_start_time".format(iteration)] = time.time() - pattern_start_time

                s_units = []
                for s_instance in range(1, pattern._simulation_instances+1):

                    sim_step = pattern.simulation_step(iteration=iteration, instance=s_instance)

                    sim_step._bind_to_resource(resource._resource_key)

                    # Resolve all placeholders
                    if sim_step.link_input_data is not None:
                        for i in range(len(sim_step.link_input_data)):
                            sim_step.link_input_data[i] = resolve_placeholder_vars(working_dirs, s_instance, iteration, pattern._simulation_instances, pattern._analysis_instances, "simulation", sim_step.link_input_data[i])

                    cud = radical.pilot.ComputeUnitDescription()
                    cud.name = "sim ;{iteration} ;{instance}".format(iteration=iteration, instance=s_instance)

                    cud.pre_exec       = sim_step._cu_def_pre_exec
                    cud.executable     = sim_step._cu_def_executable
                    cud.arguments      = sim_step.arguments
                    cud.mpi            = sim_step.uses_mpi
                    cud.input_staging  = sim_step._cu_def_input_data
                    cud.output_staging = sim_step._cu_def_output_data

                    # This is a good time to replace all placeholders in the
                    # pre_exec list.


                    s_units.append(cud)
                    self.get_logger().debug("Created simulation CU: {0}.".format(cud.as_dict()))

                s_cus = resource._umgr.submit_units(s_units)
                all_cus.extend(s_cus)

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

                pattern._execution_profile["sim_{0}_end_time".format(iteration)] = time.time() - pattern_start_time


                ################################################################
                # EXECUTE ANALYSIS STEPS
                pattern._execution_profile["ana_{0}_start_time".format(iteration)] = time.time() - pattern_start_time

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

                            # Resolve all placeholders
                            if ana_step.link_input_data is not None:
                                for i in range(len(ana_step.link_input_data)):
                                    ana_step.link_input_data[i] = resolve_placeholder_vars(working_dirs, a_instance, iteration, pattern._simulation_instances, pattern._analysis_instances, "analysis", ana_step.link_input_data[i])

                            cud = radical.pilot.ComputeUnitDescription()
                            cud.name = "ana ; {iteration}; {instance}".format(iteration=iteration, instance=a_instance)

                            cud.pre_exec       = ana_step._cu_def_pre_exec
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
                            all_cus.extend(a_cus)

                            self.get_logger().info("Submitted tasks for analysis iteration {0}/ kernel {1}.".format(iteration,cur_kernel))
                            self.get_logger().info("Waiting for analysis tasks in iteration {0}/kernel {1} to complete.".format(iteration,cur_kernel))
                            resource._umgr.wait_units()
                            self.get_logger().info("Analysis in iteration {0}/kernel {1}:{2} completed.".format(iteration,cur_kernel,ana_step.name))

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

                        # Resolve all placeholders
                        if analysis_step.link_input_data is not None:
                            for i in range(len(analysis_step.link_input_data)):
                                analysis_step.link_input_data[i] = resolve_placeholder_vars(working_dirs, a_instance, iteration, pattern._simulation_instances, pattern._analysis_instances, "analysis", analysis_step.link_input_data[i])

                        cud = radical.pilot.ComputeUnitDescription()
                        cud.name = "ana; {iteration};{instance}".format(iteration=iteration, instance=a_instance)

                        cud.pre_exec       = analysis_step._cu_def_pre_exec
                        cud.executable     = analysis_step._cu_def_executable
                        cud.arguments      = analysis_step.arguments
                        cud.mpi            = analysis_step.uses_mpi
                        cud.input_staging  = analysis_step._cu_def_input_data
                        cud.output_staging = analysis_step._cu_def_output_data

                        a_units.append(cud)

                        self.get_logger().debug("Created analysis CU: {0}.".format(cud.as_dict()))

                if len(analysis_list)==1:
                    a_cus = resource._umgr.submit_units(a_units)
                    all_cus.extend(a_cus)


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

                pattern._execution_profile["ana_{0}_end_time".format(iteration)] = time.time() - pattern_start_time

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
        do_profile = os.getenv('RADICAL_ENMD_PROFILING', '0')

        if do_profile != '0':

            outfile = "execution_profile_{time}.csv".format(time=datetime.datetime.now().isoformat())
            self.get_logger().info("Saving execution profile in {outfile}".format(outfile=outfile))

            with open(outfile, 'w+') as f:
                # General format of a profiling file is row based and follows the
                # structure <unit id>; <s_time>; <stop_t>; <tag1>; <tag2>; ...
                head = "task; start_time; stop_time; stage; iteration; instance"
                f.write("{row}\n".format(row=head))

                for cu in all_cus:
                    row = "{uid}; {start_time}; {stop_time}; {tags}".format(
                        uid=cu.uid,
                        start_time=cu.start_time,
                        stop_time=cu.stop_time,
                        tags=cu.name
                    )
                    f.write("{row}\n".format(row=row))
