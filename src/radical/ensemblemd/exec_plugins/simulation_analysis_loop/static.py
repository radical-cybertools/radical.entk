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
from radical.ensemblemd.utils import extract_timing_info
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

    #source placeholder
    if path.startswith('$'):
        placeholder = path.split('/')[0]

    #dest placeholder
    else:
        placeholder = path.split('>')[1].strip().split('/')[0]

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

        pattern_start_time = datetime.datetime.now()

        #-----------------------------------------------------------------------
        #
        def unit_state_cb (unit, state) :

            if state == radical.pilot.FAILED:
                self.get_logger().error("ComputeUnit error: STDERR: {0}, STDOUT: {0}".format(unit.stderr, unit.stdout))
                self.get_logger().error("Pattern execution FAILED.")


        self.get_logger().info("Executing simulation-analysis loop with {0} iterations on {1} allocated core(s) on '{2}'".format(pattern.iterations, resource._cores, resource._resource_key))

        working_dirs = {}
        all_cus = []

        pattern._execution_profile = []

        try:
            resource._umgr.register_callback(unit_state_cb)

            ########################################################################
            # execute pre_loop
            #
            try:
                ################################################################
                # EXECUTE PRE-LOOP

                step_timings = {
                    "name": "pre_loop",
                    "timings": {}
                }
                step_start_time_abs = datetime.datetime.now()

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

                step_end_time_abs = datetime.datetime.now()

                if unit.state != radical.pilot.DONE:
                    raise EnsemblemdError("Pre-loop CU failed with error: {0}".format(unit.stdout))
                pre_loop_cu = [unit]
                working_dirs["pre_loop"] = saga.Url(unit.working_directory).path

                # Process CU information and append it to the dictionary
                tinfo = extract_timing_info(pre_loop_cu, pattern_start_time, step_start_time_abs, step_end_time_abs)

                for key, val in tinfo.iteritems():
                    step_timings['timings'][key] = val

                # Write the whole thing to the profiling dict
                pattern._execution_profile.append(step_timings)

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
                step_timings = {
                    "name": "simulation_iteration_{0}".format(iteration),
                    "timings": {}
                }
                step_start_time_abs = datetime.datetime.now()

                if isinstance(pattern.simulation_step(iteration=1, instance=1),list):
                    num_sim_kerns = len(pattern.simulation_step(iteration=1, instance=1))
                else:
                    num_sim_kerns = 1
                #print num_sim_kerns
                for kern_step in range(0,num_sim_kerns):

                    s_units = []
                    for s_instance in range(1, pattern._simulation_instances+1):

                        if isinstance(pattern.simulation_step(iteration=iteration, instance=s_instance),list):
                            sim_step = pattern.simulation_step(iteration=iteration, instance=s_instance)[kern_step]
                        else:
                            sim_step = pattern.simulation_step(iteration=iteration, instance=s_instance)

                        sim_step._bind_to_resource(resource._resource_key)

                        # Resolve all placeholders
                        #if sim_step.link_input_data is not None:
                        #    for i in range(len(sim_step.link_input_data)):
                        #        sim_step.link_input_data[i] = resolve_placeholder_vars(working_dirs, s_instance, iteration, pattern._simulation_instances, pattern._analysis_instances, "simulation", sim_step.link_input_data[i])


                        cud = radical.pilot.ComputeUnitDescription()
                        cud.name = "sim ;{iteration} ;{instance}".format(iteration=iteration, instance=s_instance)

                        cud.pre_exec       = sim_step._cu_def_pre_exec
                        cud.executable     = sim_step._cu_def_executable
                        cud.arguments      = sim_step.arguments
                        cud.mpi            = sim_step.uses_mpi
                        cud.input_staging  = None
                        cud.output_staging = None

                        # INPUT DATA:
                        #------------------------------------------------------------------------------------------------------------------
                        # upload_input_data
                        data_in = []
                        if sim_step._kernel._upload_input_data is not None:
                            if isinstance(sim_step._kernel._upload_input_data,list):
                                pass
                            else:
                                sim_step._kernel._upload_input_data = [sim_step._kernel._upload_input_data]
                            for i in range(0,len(sim_step._kernel._upload_input_data)):
                                var=resolve_placeholder_vars(working_dirs, s_instance, iteration, pattern._simulation_instances, pattern._analysis_instances, "simulation", sim_step._kernel._upload_input_data[i])
                                if len(var.split('>')) > 1:
                                    temp = {
                                            'source': var.split('>')[0].strip(),
                                            'target': var.split('>')[1].strip()
                                        }
                                else:
                                    temp = {
                                            'source': var.split('>')[0].strip(),
                                            'target': os.path.basename(var.split('>')[0].strip())
                                        }
                                data_in.append(temp)

                        if cud.input_staging is None:
                            cud.input_staging = data_in
                        else:
                            cud.input_staging += data_in
                        #------------------------------------------------------------------------------------------------------------------

                        #------------------------------------------------------------------------------------------------------------------
                        # link_input_data
                        data_in = []
                        if sim_step._kernel._link_input_data is not None:
                            if isinstance(sim_step._kernel._link_input_data,list):
                                pass
                            else:
                                sim_step._kernel._link_input_data = [sim_step._kernel._link_input_data]
                            for i in range(0,len(sim_step._kernel._link_input_data)):
                                var=resolve_placeholder_vars(working_dirs, s_instance, iteration, pattern._simulation_instances, pattern._analysis_instances, "simulation", sim_step._kernel._link_input_data[i])
                                if len(var.split('>')) > 1:
                                    temp = {
                                            'source': var.split('>')[0].strip(),
                                            'target': var.split('>')[1].strip(),
                                            'action': radical.pilot.LINK
                                        }
                                else:
                                    temp = {
                                            'source': var.split('>')[0].strip(),
                                            'target': os.path.basename(var.split('>')[0].strip()),
                                            'action': radical.pilot.LINK
                                        }
                                data_in.append(temp)

                        if cud.input_staging is None:
                            cud.input_staging = data_in
                        else:
                            cud.input_staging += data_in
                        #------------------------------------------------------------------------------------------------------------------

                        #------------------------------------------------------------------------------------------------------------------
                        # copy_input_data
                        data_in = []
                        if sim_step._kernel._copy_input_data is not None:
                            if isinstance(sim_step._kernel._copy_input_data,list):
                                pass
                            else:
                                sim_step._kernel._copy_input_data = [sim_step._kernel._copy_input_data]
                            for i in range(0,len(sim_step._kernel._copy_input_data)):
                                var=resolve_placeholder_vars(working_dirs, s_instance, iteration, pattern._simulation_instances, pattern._analysis_instances, "simulation", sim_step._kernel._copy_input_data[i])
                                if len(var.split('>')) > 1:
                                    temp = {
                                            'source': var.split('>')[0].strip(),
                                            'target': var.split('>')[1].strip(),
                                            'action': radical.pilot.COPY
                                        }
                                else:
                                    temp = {
                                            'source': var.split('>')[0].strip(),
                                            'target': os.path.basename(var.split('>')[0].strip()),
                                            'action': radical.pilot.COPY
                                        }
                                data_in.append(temp)

                        if cud.input_staging is None:
                            cud.input_staging = data_in
                        else:
                            cud.input_staging += data_in
                        #------------------------------------------------------------------------------------------------------------------

                        #------------------------------------------------------------------------------------------------------------------
                        # download input data
                        if sim_step.download_input_data is not None:
                            data_in  = sim_step.download_input_data
                            if cud.input_staging is None:
                                cud.input_staging = data_in
                            else:
                                cud.input_staging += data_in
                        #------------------------------------------------------------------------------------------------------------------

                        # OUTPUT DATA:
                        #------------------------------------------------------------------------------------------------------------------
                        # copy_output_data
                        data_out = []
                        if sim_step._kernel._copy_output_data is not None:
                            if isinstance(sim_step._kernel._copy_output_data,list):
                                pass
                            else:
                                sim_step._kernel._copy_output_data = [sim_step._kernel._copy_output_data]
                            for i in range(0,len(sim_step._kernel._copy_output_data)):
                                var=resolve_placeholder_vars(working_dirs, s_instance, iteration, pattern._simulation_instances, pattern._analysis_instances, "simulation", sim_step._kernel._copy_output_data[i])
                                if len(var.split('>')) > 1:
                                    temp = {
                                            'source': var.split('>')[0].strip(),
                                            'target': var.split('>')[1].strip(),
                                            'action': radical.pilot.COPY
                                        }
                                else:
                                    temp = {
                                            'source': var.split('>')[0].strip(),
                                            'target': os.path.basename(var.split('>')[0].strip()),
                                            'action': radical.pilot.COPY
                                        }
                                data_out.append(temp)

                        if cud.output_staging is None:
                            cud.output_staging = data_out
                        else:
                            cud.output_staging += data_out
                        #------------------------------------------------------------------------------------------------------------------

                        #------------------------------------------------------------------------------------------------------------------
                        # download_output_data
                        data_out = []
                        if sim_step._kernel._download_output_data is not None:
                            if isinstance(sim_step._kernel._download_output_data,list):
                                pass
                            else:
                                sim_step._kernel._download_output_data = [sim_step._kernel._download_output_data]
                            for i in range(0,len(sim_step._kernel._download_output_data)):
                                var=resolve_placeholder_vars(working_dirs, s_instance, iteration, pattern._simulation_instances, pattern._analysis_instances, "simulation", sim_step._kernel._download_output_data[i])
                                if len(var.split('>')) > 1:
                                    temp = {
                                            'source': var.split('>')[0].strip(),
                                            'target': var.split('>')[1].strip()
                                        }
                                else:
                                    temp = {
                                            'source': var.split('>')[0].strip(),
                                            'target': os.path.basename(var.split('>')[0].strip())
                                        }
                                data_out.append(temp)

                        if cud.output_staging is None:
                            cud.output_staging = data_out
                        else:
                            cud.output_staging += data_out
                        #------------------------------------------------------------------------------------------------------------------


                        if sim_step.cores is not None:
                            cud.cores = sim_step.cores

                        s_units.append(cud)

                        if sim_step.get_instance_type() == 'single':
                            break
                        
                    self.get_logger().debug("Created simulation CU: {0}.".format(cud.as_dict()))
                    s_cus = resource._umgr.submit_units(s_units)
                    all_cus.extend(s_cus)

                    self.get_logger().info("Submitted tasks for simulation iteration {0}.".format(iteration))
                    self.get_logger().info("Waiting for simulations in iteration {0}/ kernel {1}: {2} to complete.".format(iteration,kern_step+1,sim_step.name))
                    resource._umgr.wait_units()
                    self.get_logger().info("Simulations in iteration {0}/ kernel {1}: {2} completed.".format(iteration,kern_step+1,sim_step.name))

                    failed_units = ""
                    for unit in s_cus:
                        if unit.state != radical.pilot.DONE:
                            failed_units += " * Simulation task {0} failed with an error: {1}\n".format(unit.uid, unit.stderr)

                step_end_time_abs = datetime.datetime.now()

                # TODO: ensure working_dir <-> instance mapping
                i = 0
                for cu in s_cus:
                    i += 1
                    working_dirs['iteration_{0}'.format(iteration)]['simulation_{0}'.format(i)] = saga.Url(cu.working_directory).path
       
                # Process CU information and append it to the dictionary
                tinfo = extract_timing_info(s_cus, pattern_start_time, step_start_time_abs, step_end_time_abs)
                for key, val in tinfo.iteritems():
                    step_timings['timings'][key] = val

                # Write the whole thing to the profiling dict
                pattern._execution_profile.append(step_timings)


                ################################################################
                # EXECUTE ANALYSIS STEPS
                step_timings = {
                    "name": "analysis_iteration_{0}".format(iteration),
                    "timings": {}
                }
                step_start_time_abs = datetime.datetime.now()

                if isinstance(pattern.analysis_step(iteration=1, instance=1),list):
                    num_ana_kerns = len(pattern.analysis_step(iteration=1, instance=1))
                else:
                    num_ana_kerns = 1
                #print num_ana_kerns
                for kern_step in range(0,num_ana_kerns):

                    a_units = []
                    for a_instance in range(1, pattern._analysis_instances+1):

                        if isinstance(pattern.analysis_step(iteration=iteration, instance=s_instance),list):
                            ana_step = pattern.analysis_step(iteration=iteration, instance=a_instance)[kern_step]
                        else:
                            ana_step = pattern.analysis_step(iteration=iteration, instance=s_instance)

                        ana_step._bind_to_resource(resource._resource_key)

                        # Resolve all placeholders
                        if ana_step.link_input_data is not None:
                            for i in range(len(ana_step.link_input_data)):
                                ana_step.link_input_data[i] = resolve_placeholder_vars(working_dirs, a_instance, iteration, pattern._simulation_instances, pattern._analysis_instances, "analysis", ana_step.link_input_data[i])

                        cud = radical.pilot.ComputeUnitDescription()
                        cud.name = "ana ; {iteration}; {instance}".format(iteration=iteration, instance=a_instance)

                        cud.pre_exec       = ana_step._cu_def_pre_exec
                        cud.executable     = ana_step._cu_def_executable
                        cud.arguments      = ana_step.arguments
                        cud.mpi            = ana_step.uses_mpi
                        cud.input_staging  = None
                        cud.output_staging = None

                        #------------------------------------------------------------------------------------------------------------------
                        # upload_input_data
                        data_in = []
                        if ana_step._kernel._upload_input_data is not None:
                            if isinstance(ana_step._kernel._upload_input_data,list):
                                pass
                            else:
                                ana_step._kernel._upload_input_data = [ana_step._kernel._upload_input_data]
                            for i in range(0,len(ana_step._kernel._upload_input_data)):
                                var=resolve_placeholder_vars(working_dirs, a_instance, iteration, pattern._simulation_instances, pattern._analysis_instances, "analysis", ana_step._kernel._upload_input_data[i])
                                if len(var.split('>')) > 1:
                                    temp = {
                                            'source': var.split('>')[0].strip(),
                                            'target': var.split('>')[1].strip()
                                        }
                                else:
                                    temp = {
                                            'source': var.split('>')[0].strip(),
                                            'target': os.path.basename(var.split('>')[0].strip())
                                        }
                                data_in.append(temp)

                        if cud.input_staging is None:
                            cud.input_staging = data_in
                        else:
                            cud.input_staging += data_in
                        #------------------------------------------------------------------------------------------------------------------

                        #------------------------------------------------------------------------------------------------------------------
                        # link_input_data
                        data_in = []
                        if ana_step._kernel._link_input_data is not None:
                            if isinstance(ana_step._kernel._link_input_data,list):
                                pass
                            else:
                                ana_step._kernel._link_input_data = [ana_step._kernel._link_input_data]
                            for i in range(0,len(ana_step._kernel._link_input_data)):
                                var=resolve_placeholder_vars(working_dirs, a_instance, iteration, pattern._simulation_instances, pattern._analysis_instances, "analysis", ana_step._kernel._link_input_data[i])
                                if len(var.split('>')) > 1:
                                    temp = {
                                            'source': var.split('>')[0].strip(),
                                            'target': var.split('>')[1].strip(),
                                            'action': radical.pilot.LINK
                                        }
                                else:
                                    temp = {
                                            'source': var.split('>')[0].strip(),
                                            'target': os.path.basename(var.split('>')[0].strip()),
                                            'action': radical.pilot.LINK
                                        }
                                data_in.append(temp)

                        if cud.input_staging is None:
                            cud.input_staging = data_in
                        else:
                            cud.input_staging += data_in
                        #------------------------------------------------------------------------------------------------------------------

                        #------------------------------------------------------------------------------------------------------------------
                        # copy_input_data
                        data_in = []
                        if ana_step._kernel._copy_input_data is not None:
                            if isinstance(ana_step._kernel._copy_input_data,list):
                                pass
                            else:
                                ana_step._kernel._copy_input_data = [ana_step._kernel._copy_input_data]
                            for i in range(0,len(ana_step._kernel._copy_input_data)):
                                var=resolve_placeholder_vars(working_dirs, a_instance, iteration, pattern._simulation_instances, pattern._analysis_instances, "analysis", ana_step._kernel._copy_input_data[i])
                                if len(var.split('>')) > 1:
                                    temp = {
                                            'source': var.split('>')[0].strip(),
                                            'target': var.split('>')[1].strip(),
                                            'action': radical.pilot.COPY
                                        }
                                else:
                                    temp = {
                                            'source': var.split('>')[0].strip(),
                                            'target': os.path.basename(var.split('>')[0].strip()),
                                            'action': radical.pilot.COPY
                                        }
                                data_in.append(temp)

                        if cud.input_staging is None:
                            cud.input_staging = data_in
                        else:
                            cud.input_staging += data_in
                        #------------------------------------------------------------------------------------------------------------------

                        #------------------------------------------------------------------------------------------------------------------
                        # download input data
                        if ana_step.download_input_data is not None:
                            data_in  = ana_step.download_input_data
                            if cud.input_staging is None:
                                cud.input_staging = data_in
                            else:
                                cud.input_staging += data_in
                        #------------------------------------------------------------------------------------------------------------------


                        #------------------------------------------------------------------------------------------------------------------
                        # copy_output_data
                        data_out = []
                        if ana_step._kernel._copy_output_data is not None:
                            if isinstance(ana_step._kernel._copy_output_data,list):
                                pass
                            else:
                                ana_step._kernel._copy_output_data = [ana_step._kernel._copy_output_data]
                            for i in range(0,len(ana_step._kernel._copy_output_data)):
                                var=resolve_placeholder_vars(working_dirs, a_instance, iteration, pattern._simulation_instances, pattern._analysis_instances, "analysis", ana_step._kernel._copy_output_data[i])
                                if len(var.split('>')) > 1:
                                    temp = {
                                            'source': var.split('>')[0].strip(),
                                            'target': var.split('>')[1].strip(),
                                            'action': radical.pilot.COPY
                                        }
                                else:
                                    temp = {
                                            'source': var.split('>')[0].strip(),
                                            'target': os.path.basename(var.split('>')[0].strip()),
                                            'action': radical.pilot.COPY
                                        }
                                data_out.append(temp)

                        if cud.output_staging is None:
                            cud.output_staging = data_out
                        else:
                            cud.output_staging += data_out
                        #------------------------------------------------------------------------------------------------------------------

                        #------------------------------------------------------------------------------------------------------------------
                        # download_output_data
                        data_out = []
                        if ana_step._kernel._download_output_data is not None:
                            if isinstance(ana_step._kernel._download_output_data,list):
                                pass
                            else:
                                ana_step._kernel._download_output_data = [ana_step._kernel._download_output_data]
                            for i in range(0,len(ana_step._kernel._download_output_data)):
                                var=resolve_placeholder_vars(working_dirs, a_instance, iteration, pattern._simulation_instances, pattern._analysis_instances, "analysis", ana_step._kernel._download_output_data[i])
                                if len(var.split('>')) > 1:
                                    temp = {
                                            'source': var.split('>')[0].strip(),
                                            'target': var.split('>')[1].strip()
                                        }
                                else:
                                    temp = {
                                            'source': var.split('>')[0].strip(),
                                            'target': os.path.basename(var.split('>')[0].strip())
                                        }
                                data_out.append(temp)

                        if cud.output_staging is None:
                            cud.output_staging = data_out
                        else:
                            cud.output_staging += data_out
                        #------------------------------------------------------------------------------------------------------------------


                        if ana_step.cores is not None:
                            cud.cores = ana_step.cores

                        a_units.append(cud)

                        if ana_step.get_instance_type == 'single':
                            break

                    self.get_logger().debug("Created analysis CU: {0}.".format(cud.as_dict()))
                    a_cus = resource._umgr.submit_units(a_units)
                    all_cus.extend(a_cus)

                    self.get_logger().info("Submitted tasks for analysis iteration {0}.".format(iteration))
                    self.get_logger().info("Waiting for analysis tasks in iteration {0}/kernel {1}: {2} to complete.".format(iteration,kern_step+1,ana_step.name))
                    resource._umgr.wait_units()
                    self.get_logger().info("Analysis in iteration {0}/kernel {1}: {2} completed.".format(iteration,kern_step+1,ana_step.name))

                    failed_units = ""
                    for unit in a_cus:
                        if unit.state != radical.pilot.DONE:
                            failed_units += " * Analysis task {0} failed with an error: {1}\n".format(unit.uid, unit.stderr)

                
                step_end_time_abs = datetime.datetime.now()

                i = 0
                for cu in a_cus:
                    i += 1
                    working_dirs['iteration_{0}'.format(iteration)]['analysis_{0}'.format(i)] = saga.Url(cu.working_directory).path

                # Process CU information and append it to the dictionary
                tinfo = extract_timing_info(a_cus, pattern_start_time, step_start_time_abs, step_end_time_abs)

                for key, val in tinfo.iteritems():
                    step_timings['timings'][key] = val

                # Write the whole thing to the profiling dict
                pattern._execution_profile.append(step_timings)

        except Exception, ex:
            self.get_logger().error("Fatal error during execution: {0}.".format(str(ex)))
            raise

        finally:
            self.get_logger().info("Deallocating resource.")
            resource.deallocate()
