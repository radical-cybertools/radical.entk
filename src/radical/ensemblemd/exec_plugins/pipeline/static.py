#!/usr/bin/env python

"""A static execution plugin for single tasks.
"""

__author__    = "Ole Weider <ole.weidner@rutgers.edu>"
__copyright__ = "Copyright 2014, http://radical.rutgers.edu"
__license__   = "MIT"

import os
import sys
import saga
import time
import traceback
import pickle
import datetime
import radical.pilot

from radical.ensemblemd.utils import extract_timing_info
from radical.ensemblemd.exceptions import NotImplementedError, EnsemblemdError
from radical.ensemblemd.exec_plugins.plugin_base import PluginBase

# ------------------------------------------------------------------------------
#
_PLUGIN_INFO = {
    "name":         "pipeline.static.default",
    "pattern":      "Pipeline",
    "context_type": "Static"
}

_PLUGIN_OPTIONS = []


def resolve_placeholder_vars(working_dirs, instance, total_steps, path):

    # If replacement not require, return the path as is
    if '$' not in path:
        return path

    # Extract placeholder from path
    if len(path.split('>'))==1:
        placeholder = path.split('/')[0]
    else:
        if path.split('>')[0].strip().startswith('$'):
            placeholder = path.split('>')[0].strip().split('/')[0]
        else:
            placeholder = path.split('>')[1].strip().split('/')[0]

    step_number = int(placeholder.split('_')[1])

    if ((step_number < 1)or(step_number > total_steps)):
        raise Exception("$STEP_{0} used in invalid context.".format(step_number))
    else:
        return path.replace(placeholder, working_dirs['step_{0}'.format(step_number)]['instance_{0}'.format(instance)])
         
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
        self.get_logger().info("Verifying pattern...")

    # --------------------------------------------------------------------------
    #
    def execute_pattern(self, pattern, resource):

        pattern_start_time = datetime.datetime.now()
        
        #-----------------------------------------------------------------------
        #
        def unit_state_cb (unit, state) :

            if state == radical.pilot.FAILED:
                self.get_logger().error("Task with ID {0} failed: STDERR: {1}, STDOUT: {2} LAST LOG: {3}".format(unit.uid, unit.stderr, unit.stdout, unit.log[-1]))

        pipeline_instances = pattern.instances
        pipeline_steps = pattern.steps
        self.get_logger().info("Executing {0} pipeline instances of {1} steps on {2} allocated core(s) on '{3}'".format(
            pipeline_instances, pipeline_steps,resource._cores, resource._resource_key))

        
        working_dirs = {}
        all_cus = []

        self.get_logger().info("Waiting for pilot on {0} to go Active".format(resource._resource_key))
        resource._pmgr.wait_pilots(resource._pilot.uid,'Active')

        profiling = int(os.environ.get('RADICAL_ENMD_PROFILING',0))

        if profiling == 1:
            pattern._execution_profile = []

        try:

            start_now = datetime.datetime.now()
            resource._umgr.register_callback(unit_state_cb)

            # Iterate over the different steps.
            for step in range(1, pipeline_steps+1):

                if profiling == 1:
                    step_timings = {
                        "name": "step_{0}".format(step),
                        "timings": {}
                    }
                    step_start_time_abs = datetime.datetime.now()

                working_dirs['step_{0}'.format(step)] = {}

                # Get the method names
                s_meth = getattr(pattern, 'step_{0}'.format(step))

                try:
                    kernel = s_meth(0)
                except NotImplementedError, ex:
                    # Not implemented means there are no further steps.
                    break

                p_units=[]
                all_step_cus = []

                for instance in range(1, pipeline_instances+1):

                    # Create a new journal entry for the instance.
                    kernel = s_meth(instance)
                    kernel._bind_to_resource(resource._resource_key)

                    cud = radical.pilot.ComputeUnitDescription()
                    cud.name = "step_{0}".format(step)

                    cud.pre_exec       = kernel._cu_def_pre_exec
                    cud.executable     = kernel._cu_def_executable
                    cud.arguments      = kernel.arguments
                    cud.mpi            = kernel.uses_mpi
                    cud.input_staging  = None
                    cud.output_staging = None

                    # INPUT DATA:
                    #------------------------------------------------------------------------------------------------------------------
                    # upload_input_data
                    data_in = []
                    if kernel._kernel._upload_input_data is not None:
                        if isinstance(kernel._kernel._upload_input_data,list):
                            pass
                        else:
                            kernel._kernel._upload_input_data = [kernel._kernel._upload_input_data]
                        for i in range(0,len(kernel._kernel._upload_input_data)):
                            var=resolve_placeholder_vars(working_dirs, instance, pipeline_steps,kernel._kernel._upload_input_data[i])
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
                    if kernel._kernel._link_input_data is not None:
                        if isinstance(kernel._kernel._link_input_data,list):
                            pass
                        else:
                            kernel._kernel._link_input_data = [kernel._kernel._link_input_data]
                        for i in range(0,len(kernel._kernel._link_input_data)):
                            var=resolve_placeholder_vars(working_dirs, instance, pipeline_steps, kernel._kernel._link_input_data[i])
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
                    if kernel._kernel._copy_input_data is not None:
                        if isinstance(kernel._kernel._copy_input_data,list):
                            pass
                        else:
                            kernel._kernel._copy_input_data = [kernel._kernel._copy_input_data]
                        for i in range(0,len(kernel._kernel._copy_input_data)):
                            var=resolve_placeholder_vars(working_dirs, instance, pipeline_steps, kernel._kernel._copy_input_data[i])
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
                    if kernel.download_input_data is not None:
                        data_in  = kernel.download_input_data
                        if cud.input_staging is None:
                            cud.input_staging = data_in
                        else:
                            cud.input_staging += data_in
                    #------------------------------------------------------------------------------------------------------------------

                    # OUTPUT DATA:
                    #------------------------------------------------------------------------------------------------------------------
                    # copy_output_data
                    data_out = []
                    if kernel._kernel._copy_output_data is not None:
                        if isinstance(kernel._kernel._copy_output_data,list):
                            pass
                        else:
                            kernel._kernel._copy_output_data = [kernel._kernel._copy_output_data]
                        for i in range(0,len(kernel._kernel._copy_output_data)):
                            var=resolve_placeholder_vars(working_dirs, instance, pipeline_steps, kernel._kernel._copy_output_data[i])
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
                    if kernel._kernel._download_output_data is not None:
                        if isinstance(kernel._kernel._download_output_data,list):
                            pass
                        else:
                            kernel._kernel._download_output_data = [kernel._kernel._download_output_data]
                        for i in range(0,len(kernel._kernel._download_output_data)):
                            var=resolve_placeholder_vars(working_dirs, instance, pipeline_steps, kernel._kernel._download_output_data[i])
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


                    if kernel.cores is not None:
                        cud.cores = kernel.cores

                    p_units.append(cud)

                self.get_logger().debug("Created step_{0} CU: {1}.".format(step,cud.as_dict()))
                p_cus = resource._umgr.submit_units(p_units)
                all_step_cus.extend(p_cus)

                self.get_logger().info("Submitted tasks for step_{0}.".format(step))
                self.get_logger().info("Waiting for step_{0} to complete.".format(step))
                resource._umgr.wait_units()
                self.get_logger().info("step_{0} completed.".format(step))

                failed_units = ""
                for unit in p_cus:
                    if unit.state != radical.pilot.DONE:
                        failed_units += " * step_{0} failed with an error: {1}\n".format(step, unit.stderr)

                if profiling == 1:
                    step_end_time_abs = datetime.datetime.now()

                # TODO: ensure working_dir <-> instance mapping
                i = 0
                for cu in p_cus:
                    i += 1
                    working_dirs['step_{0}'.format(step)]['instance_{0}'.format(i)] = saga.Url(cu.working_directory).path


                # Process CU information and append it to the dictionary
                if profiling == 1:
                    tinfo = extract_timing_info(all_step_cus, pattern_start_time, step_start_time_abs, step_end_time_abs)
                    for key, val in tinfo.iteritems():
                        step_timings['timings'][key] = val

                    # Write the whole thing to the profiling dict
                    pattern._execution_profile.append(step_timings)


        except KeyboardInterrupt:
            traceback.print_exc()
