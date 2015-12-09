#!/usr/bin/env python

"""A static execution plugin for the MTMS pattern
"""

__author__    = "Vivek Balasubramanian <vivek.balasubramanian@rutgers.edu>"
__copyright__ = "Copyright 2015, http://radical.rutgers.edu"
__license__   = "MIT"

import os
import sys
import saga
import time
import traceback
import pickle
import datetime
import radical.pilot

from radical.ensemblemd.exceptions import NotImplementedError, EnsemblemdError
from radical.ensemblemd.exec_plugins.plugin_base import PluginBase


# ------------------------------------------------------------------------------
#
_PLUGIN_INFO = {
    "name":         "mtms.static.default",
    "pattern":      "MTMS",
    "context_type": "Static"
}

_PLUGIN_OPTIONS = []

# ------------------------------------------------------------------------------
#

def resolve_placeholder_vars(working_dirs, stage, instance, path):

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


    if placeholder.startswith("$STAGE_"):
        stage = placeholder.split("$STAGE_")[1]
        return path.replace(placeholder,working_dirs['stage_{0}'.format(stage)]['task_{0}'.format(instance)])
    else:
        raise Exception("placeholder $STAGE_ used in invalid context.")

class Plugin(PluginBase):

	# --------------------------------------------------------------------------
    #
    def __init__(self):
        super(Plugin, self).__init__(_PLUGIN_INFO, _PLUGIN_OPTIONS)
        self.tot_fin_tasks=0
        self.working_dirs = {}

    # --------------------------------------------------------------------------
    #
    def verify_pattern(self, pattern, resource):
        self.get_logger().info("Verifying pattern...")

    # --------------------------------------------------------------------------
    #
    def execute_pattern(self, pattern, resource):


        #-----------------------------------------------------------------------
        # Get details of the Bag of Pipes
        num_tasks = pattern.tasks
        num_stages = pattern.stages
        #-----------------------------------------------------------------------
        
        #-----------------------------------------------------------------------
        # Use callback to trigger next stage
        def unit_state_cb (unit, state):

            if state == radical.pilot.DONE:
                cur_stage = int(unit.name.split('-')[1])
                cur_task = int(unit.name.split('-')[3])
                self.get_logger().info('Task {0} of stage {1} has finished'.format(cur_task,cur_stage))

                #-----------------------------------------------------------------------
                # Log unit working directories for placeholders
                if 'stage_{0}'.format(cur_stage) not in self.working_dirs:
                    self.working_dirs['stage_{0}'.format(cur_stage)] = {}

                self.working_dirs['stage_{0}'.format(cur_stage)]['task_{0}'.format(cur_task)] = unit.working_directory
                #print self.working_dirs['stage_{0}'.format(cur_stage)]['task_{0}'.format(cur_task)]
                #-----------------------------------------------------------------------

                cud = create_next_stage_cud(unit)
                if cud is not None:
                    launch_next_stage(cud)
        #-----------------------------------------------------------------------

        self.get_logger().info("Executing {0} pipeline instances of {1} stages on {2} allocated core(s) on '{3}'".format(num_tasks, num_stages,
            resource._cores, resource._resource_key))
        #-----------------------------------------------------------------------
        # Wait for Pilot to go Active
        resource._pmgr.wait_pilots(resource._pilot.uid,u'Active')
		#-----------------------------------------------------------------------


        #-----------------------------------------------------------------------
        # Register CB
        resource._umgr.register_callback(unit_state_cb)
        #-----------------------------------------------------------------------


        #-----------------------------------------------------------------------
        # Get input data for the kernel
        def get_input_data(kernel,stage,task):
        	return None
        #-----------------------------------------------------------------------

        #-----------------------------------------------------------------------
        # Get output data for the kernel
        def get_output_data(kernel,stage,task):
            return None
        #-----------------------------------------------------------------------

        #-----------------------------------------------------------------------
        # Launch first stage of all tasks

        task_method = getattr(pattern, 'stage_1')
        task_units_desc = []
        for task_instance in range(1, num_tasks+1):

            kernel = task_method(task_instance)
            kernel._bind_to_resource(resource._resource_key)

            cud = radical.pilot.ComputeUnitDescription()
            cud.name = "stage-1-task-{0}".format(task_instance)

            cud.pre_exec 		= kernel._cu_def_pre_exec
            cud.executable     	= kernel._cu_def_executable
            cud.arguments      	= kernel.arguments
            cud.mpi            	= kernel.uses_mpi
            cud.input_staging  	= get_input_data(kernel,1,task_instance)
            cud.output_staging 	= get_output_data(kernel,1,task_instance)

            task_units_desc.append(cud)

        task_units = resource._umgr.submit_units(task_units_desc)
        self.get_logger().info('Submitted all tasks of stage 1')

        #-----------------------------------------------------------------------

        #-----------------------------------------------------------------------
        # Create next CU at the end of each CU
        def create_next_stage_cud(unit):
            
            
            cur_stage = int(unit.name.split('-')[1])+1
            cur_task = int(unit.name.split('-')[3])
            self.tot_fin_tasks+=1

            if cur_stage <= num_stages:
                self.get_logger().info('Submitting task {0} of stage {1}'.format(cur_task,cur_stage))

                task_method = getattr(pattern, 'stage_{0}'.format(cur_stage))

                kernel = task_method(cur_task)
                kernel._bind_to_resource(resource._resource_key)

                cud = radical.pilot.ComputeUnitDescription()
                cud.name = "stage-{0}-task-{1}".format(cur_stage,cur_task)

                cud.pre_exec        = kernel._cu_def_pre_exec
                cud.executable      = kernel._cu_def_executable
                cud.arguments       = kernel.arguments
                cud.mpi             = kernel.uses_mpi
                cud.input_staging   = get_input_data(kernel,1,task_instance)
                cud.output_staging  = get_output_data(kernel,1,task_instance)

                return cud

            else:
                return None

        #-----------------------------------------------------------------------

        #-----------------------------------------------------------------------
        # Launch the CU of the next stage
        def launch_next_stage(cud):

            resource._umgr.submit_units(cud)    
            return None    
        #-----------------------------------------------------------------------

        #-----------------------------------------------------------------------
        # Wait for all tasks to finish
        while(self.tot_fin_tasks<(num_stages*num_tasks)):
            resource._umgr.wait_units()    

        #-----------------------------------------------------------------------
