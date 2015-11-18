#!/usr/bin/env python

"""A static execution plugin for the MTMS pattern
"""

__author__    = "Vivek Balasubramanian <vivek.Balasubramanian@rutgers.edu>"
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

        
        #-----------------------------------------------------------------------
        # Use callback to trigger next stage
        def unit_state_cb (unit, state) :

            if state == radical.pilot.DONE:
        		cud = create_next_stage_cud()
        		launch_next_stage(cud)
        #-----------------------------------------------------------------------

        #-----------------------------------------------------------------------
        # Wait for Pilot to go Active
        resource._pmgr.wait_pilots(resource._pilot.uid,'Active')
		#-----------------------------------------------------------------------

        #-----------------------------------------------------------------------
        # Get details of the Bag of Pipes
        num_tasks = pattern.tasks
        num_stages = pattern.stages
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
            cud.name = "stage_1"

            cud.pre_exec 		= kernel._cu_def_pre_exec
            cud.executable     	= kernel._cu_def_executable
            cud.arguments      	= kernel.arguments
            cud.mpi            		= kernel.uses_mpi
            cud.input_staging  	= get_input_data(kernel,1,task_instance)
            cud.output_staging 	= get_output_data(kernel,1,task_instance)

            task_units = resource._umgr.submit_units(task_units_desc)

        #-----------------------------------------------------------------------

        def create_next_stage_cud():
            pass

        def launch_next_stage():
            pass

