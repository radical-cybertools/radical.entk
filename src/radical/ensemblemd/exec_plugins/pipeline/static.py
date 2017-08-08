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

from collections import OrderedDict as od
# ------------------------------------------------------------------------------
#
_PLUGIN_INFO = {
	"name":         "pipeline.static.default",
	"pattern":      "Pipeline",
	"context_type": "Static"
}

_PLUGIN_OPTIONS = []

# ------------------------------------------------------------------------------
#

def resolve_placeholder_vars(working_dirs, stage, task, path):

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

	# SHARED
	if placeholder == "$SHARED":
		return path.replace(placeholder, 'staging://')

	if placeholder.startswith("$STAGE_"):
		stage = placeholder.split("$STAGE_")[1]
		return path.replace(placeholder,working_dirs['stage_{0}'.format(stage)]['task_{0}'.format(task)])
	else:
		raise Exception("placeholder $STAGE_ used in invalid context.")

class Plugin(PluginBase):

	# --------------------------------------------------------------------------
	#
	def __init__(self):
		super(Plugin, self).__init__(_PLUGIN_INFO, _PLUGIN_OPTIONS)
		self.tot_fin_tasks= [0]
		self.working_dirs = {}

	# --------------------------------------------------------------------------
	#
	def verify_pattern(self, pattern, resource):
		pass

	# --------------------------------------------------------------------------
	#
	def execute_pattern(self, pattern, resource):




		#-----------------------------------------------------------------------
		# Get input data for the kernel
		def get_input_data(kernel,stage,task):
			# INPUT DATA:

			ip_list = []
			#------------------------------------------------------------------------------------------------------------------
			# upload_input_data
			data_in = []
			if kernel._kernel._upload_input_data is not None:
				if isinstance(kernel._kernel._upload_input_data,list):
					pass
				else:
					kernel._kernel._upload_input_data = [kernel._kernel._upload_input_data]
				for i in range(0,len(kernel._kernel._upload_input_data)):
					var=resolve_placeholder_vars(self.working_dirs, stage, task, kernel._kernel._upload_input_data[i])
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

			if ip_list is None:
				ip_list = data_in
			else:
				ip_list += data_in
			#-----------------------------------------------------------------------

			#------------------------------------------------------------------------------------------------------------------
			# link_input_data
			data_in = []
			if kernel._kernel._link_input_data is not None:
				if isinstance(kernel._kernel._link_input_data,list):
					pass
				else:
					kernel._kernel._link_input_data = [kernel._kernel._link_input_data]
				for i in range(0,len(kernel._kernel._link_input_data)):
					var=resolve_placeholder_vars(self.working_dirs, stage, task, kernel._kernel._link_input_data[i])
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

			if ip_list is None:
				ip_list = data_in
			else:
				ip_list += data_in
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
					var=resolve_placeholder_vars(self.working_dirs, stage, task, kernel._kernel._copy_input_data[i])
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

			if ip_list is None:
				ip_list = data_in
			else:
				ip_list += data_in
			#------------------------------------------------------------------------------------------------------------------

			#------------------------------------------------------------------------------------------------------------------
			# download input data
			if kernel.download_input_data is not None:
				data_in  = kernel.download_input_data
				if ip_list is None:
					ip_list = data_in
				else:
					ip_list += data_in
			#------------------------------------------------------------------------------------------------------------------


			return ip_list
		#-----------------------------------------------------------------------

		#-----------------------------------------------------------------------
		# Get output data for the kernel
		def get_output_data(kernel,stage,task):

			# OUTPUT DATA:
			#------------------------------------------------------------------------------------------------------------------
			# copy_output_data
			op_list = []
			data_out = []
			if kernel._kernel._copy_output_data is not None:
				if isinstance(kernel._kernel._copy_output_data,list):
					pass
				else:
					kernel._kernel._copy_output_data = [kernel._kernel._copy_output_data]
				for i in range(0,len(kernel._kernel._copy_output_data)):
					var=resolve_placeholder_vars(self.working_dirs, stage, task, kernel._kernel._copy_output_data[i])
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

			if op_list is None:
				op_list = data_out
			else:
				op_list += data_out
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
					var=resolve_placeholder_vars(self.working_dirs, stage, task, kernel._kernel._download_output_data[i])
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

			if op_list is None:
				op_list = data_out
			else:
				op_list += data_out
			#------------------------------------------------------------------------------------------------------------------

			return op_list
			
		#-----------------------------------------------------------------------

		
		
		
		#-----------------------------------------------------------------------
		# Use callback to trigger next stage
		def unit_state_cb (unit, state):

			if state == radical.pilot.DONE:
				try:
					cur_stage = int(unit.name.split('-')[1])
					cur_task = int(unit.name.split('-')[3])

					if profiling == 1:
						enmd_overhead_dict['stage_%s'%cur_stage]['pipeline_%s'%cur_task]['res_time'] = datetime.datetime.now()
			
					self.get_logger().info('Task {0} of stage {1} has finished'.format(cur_task,cur_stage))

					#-----------------------------------------------------------------------
					# Increment tasks list accordingly
					if self.tot_fin_tasks[0] == 0:
						self.tot_fin_tasks[0] = 1
					else:
						self.tot_fin_tasks[cur_stage-1]+=1
						# Check if this is the last task of the stage
						if self.tot_fin_tasks[cur_stage-1] == num_tasks:
							self._reporter.info('\nAll tasks in stage {0} have finished'.format(cur_stage))
							self._reporter.ok('>> done')
							self.get_logger().info('All tasks in stage {0} has finished'.format(cur_stage))
					#-----------------------------------------------------------------------
					# Log unit working directories for placeholders
					if 'stage_{0}'.format(cur_stage) not in self.working_dirs:
						self.working_dirs['stage_{0}'.format(cur_stage)] = {}

					self.working_dirs['stage_{0}'.format(cur_stage)]['task_{0}'.format(cur_task)] = unit.working_directory
					#-----------------------------------------------------------------------

					if profiling == 1:
						enmd_overhead_dict['stage_%s'%cur_stage]['pipeline_%s'%cur_task]['done_time'] = datetime.datetime.now()


					cud = create_next_stage_cud(unit)
					if cud is not None:
						launch_next_stage(cud)

				except Exception as ex:
					self.get_logger().error('CB failed, error: %s'%ex)
					print traceback.format_exc()

		#-----------------------------------------------------------------------

		#-----------------------------------------------------------------------
		# Create next CU at the end of each CU
		def create_next_stage_cud(unit):
						
			cur_stage = int(unit.name.split('-')[1])+1
			cur_task = int(unit.name.split('-')[3])

			if cur_stage <= num_stages:

				if profiling == 1:
					if 'stage_%s'%cur_stage not in enmd_overhead_dict:
						enmd_overhead_dict['stage_%s'%cur_stage] = od()
					enmd_overhead_dict['stage_%s'%cur_stage]['pipeline_%s'%cur_task] = od()
					enmd_overhead_dict['stage_%s'%cur_stage]['pipeline_%s'%cur_task]['start_time'] = datetime.datetime.now()

					if 'stage_%s'%cur_stage not in cu_dict:
						cu_dict['stage_%s'%cur_stage] = list()

				if len(self.tot_fin_tasks) < cur_stage:
					self.tot_fin_tasks.append(0)
					self._reporter.info('\nStarting submission of tasks in stage {0}'.format(cur_stage))
					self._reporter.ok('>> ok')
					
				self.get_logger().debug('Creating task {0} of stage {1}'.format(cur_task,cur_stage))

				task_method = getattr(pattern, 'stage_{0}'.format(cur_stage))

				kernel = task_method(cur_task)
				kernel._bind_to_resource(resource._resource_key)

				cud = radical.pilot.ComputeUnitDescription()
				cud.name = "stage-{0}-task-{1}".format(cur_stage,cur_task)

				cud.pre_exec        = kernel._cu_def_pre_exec
				cud.executable      = kernel._cu_def_executable
				cud.arguments       = kernel.arguments
				cud.mpi             = kernel.uses_mpi

				if kernel.cores:
					cud.cores = kernel.cores
					
				cud.input_staging   = get_input_data(kernel,cur_stage,cur_task)
				cud.output_staging  = get_output_data(kernel,cur_stage,cur_task)

				return cud

			else:
				return None

		#-----------------------------------------------------------------------

		#-----------------------------------------------------------------------
		# Launch the CU of the next stage
		def launch_next_stage(cud):

			cur_stage = int(cud.name.split('-')[1])
			cur_task = int(cud.name.split('-')[3])
			self.get_logger().info('Submitting task {0} of stage {1}'.format(cur_task,cur_stage))

			if profiling == 1:
				enmd_overhead_dict['stage_%s'%cur_stage]['pipeline_%s'%cur_task]['wait_time'] = datetime.datetime.now()

			cu = resource._umgr.submit_units(cud)    
			cu_dict['stage_%s'%cur_stage].append(cu)
		#-----------------------------------------------------------------------


		try:

			#-----------------------------------------------------------------------
			# Get details of the Bag of Pipes
			num_tasks = pattern.tasks
			num_stages = pattern.stages
			#-----------------------------------------------------------------------


			self.get_logger().info("Executing {0} pipes of {1} stages on {2} allocated core(s) on '{3}'".format(num_tasks, num_stages,
				resource._cores, resource._resource_key))

			self._reporter.header("Executing {0} pipes of {1} steps on {2} allocated core(s) on '{3}'".format(num_tasks, num_stages,
				resource._cores, resource._resource_key))


			working_dirs = {}


			#-----------------------------------------------------------------------
			# Wait for Pilot to go Active
			self.get_logger().info("Waiting for pilot on {0} to go Active".format(resource._resource_key))
			self._reporter.info("Job waiting on queue...".format(resource._resource_key))
			resource._pmgr.wait_pilots(resource._pilot.uid,u'Active')
			self._reporter.ok("\nJob is now running !\n".format(resource._resource_key))
			#-----------------------------------------------------------------------


			profiling = int(os.environ.get('RADICAL_ENMD_PROFILING',0))
			if profiling == 1:				
				pattern._execution_profile = []
				enmd_overhead_dict = od()
				cu_dict = od()


			#-----------------------------------------------------------------------
			# Register CB
			resource._umgr.register_callback(unit_state_cb)
			#-----------------------------------------------------------------------


			#-----------------------------------------------------------------------
			# Launch first stage of all tasks

			task_method = getattr(pattern, 'stage_1')
			task_units_desc = []

			if profiling == 1:				
				enmd_overhead_dict['stage_1'] = od()
				cu_dict['stage_1'] = list()

			for task_instance in range(1, num_tasks+1):

				if profiling == 1:
					enmd_overhead_dict['stage_1']['pipeline_%s'%task_instance] = od()
					enmd_overhead_dict['stage_1']['pipeline_%s'%task_instance]['start_time'] = datetime.datetime.now()
					cu_dict['stage_1'] = list()

				kernel = task_method(task_instance)
				kernel._bind_to_resource(resource._resource_key)

				self.get_logger().debug('Creating task {0} of stage 1'.format(task_instance))

				cud = radical.pilot.ComputeUnitDescription()
				cud.name = "stage-1-task-{0}".format(task_instance)

				cud.pre_exec        = kernel._cu_def_pre_exec
				cud.executable      = kernel._cu_def_executable
				cud.arguments       = kernel.arguments
				cud.mpi             = kernel.uses_mpi

				if kernel.cores:
					cud.cores = kernel.cores

				cud.input_staging   = get_input_data(kernel,1,task_instance)
				cud.output_staging  = get_output_data(kernel,1,task_instance)

				task_units_desc.append(cud)

			if profiling == 1:

				for task_instance in range(1, num_tasks+1):
					enmd_overhead_dict['stage_1']['pipeline_%s'%task_instance]['wait_time'] = datetime.datetime.now()

			task_units = resource._umgr.submit_units(task_units_desc)

			if profiling == 1:
				cu_dict['stage_1'] = task_units

			self.get_logger().info('Submitted all tasks of stage 1')
			self._reporter.info('Submitted all tasks of stage 1')
			self._reporter.ok('>> ok')
			#-----------------------------------------------------------------------

			#-----------------------------------------------------------------------
			# Wait for all tasks to finish
			while(sum(self.tot_fin_tasks)!=(num_stages*num_tasks)):
				resource._umgr.wait_units()    

			#-----------------------------------------------------------------------

			#PROFILING
			if profiling == 1:

				#Pattern overhead logging
				title = "stage, pipeline, probe,timestamp"
				f1 = open('enmd_pat_overhead.csv','w')
				f1.write(title + "\n\n")

				num_stages = pattern.stages
				num_pipelines = pattern.tasks

				for s in range(1,num_stages+1):
					for p in range(1, num_pipelines+1):
						for key,val in enmd_overhead_dict['stage_%s'%s]['pipeline_%s'%p].items():                        
							probe = key
							timestamp = val
							entry = 'stage_{0},pipeline_{1},{2},{3}\n'.format(s,p,probe,timestamp)
							f1.write(entry)

				f1.close()

				#CU data logging
				title = "uid, stage, Scheduling, StagingInput, AgentStagingInputPending, AgentStagingInput, AllocatingPending, Allocating, ExecutingPending, Executing, AgentStagingOutputPending, AgentStagingOutput, PendingOutputStaging, StagingOutput, Done"
				f2 = open("execution_profile_{mysession}.csv".format(mysession=resource._session.uid),'w')
				f2.write(title + "\n\n")

				for s in range(1,num_stages+1):
					stage = 'stage_{0}'.format(s)
					cus = cu_dict[stage]

					for cu in cus:
						st_data = {}
						for st in cu.state_history:
							st_dict = st.as_dict()
							st_data["{0}".format( st_dict["state"] )] = {}
							st_data["{0}".format( st_dict["state"] )] = st_dict["timestamp"]

						states = [	'Scheduling,' 
									'StagingInput', 'AgentStagingInputPending', 'AgentStagingInput',
									'AllocatingPending', 'Allocating', 
									'ExecutingPending', 'Executing', 
									'AgentStagingOutputPending', 'AgentStagingOutput', 'PendingOutputStaging', 
									'StagingOutput', 
									'Done']

						for state in states:
							if (state in st_data) is False:
								st_data[state] = None

						line = "{uid}, {stage}, {Scheduling}, {StagingInput}, {AgentStagingInputPending}, {AgentStagingInput}, {AllocatingPending}, {Allocating}, {ExecutingPending},{Executing}, {AgentStagingOutputPending}, {AgentStagingOutput}, {PendingOutputStaging}, {StagingOutput}, {Done}".format(
							uid=cu.uid,
							stage=stage,
							Scheduling=(st_data['Scheduling']),
							StagingInput=(st_data['StagingInput']),
							AgentStagingInputPending=(st_data['AgentStagingInputPending']),
							AgentStagingInput=(st_data['AgentStagingInput']),
							AllocatingPending=(st_data['AllocatingPending']),
							Allocating=(st_data['Allocating']),
							ExecutingPending=(st_data['ExecutingPending']),
							Executing=(st_data['Executing']),
							AgentStagingOutputPending=(st_data['AgentStagingOutputPending']),
							AgentStagingOutput=(st_data['AgentStagingOutput']),
							PendingOutputStaging=(st_data['PendingOutputStaging']),
							StagingOutput=(st_data['StagingOutput']),
							Done=(st_data['Done']))

						f2.write(line + '\n')

				f2.close()

		except Exception as ex:

			self.get_logger().error('Execution plugin failed, error: %s'%ex)
			print traceback.format_exc()