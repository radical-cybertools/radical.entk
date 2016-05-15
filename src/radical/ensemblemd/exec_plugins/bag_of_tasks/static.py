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

from radical.ensemblemd.exceptions import NotImplementedError, EnsemblemdError
from radical.ensemblemd.exec_plugins.plugin_base import PluginBase


# ------------------------------------------------------------------------------
#
_PLUGIN_INFO = {
	"name":         "bag_of_tasks.static.default",
	"pattern":      "BagofTasks",
	"context_type": "Static"
}

_PLUGIN_OPTIONS = []


def resolve_placeholder_vars(working_dirs, instance, total_stages, path):

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

	stage_number = int(placeholder.split('_')[1])

	if ((stage_number < 1)or(stage_number > total_stages)):
		raise Exception("$STAGE_{0} used in invalid context.".format(stage_number))
	else:
		return path.replace(placeholder, working_dirs['stage_{0}'.format(stage_number)]['instance_{0}'.format(instance)])
		 
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
				self.get_logger().error("Pattern execution FAILED.")
				sys.exit(1)

		self._reporter.ok('>>ok')
		pipeline_instances = pattern.instances

		pipeline_stages = pattern.stages
		self.get_logger().info("Executing {0} instances of {1} stages on {2} allocated core(s) on '{3}'".format(
			pipeline_instances, pipeline_stages,resource._cores, resource._resource_key))

		self._reporter.header("Executing {0} instances of {1} stages on {2} allocated core(s) on '{3}'".format(
			pipeline_instances, pipeline_stages,resource._cores, resource._resource_key))

		
		working_dirs = {}

		self.get_logger().info("Waiting for pilot on {0} to go Active".format(resource._resource_key))
		self._reporter.info("Job waiting on queue...".format(resource._resource_key))
		resource._pmgr.wait_pilots(resource._pilot.uid,'Active')
		self._reporter.ok("\nJob is now running !".format(resource._resource_key))

		profiling = int(os.environ.get('RADICAL_ENMD_PROFILING',0))

		if profiling == 1:
			from collections import OrderedDict as od
			pattern._execution_profile = []
			enmd_overhead_dict = od()
			cu_dict = od()

		try:

			resource._umgr.register_callback(unit_state_cb)

			enmd_overhead_list = []
			rp_overhead_list = []
			# Iterate over the different stages.
			for stage in range(1, pipeline_stages+1):

				if profiling == 1:
					probe_start_time = datetime.datetime.now()
					enmd_overhead_dict['stage_{0}'.format(stage)] = od()
					cu_dict['stage_{0}'.format(stage)] = list()

					enmd_overhead_dict['stage_{0}'.format(stage)]['start_time'] = probe_start_time

				working_dirs['stage_{0}'.format(stage)] = {}
				check_instance_files = []

				# Get the method names
				s_meth = getattr(pattern, 'stage_{0}'.format(stage))

				try:
					kernel = s_meth(0)
				except NotImplementedError, ex:
					# Not implemented means there are no further stages.
					break

				p_units=[]
				all_stage_cus = []

				for instance in range(1, pipeline_instances+1):

					kernel = s_meth(instance)
					kernel._bind_to_resource(resource._resource_key)

					cud = radical.pilot.ComputeUnitDescription()
					cud.name = "stage_{0}".format(stage)

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
							var=resolve_placeholder_vars(working_dirs, instance, pipeline_stages,kernel._kernel._upload_input_data[i])
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
							var=resolve_placeholder_vars(working_dirs, instance, pipeline_stages, kernel._kernel._link_input_data[i])
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
							var=resolve_placeholder_vars(working_dirs, instance, pipeline_stages, kernel._kernel._copy_input_data[i])
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
							var=resolve_placeholder_vars(working_dirs, instance, pipeline_stages, kernel._kernel._copy_output_data[i])
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
							var=resolve_placeholder_vars(working_dirs, instance, pipeline_stages, kernel._kernel._download_output_data[i])
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

				self.get_logger().debug("Created stage_{0} CU: {1}.".format(stage,cud.as_dict()))
				

				self.get_logger().info("Submitted tasks for stage_{0}.".format(stage))
				self._reporter.info("\nWaiting for stage_{0} to complete.".format(stage))

				if profiling == 1:
					enmd_overhead_dict['stage_{0}'.format(stage)]['wait_time'] = datetime.datetime.now()


				p_cus = resource._umgr.submit_units(p_units)
				all_stage_cus.extend(p_cus)

				uids = [cu.uid for cu in p_cus]
				resource._umgr.wait_units(uids)
				

				self.get_logger().info("stage_{0}/kernel {1}: completed.".format(stage,kernel.name))

				if profiling == 1:
					enmd_overhead_dict['stage_{0}'.format(stage)]['res_time'] = datetime.datetime.now()


				# TODO: ensure working_dir <-> instance mapping
				i = 0
				for cu in p_cus:
					i += 1
					working_dirs['stage_{0}'.format(stage)]['instance_{0}'.format(i)] = saga.Url(cu.working_directory).path

				failed_units = ""
				for unit in p_cus:
					if unit.state != radical.pilot.DONE:
						failed_units += " * stage_{0} failed with an error: {1}\n".format(stage, unit.stderr)

				if profiling == 1:
					enmd_overhead_dict['stage_{0}'.format(stage)]['stop_time'] = datetime.datetime.now()
					cu_dict['stage_{0}'.format(stage)] = p_cus

				self._reporter.ok('>> done')

			#Pattern Finished
			self._reporter.header('Pattern execution successfully finished')


			#PROFILING
			if profiling == 1:

				#Pattern overhead logging
				title = "stage,probe,timestamp"
				f1 = open('enmd_pat_overhead.csv','w')
				f1.write(title + "\n\n")

				pipeline_stages = pattern.stages

				for i in range(1,pipeline_stages+1):
					stage = 'stage_{0}'.format(i)
					for key,val in enmd_overhead_dict[stage].items():                        
						probe = key
						timestamp = val
						entry = '{0},{1},{2}\n'.format(stage,probe,timestamp)
						f1.write(entry)

				f1.close()

				#CU data logging
				title = "uid, stage, Scheduling, StagingInput, AgentStagingInputPending, AgentStagingInput, AllocatingPending, Allocating, ExecutingPending, Executing, AgentStagingOutputPending, AgentStagingOutput, PendingOutputStaging, StagingOutput, Done"
				f2 = open("execution_profile_{mysession}.csv".format(mysession=resource._session.uid),'w')
				f2.write(title + "\n\n")

				for i in range(1,pipeline_stages+1):
					stage = 'stage_{0}'.format(i)
					cus = cu_dict[stage]

					for cu in cus:
						st_data = {}
						for st in cu.state_history:
							st_dict = st.as_dict()
							st_data["{0}".format( st_dict["state"] )] = {}
							st_data["{0}".format( st_dict["state"] )] = st_dict["timestamp"]

						states = ['Scheduling,' 
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
					

		except KeyboardInterrupt:

			self._reporter.error('Execution interupted')
			traceback.print_exc()
