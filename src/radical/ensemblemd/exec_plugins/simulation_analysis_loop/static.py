#!/usr/bin/env python

"""A static execution plugin for the 'simulation-analysis' pattern.
"""

__author__    = "Vivek Balasubramanian <vivek.balasubramanian@rutgers.edu>"
__copyright__ = "Copyright 2014, http://radical.rutgers.edu"
__license__   = "MIT"

import os
import sys
import traceback
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
def resolve_placeholder_vars(working_dirs, path, instance=None, iteration=None, type=None):

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

	# $PRE_LOOP
	if placeholder == "$PRE_LOOP":
		return path.replace(placeholder, working_dirs["pre_loop"])

	# $POST_LOOP
	elif placeholder == "$POST_LOOP":
		return path.replace(placeholder, working_dirs["post_loop"])

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
		self.working_dirs = {}

	# --------------------------------------------------------------------------
	#
	def verify_pattern(self, pattern, resource):
		pass

	# --------------------------------------------------------------------------
	#
	def execute_pattern(self, pattern, resource):

		pattern_start_time = datetime.datetime.now()

		def get_input_data(kernel,instance=None,iteration=None,ktype=None):

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
					if (ktype=='simulation' or ktype=='analysis'):
						var=resolve_placeholder_vars(working_dirs=self.working_dirs, 
							path=kernel._kernel._upload_input_data[i], 
                        						instance=instance, iteration=iteration, type=ktype)
					else:
						var=resolve_placeholder_vars(working_dirs=self.working_dirs, 
							path=kernel._kernel._upload_input_data[i])

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
					if (ktype=='simulation' or ktype=='analysis'):
						var=resolve_placeholder_vars(working_dirs=self.working_dirs, 
							path=kernel._kernel._link_input_data[i], 
							instance=instance, iteration=iteration, type=ktype)
					else:
						var=resolve_placeholder_vars(working_dirs=self.working_dirs, 
							path=kernel._kernel._link_input_data[i])

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
					if (ktype=='simulation' or ktype=='analysis'):
						var=resolve_placeholder_vars(working_dirs=self.working_dirs, 
							path=kernel._kernel._copy_input_data[i], 
							instance=instance, iteration=iteration, type=ktype)
					else:
						var=resolve_placeholder_vars(working_dirs=self.working_dirs, 
							path=kernel._kernel._copy_input_data[i])

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
			#------------------------------------------------------------------------------------------------------------------

		def get_output_data(kernel,instance=None,iteration=None,ktype=None):
			# OUTPUT DATA:
			op_list = []
			#------------------------------------------------------------------------------------------------------------------
			# copy_output_data
			data_out = []

			if kernel._kernel._copy_output_data is not None:
				if isinstance(kernel._kernel._copy_output_data,list):
					pass
				else:
					kernel._kernel._copy_output_data = [kernel._kernel._copy_output_data]

				for i in range(0,len(kernel._kernel._copy_output_data)):
					if (ktype=='simulation' or ktype=='analysis'):
						var=resolve_placeholder_vars(working_dirs=self.working_dirs, path=kernel._kernel._copy_output_data[i], 
						instance=instance, iteration=iteration, type=ktype)
					else:
						var=resolve_placeholder_vars(working_dirs=self.working_dirs, path=kernel._kernel._copy_output_data[i])
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
			#-----------------------------------------------------------------------------------------------------------------

			#------------------------------------------------------------------------------------------------------------------
			# download_output_data
			data_out = []

			if kernel._kernel._download_output_data is not None:
				if isinstance(kernel._kernel._download_output_data,list):
					pass
				else:
					kernel._kernel._download_output_data = [kernel._kernel._download_output_data]

				for i in range(0,len(kernel._kernel._download_output_data)):
					if (ktype=='simulation' or ktype=='analysis'):
						var=resolve_placeholder_vars(working_dirs=self.working_dirs, 
							path=kernel._kernel._download_output_data[i], 
							instance=instance, iteration=iteration, type=ktype)
					else:
						var=resolve_placeholder_vars(working_dirs=self.working_dirs, 
							path=kernel._kernel._download_output_data[i])

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
			#------------------------------------------------------------------------------------------------------------------


		#-----------------------------------------------------------------------
		#
		def unit_state_cb (unit, state) :

			if state == radical.pilot.FAILED:
				self.get_logger().error("ComputeUnit error: STDERR: {0}, STDOUT: {0}".format(unit.stderr, unit.stdout))
				self.get_logger().error("Pattern execution FAILED.")
				sys.exit(1)


		#-----------------------------------------------------------------------
		#
		def create_filecheck_command(files_list):

			command_list = []
			for f in files_list:
				command = 'if [ -f "{0}" ]; then exit 0; else echo "File {0} does not exist" >&2; exit 1; fi;'.format(f)
				command_list.append(command)

			return command_list

            		#-----------------------------------------------------------------------

		self._reporter.ok('>>ok')
		self.get_logger().info("Executing simulation-analysis loop with {0} iterations on {1} allocated core(s) on '{2}'".format(
			pattern.iterations, 
			resource._cores, 
			resource._resource_key))
		self._reporter.header("Executing simulation-analysis loop with {0} iterations on {1} allocated core(s) on '{2}'".format(
			pattern.iterations, 
			resource._cores, 
			resource._resource_key))

		all_cus = []

		#print resource._pilot.description['cores']

		self.get_logger().info("Waiting for pilot on {0} to go Active".format(resource._resource_key))
		self._reporter.info("Job waiting on queue...".format(resource._resource_key))
		resource._pmgr.wait_pilots(resource._pilot.uid,'Active')
		self._reporter.ok("\nJob is now running !".format(resource._resource_key))

		profiling = int(os.environ.get('RADICAL_ENTK_PROFILING',0))

		if profiling == 1:
			from collections import OrderedDict as od
			pattern._execution_profile = []
			enmd_overhead_dict = od()
			cu_dict = od()

		try:
			start_now = datetime.datetime.now()
			resource._umgr.register_callback(unit_state_cb)

			#############################################################
			# EXECUTE PRE-LOOP			
                
			pre_loop = pattern.pre_loop()

			if pre_loop is not None:
				# Pre-loop stage exists in the pattern

				if profiling == 1:
					probe_preloop_start = datetime.datetime.now()
					enmd_overhead_dict['preloop'] = od()
					enmd_overhead_dict['preloop']['start_time'] = probe_preloop_start

				pre_loop._bind_to_resource(resource._resource_key)

				cud = radical.pilot.ComputeUnitDescription()
				cud.name = "pre_loop"

				cud.pre_exec 		= pre_loop._cu_def_pre_exec
				cud.executable     	= pre_loop._cu_def_executable
				cud.arguments      	= pre_loop.arguments
				cud.mpi            		= pre_loop.uses_mpi
				cud.input_staging  	= get_input_data(kernel=pre_loop)
				cud.output_staging 	= get_output_data(kernel=pre_loop)

				if pre_loop.exists_remote is not None:
					cud.post_exec = create_filecheck_command(pre_loop.exists_remote)
                           

				self.get_logger().debug("Created pre_loop CU: {0}.".format(cud.as_dict()))
				self.get_logger().info("Submitted ComputeUnit(s) for pre_loop stage.")
				self._reporter.info("\nWaiting for pre_loop stage to complete.")

				if profiling == 1:
					probe_preloop_wait = datetime.datetime.now()
					enmd_overhead_dict['preloop']['wait_time'] = probe_preloop_wait

				unit = resource._umgr.submit_units(cud)
				all_cus.append(unit)
				resource._umgr.wait_units(unit.uid)

				if profiling == 1:
					probe_preloop_res = datetime.datetime.now()
					enmd_overhead_dict['preloop']['res_time'] = probe_preloop_res

				self.get_logger().info("Pre_loop completed.")

				if unit.state != radical.pilot.DONE:
					raise EnsemblemdError("Pre-loop CU failed with error: {0}".format(unit.stdout))

				self.working_dirs["pre_loop"] = saga.Url(unit.working_directory).path

				if profiling == 1:
					probe_preloop_done = datetime.datetime.now()
					enmd_overhead_dict['preloop']['stop_time'] = probe_preloop_done
					cu_dict['pre_loop'] = unit

				self._reporter.ok('>> done')

			else:
				# Pre-loop does not exist
				self.get_logger().info("No pre_loop stage.")


			###################################################################
			# execute simulation analysis loop

			for iteration in range(1, pattern.iterations+1):

				self.working_dirs['iteration_{0}'.format(iteration)] = {}

				##############################################################
				# EXECUTE SIMULATION STEPS

				if profiling == 1:
					enmd_overhead_dict['iter_{0}'.format(iteration)] = od()
					cu_dict['iter_{0}'.format(iteration)] = od()

				# Get number of kernels in the simulation stage
				if isinstance(pattern.simulation_stage(iteration=iteration, instance=1),list):
					num_sim_kerns = len(pattern.simulation_stage(iteration=iteration, instance=1))
				else:
					num_sim_kerns = 1

				all_sim_cus = []

				if profiling == 1:
					enmd_overhead_dict['iter_{0}'.format(iteration)]['sim']= od()
					cu_dict['iter_{0}'.format(iteration)]['sim']= list()

				# Iterate through each kernel in the simulation stage
				for kern_stage in range(0,num_sim_kerns):

					if profiling == 1:
						probe_sim_start = datetime.datetime.now()
						#Log details of each kernel
						enmd_overhead_dict['iter_{0}'.format(iteration)]['sim']['kernel_{0}'.format(kern_stage)]= od()
						enmd_overhead_dict['iter_{0}'.format(iteration)]['sim']['kernel_{0}'.format(kern_stage)]['start_time'] = probe_sim_start

					# List of CUs for kernels[kernel_stage]
					s_units = []

					#Create CUs for each kernel of the simulation stage
					for s_instance in range(1, pattern._simulation_instances+1):

						# Get details of kernels[kernel_stage]
						if isinstance(pattern.simulation_stage(iteration=iteration, instance=s_instance),list):
							sim_stage = pattern.simulation_stage(iteration=iteration, instance=s_instance)[kern_stage]
						else:
							sim_stage = pattern.simulation_stage(iteration=iteration, instance=s_instance)

						sim_stage._bind_to_resource(resource._resource_key)

						cud = radical.pilot.ComputeUnitDescription()
						cud.name = "sim ;{iteration} ;{instance}".format(iteration=iteration, instance=s_instance)

						cud.pre_exec 	= sim_stage._cu_def_pre_exec
						cud.executable 	= sim_stage._cu_def_executable
						cud.arguments 	= sim_stage.arguments
						cud.mpi          	= sim_stage.uses_mpi

						cud.input_staging  = get_input_data(kernel=sim_stage,instance=s_instance, 
                        						iteration=iteration,ktype='simulation')

						cud.output_staging = get_output_data(kernel=sim_stage,instance=s_instance, 
							iteration=iteration,ktype='simulation')
                     

						if sim_stage.cores is not None:
							cud.cores = sim_stage.cores

						if sim_stage.exists_remote is not None:
							cud.post_exec = create_filecheck_command(sim_stage.exists_remote)

						s_units.append(cud)

						if sim_stage.get_instance_type == 'single':
							break
                        
						self.get_logger().debug("Created simulation CU: {0}.".format(cud.as_dict()))
                    
					self.get_logger().info("Submitting tasks for simulation iteration {0}.".format(iteration))
	
					if profiling == 1:
						probe_sim_wait = datetime.datetime.now()
						enmd_overhead_dict['iter_{0}'.format(iteration)]['sim']['kernel_{0}'.format(kern_stage)]['wait_time'] = probe_sim_wait

					s_cus = resource._umgr.submit_units(s_units)

					self.get_logger().info("Submitted tasks for simulation iteration {0}.".format(iteration))

					# List of ALL CUs
					all_cus.extend(s_cus)
					# List of all SIM CUs
					all_sim_cus.extend(s_cus)

					self.get_logger().info("Waiting for {3} simulations in iteration {0}/ kernel {1}: {2} to complete.".format(
						iteration,kern_stage+1,sim_stage.name,pattern._simulation_instances))
					self._reporter.info("\nIteration {0}: Waiting for {2} simulation tasks: {1} to complete".format(
						iteration,sim_stage.name, pattern._simulation_instances))


					# Wait for the above CUs to finish
					uids = [cu.uid for cu in s_cus]
					resource._umgr.wait_units(uids)


					if profiling == 1:
						probe_sim_res = datetime.datetime.now()
						enmd_overhead_dict['iter_{0}'.format(iteration)]['sim']['kernel_{0}'.format(kern_stage)]['res_time'] = probe_sim_res


					self.get_logger().info("Simulations in iteration {0}/ kernel {1}: {2} completed.".format(
						iteration,kern_stage+1,sim_stage.name))


					failed_units = ""
					for unit in s_cus:
						if unit.state != radical.pilot.DONE:

							failed_units += " * Simulation task {0} failed with an error: {1}\n".format(
								unit.uid, unit.stderr)

					if profiling == 1:
						probe_sim_done = datetime.datetime.now()
						enmd_overhead_dict['iter_{0}'.format(iteration)]['sim']['kernel_{0}'.format(kern_stage)]['stop_time'] = probe_sim_done

					self._reporter.ok('>> done')

				if profiling == 1:
					probe_post_sim_start = datetime.datetime.now()
					enmd_overhead_dict['iter_{0}'.format(iteration)]['sim']['post'] = od()
					enmd_overhead_dict['iter_{0}'.format(iteration)]['sim']['post']['start_time'] = probe_post_sim_start

				# TODO: ensure working_dir <-> instance mapping
				i = 0
				for cu in s_cus:
					i += 1
					self.working_dirs['iteration_{0}'.format(iteration)]['simulation_{0}'.format(i)] = saga.Url(cu.working_directory).path

				if profiling == 1:
					probe_post_sim_end = datetime.datetime.now()
					enmd_overhead_dict['iter_{0}'.format(iteration)]['sim']['post']['stop_time'] = probe_post_sim_end
					cu_dict['iter_{0}'.format(iteration)]['sim'] = all_sim_cus


				##############################################################
				# EXECUTE ANALYSIS STEPS

				# Get number of kernels in the analysis stage
				if isinstance(pattern.analysis_stage(iteration=iteration, instance=1),list):
					num_ana_kerns = len(pattern.analysis_stage(iteration=iteration, instance=1))
				else:
					num_ana_kerns = 1

				all_ana_cus = []

				if profiling == 1:
					enmd_overhead_dict['iter_{0}'.format(iteration)]['ana'] = od()
					cu_dict['iter_{0}'.format(iteration)]['ana']= list()

				# Iterate through each kernel in the analysis stage
				for kern_stage in range(0,num_ana_kerns):

					if profiling == 1:
						probe_ana_start = datetime.datetime.now()
						enmd_overhead_dict['iter_{0}'.format(iteration)]['ana']['kernel_{0}'.format(kern_stage)]= od()
						enmd_overhead_dict['iter_{0}'.format(iteration)]['ana']['kernel_{0}'.format(kern_stage)]['start_time'] = probe_ana_start

					# List of CUs for analysis kernels[kernel_stage]
					a_units = []

					#Create CUs for each kernel of the analysis stage
					for a_instance in range(1, pattern._analysis_instances+1):

						if isinstance(pattern.analysis_stage(iteration=iteration, instance=a_instance),list):
							ana_stage = pattern.analysis_stage(iteration=iteration, instance=a_instance)[kern_stage]
						else:
							ana_stage = pattern.analysis_stage(iteration=iteration, instance=a_instance)

						ana_stage._bind_to_resource(resource._resource_key)

                       
						cud = radical.pilot.ComputeUnitDescription()
						cud.name = "ana ; {iteration}; {instance}".format(iteration=iteration, instance=a_instance)

						cud.pre_exec       	= ana_stage._cu_def_pre_exec
						cud.executable     	= ana_stage._cu_def_executable
						cud.arguments      	= ana_stage.arguments
						cud.mpi            		= ana_stage.uses_mpi
                        
						cud.input_staging 	= get_input_data(kernel=ana_stage,instance=a_instance, 
							iteration=iteration,ktype='analysis')
						cud.output_staging 	= get_output_data(kernel=ana_stage,instance=a_instance, 
							iteration=iteration,ktype='analysis')


						if ana_stage.cores is not None:
							cud.cores = ana_stage.cores

						if ana_stage.exists_remote is not None:
							cud.post_exec = create_filecheck_command(ana_stage.exists_remote)

						a_units.append(cud)

						if ana_stage.get_instance_type == 'single':
							break

						self.get_logger().debug("Created analysis CU: {0}.".format(cud.as_dict()))
                    
					self.get_logger().info("Submitting tasks for analysis iteration {0}.".format(iteration))

					if profiling == 1:
						probe_ana_wait = datetime.datetime.now()
						enmd_overhead_dict['iter_{0}'.format(iteration)]['ana']['kernel_{0}'.format(kern_stage)]['wait_time'] = probe_ana_wait


					a_cus = resource._umgr.submit_units(a_units)
				
					# List of ALL CUs
					all_cus.extend(a_cus)
					# List of all ANA CUs
					all_ana_cus.extend(a_cus)

					self.get_logger().info("Waiting for analysis tasks in iteration {0}/kernel {1}: {2} to complete.".format(
						iteration,kern_stage+1,ana_stage.name))
					self._reporter.info("\nIteration {0}: Waiting for analysis tasks: {1} to complete".format(
						iteration,ana_stage.name))
                    
                    				# Wait for the above CUs to finish
					uids = [cu.uid for cu in a_cus]
					resource._umgr.wait_units(uids)

					if profiling == 1:
						probe_ana_res = datetime.datetime.now()
						enmd_overhead_dict['iter_{0}'.format(iteration)]['ana']['kernel_{0}'.format(kern_stage)]['res_time'] = probe_ana_res

					self.get_logger().info("Analysis in iteration {0}/kernel {1}: {2} completed.".format(
						iteration,kern_stage+1,ana_stage.name))


					failed_units = ""
					for unit in a_cus:
						if unit.state != radical.pilot.DONE:
							failed_units += " * Analysis task {0} failed with an error: {1}\n".format(unit.uid, unit.stderr)

					if profiling == 1:
						probe_ana_done = datetime.datetime.now()
						enmd_overhead_dict['iter_{0}'.format(iteration)]['ana']['kernel_{0}'.format(kern_stage)]['stop_time'] = probe_ana_done

					self._reporter.ok('>> done')

				if profiling == 1:
					probe_post_ana_start = datetime.datetime.now()
					enmd_overhead_dict['iter_{0}'.format(iteration)]['ana']['post'] = od()
					enmd_overhead_dict['iter_{0}'.format(iteration)]['ana']['post']['start_time'] = probe_post_ana_start

				# Check for pattern adaptivity
				if (pattern.adaptive_simulation == False):
					pass
				else:
					# If true, extract number of sims from the analysis output
					pattern._simulation_instances = pattern.get_new_simulation_instances(a_cus[0].stdout)

				i = 0
				for cu in a_cus:
					i += 1
					self.working_dirs['iteration_{0}'.format(iteration)]['analysis_{0}'.format(i)] = saga.Url(cu.working_directory).path

				if profiling == 1:
					probe_post_ana_end = datetime.datetime.now()
					enmd_overhead_dict['iter_{0}'.format(iteration)]['ana']['post']['stop_time'] = probe_post_ana_end
					cu_dict['iter_{0}'.format(iteration)]['ana'] = all_ana_cus


			# Pattern execution completed
			self._reporter.header('Pattern execution successfully finished')

			if profiling == 1:
				return enmd_overhead_dict, cu_dict
			else:
				return None

		except KeyboardInterrupt:

			self._reporter.error('Execution interupted')
			traceback.print_exc()

	
