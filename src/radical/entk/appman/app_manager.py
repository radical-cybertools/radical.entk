__author__    = "Vivek Balasubramanian <vivek.balasubramanian@rutgers.edu>"
__copyright__ = "Copyright 2016, http://radical.rutgers.edu"
__license__   = "MIT"

from radical.entk.exceptions import *
from radical.entk.kernel_plugins.kernel_base import KernelBase
from radical.entk.kernel_plugins.kernel import Kernel
from radical.entk.monitors.monitor import Monitor
from radical.entk.execution_pattern import ExecutionPattern
from radical.entk.unit_patterns.poe.poe import PoE
from radical.entk.unit_patterns.eop.eop import EoP

import radical.utils as ru
import radical.pilot as rp
import sys

class AppManager():

	def __init__(self, name=None):

		self._name = name

		self._pattern = None
		self._loaded_kernels = list()
		self._loaded_plugins = list() 

		self._logger = ru.get_logger("radical.entk.appman")
		self._logger.info("Application Manager created")
		self._reporter = self._logger.report

		self._kernel_dict = dict()

		# Uncomment once ExecutionPattern class is available
		#self.sanity_check()

		# Load default exec plugins
		#self.load_plugins()


	def sanity_pattern_check(self):
		if self._pattern.__class__.__base__.__base__ != ExecutionPattern:
			raise TypeError(expected_type="(derived from) ExecutionPattern", actual_type=type(self._pattern))

	@property
	def name(self):
		return self._name


	def register_kernels(self, kernel_class):

		#print kernel_class.__base__
		try:
			if type(kernel_class) == list:
				for item in kernel_class:
					if not hasattr(item, '__base__'):
						raise TypeError(expected_type="KernelBase", actual_type = type(item))					
					elif item.__base__ != Kernel:
						raise TypeError(expected_type="KernelBase", actual_type = type(item()))		

					if item in self._loaded_kernels:
						raise ExistsError(item='{0}'.format(item().name), parent = 'loaded_kernels')

					self._loaded_kernels.append(item)
					self._logger.info("Kernel {0} registered with application manager".format(item().name))

			elif not hasattr(kernel_class,'__base__'):
				raise TypeError(expected_type="KernelBase", actual_type = type(kernel_class))

			elif kernel_class.__base__ != KernelBase:
				raise TypeError(expected_type="KernelBase", actual_type = type(kernel_class()))

			else:
				self._loaded_kernels.append(kernel_class)
				self._logger.info("Kernel {0} registered with application manager".format(kernel_class().name))
		
		except Exception, ex:

				self._logger.error("Kernel registration failed: {0}".format(ex))
				raise


	def list_kernels(self):

		try:
			registered_kernels = list()
			for item in self._loaded_kernels:
				registered_kernels.append(item().name)

			return registered_kernels

		except Exception, ex:

			self._logger.error("Could not list kernels: {0}".format(ex))
			raise


	def save(self, pattern):
		self._pattern = pattern
		self.sanity_pattern_check()

		# Convert pattern to JSON
		self.pattern_to_json(pattern)


	def pattern_to_json(self, pattern):
		pass


	def validate_kernel(self, user_kernel):

		try:
			found=False
			for kernel in self._loaded_kernels:

				if kernel().name == user_kernel.name:

					found=True

					new_kernel = kernel()

					if user_kernel.pre_exec != None:
						new_kernel.pre_exec = user_kernel.pre_exec

					if user_kernel.executable != None:
						new_kernel.executable = user_kernel.executable

					if user_kernel.arguments != None:
						new_kernel.arguments = user_kernel.arguments

					if user_kernel.uses_mpi != None:	
						new_kernel.uses_mpi = user_kernel.uses_mpi

					if user_kernel.cores != None:
						new_kernel.cores = user_kernel.cores

					if user_kernel.upload_input_data != None:
						new_kernel.upload_input_data = user_kernel.upload_input_data

					if user_kernel.copy_input_data != None:
						new_kernel.copy_input_data = user_kernel.copy_input_data

					if user_kernel.link_input_data != None:
						new_kernel.link_input_data = user_kernel.link_input_data

					if user_kernel.copy_output_data != None:
						new_kernel.copy_output_data = user_kernel.copy_output_data

					if user_kernel.download_output_data != None:
						new_kernel.download_output_data = user_kernel.download_output_data

					new_kernel.validate_arguments()

					self._logger.debug("Kernel {0} validated".format(new_kernel.name))

					return new_kernel

			if found==False:
				self._logger.error("Kernel {0} does not exist".format(user_kernel.name))
				raise 

		except Exception, ex:

			self._logger.error('Kernel validation failed: {0}'.format(ex))
			raise


	def add_workload(self, pattern):
		self._pattern = pattern
		try:
			self.create_record(pattern.name, pattern.total_iterations, pattern.pipeline_size, pattern.ensemble_size)
		except Exception, ex:
			self._logger.error("Create new record function call for added pattern failed, error : {0}".format(ex))
			raise


	def add_to_record(self, pattern_name, record, cus, iteration, stage, instance=None):

		try:
			if instance==None:
				inst=1
			else:
				inst=instance
				cus = [cus]

			pat_key = "pat_{0}".format(pattern_name)

			for cu in cus:

				record[pat_key]["iter_{0}".format(iteration)]["stage_{0}".format(stage)]["instance_{0}".format(inst)]["output"] = cu.stdout
				record[pat_key]["iter_{0}".format(iteration)]["stage_{0}".format(stage)]["instance_{0}".format(inst)]["uid"] = cu.uid
				record[pat_key]["iter_{0}".format(iteration)]["stage_{0}".format(stage)]["instance_{0}".format(inst)]["path"] = cu.working_directory

				inst+=1

			return record

		except Exception, ex:
			self._logger.error("Could not add new CU data to record, error: {0}".format(ex))
			raise


	def create_record(self, pattern_name, total_iterations, pipeline_size, ensemble_size):


		try:
			pat_key = "pat_{0}".format(pattern_name)
			self._kernel_dict[pat_key] = dict()

			for iter in range(1, total_iterations+1):
				self._kernel_dict[pat_key]["iter_{0}".format(iter)] = dict()

				for stage in range(1, pipeline_size+1):
					self._kernel_dict[pat_key]["iter_{0}".format(iter)]["stage_{0}".format(stage)]  = dict()

					# Set kernel default status
					self._kernel_dict[pat_key]["iter_{0}".format(iter)]["stage_{0}".format(stage)]['status'] = 'New'

					# Set available branches
					if getattr(self._pattern,'branch_{0}'.format(stage), False):
						self._kernel_dict[pat_key]["iter_{0}".format(iter)]["stage_{0}".format(stage)]['branch'] = True
					else:
						self._kernel_dict[pat_key]["iter_{0}".format(iter)]["stage_{0}".format(stage)]['branch'] = False
		
					# Create instance key/vals for each stage
					if type(ensemble_size) == int:
						instances = ensemble_size
					elif type( ensemble_size) == list:
						instances = ensemble_size[stage-1]

					for inst in range(1, instances+1):
						self._kernel_dict[pat_key]["iter_{0}".format(iter)]["stage_{0}".format(stage)]["instance_{0}".format(inst)] = dict()
						self._kernel_dict[pat_key]["iter_{0}".format(iter)]["stage_{0}".format(stage)]["instance_{0}".format(inst)]["output"] = None
						self._kernel_dict[pat_key]["iter_{0}".format(iter)]["stage_{0}".format(stage)]["instance_{0}".format(inst)]["uid"] = None
						self._kernel_dict[pat_key]["iter_{0}".format(iter)]["stage_{0}".format(stage)]["instance_{0}".format(inst)]["path"] = None

		except Exception, ex:

			self._logger.error("New record creation failed, error: {0}".format(ex))
			raise


	def get_record(self):
		return self._kernel_dict


	def run(self, resource, task_manager):

		try:
			# Create dictionary for logging
			record = self.get_record()

			if self._pattern.__class__.__base__ == PoE:
	
				# Based on the execution pattern, the app manager should choose the execution plugin
				try:
					from radical.entk.execution_plugin.poe import PluginPoE

					plugin = PluginPoE()				
					plugin.register_resource(resource = resource)
					plugin.add_manager(task_manager)

				except Exception, ex:
					self._logger.error("PoE Plugin setup failed, error: {0}".format(ex))


				try:
					# Submit kernels stage by stage to execution plugin
					while((self._pattern.iterative==True)or(self._pattern.cur_iteration <= self._pattern.total_iterations)):
			
						#for self._pattern.next_stage in range(1, self._pattern.pipeline_size+1):
						while ((self._pattern.next_stage<=self._pattern.pipeline_size)and(self._pattern.next_stage!=0)):

							# Get kernel from execution pattern
							stage =	 self._pattern.get_stage(stage=self._pattern.next_stage)

							list_kernels_stage = list()

							# Validate user specified Kernel with KernelBase and return fully defined but resource-unbound kernel
							# Create instance key/vals for each stage
							if type(self._pattern.ensemble_size) == int:
								instances = self._pattern.ensemble_size
							elif type(self._pattern.ensemble_size) == list:
								instances = self._pattern.ensemble_size[self._pattern.next_stage-1]

							# Initialization
							stage_monitor = None

							for inst in range(1, instances+1):

								stage_instance_return = stage(inst)

								if type(stage_instance_return) == list:
									if len(stage_instance_return) == 2:
										for item in stage_instance_return:
											if type(item) == Kernel:
												stage_kernel = item
											elif ((type(item) == Monitor) and stage_monitor == None):
												stage_monitor = item
									else:
										stage_kernel = stage_instance_return[0]
								else:
									stage_kernel = stage_instance_return
									
								list_kernels_stage.append(self.validate_kernel(stage_kernel))


							# Pass resource-unbound kernels to execution plugin
							#print len(list_kernels_stage)
							plugin.set_workload(kernels=list_kernels_stage, monitor=stage_monitor)
							cus = plugin.execute(record=record, pattern_name=self._pattern.name, iteration=self._pattern.cur_iteration, stage=self._pattern.next_stage)				

							# Update record
							record = self.add_to_record(record=record, cus=cus, pattern_name = self._pattern.name, iteration=self._pattern.cur_iteration, stage=self._pattern.next_stage)
							print record

							self._pattern.pattern_dict = record["pat_{0}".format(self._pattern.name)] 

							#print record
							branch_function = None

							# Execute branch if it exists
							if (record["pat_{0}".format(self._pattern.name)]["iter_{0}".format(self._pattern.cur_iteration)]["stage_{0}".format(self._pattern.next_stage)]["branch"]):
								self._logger.info('Executing branch function branch_{0}'.format(self._pattern.next_stage))
								branch_function = self._pattern.get_branch(stage=self._pattern.next_stage)
								branch_function()

							#print self._pattern.state_change
							if (self._pattern.state_change==True):
								pass
							else:
								self._pattern.next_stage+=1

							self._pattern.state_change = False

							# Terminate execution
							if self._pattern.next_stage == 0:
								self._logger.info("Branching function has set termination condition -- terminating")
								break
					

						# Terminate execution
						if self._pattern.next_stage == 0:
							break

						self._pattern.cur_iteration+=1

				except Exception, ex:
					self._logger.error("PoE Workload submission failed, error: {0}".format(ex))
					raise


			# App Manager actions for EoP pattern
			if self._pattern.__class__.__base__ == EoP:
	
				# Based on the execution pattern, the app manager should choose the execution plugin
				try:
					from radical.entk.execution_plugin.eop import PluginEoP

					plugin = PluginEoP()				
					plugin.register_resource(resource = resource)
					plugin.add_manager(task_manager)
					num_stages = self._pattern.pipeline_size
					num_tasks = self._pattern.ensemble_size

				except Exception, ex:
					self._logger.error("Plugin setup failed, error: {0}".format(ex))


				try:

					def unit_state_cb (unit, state) :

						if state == rp.FAILED:
							self._logger.error("Task with ID {0} failed: STDERR: {1}, STDOUT: {2} LAST LOG: {3}".format(unit.uid, unit.stderr, unit.stdout, unit.log[-1]))
							self._logger.error("Pattern execution FAILED.")
							sys.exit(1)

						if state == rp.DONE:

							try:

								cur_stage = int(unit.name.split('-')[1])
								cur_task = int(unit.name.split('-')[3])

								record=self.get_record()

								self._logger.info('Task {0} of stage {1} has finished'.format(cur_task,cur_stage))
								#-----------------------------------------------------------------------
								# Increment tasks list accordingly
								plugin.tot_fin_tasks[cur_stage-1]+=1

								record=self.add_to_record(record=record, cus=unit, pattern_name = self._pattern.name, iteration=self._pattern.cur_iteration, stage=cur_stage, instance=cur_task)
								self._pattern.pattern_dict = record["pat_{0}".format(self._pattern.name)] 

								# Check for branch function for current stage
								branch_function = None

								# Execute branch if it exists
								if (record["pat_{0}".format(self._pattern.name)]["iter_{0}".format(self._pattern.cur_iteration)]["stage_{0}".format(cur_stage)]["branch"]):
									self._logger.info('Executing branch function branch_{0}'.format(cur_stage))
									branch_function = self._pattern.get_branch(stage=cur_stage)
									branch_function(instance=cur_task)								

								# Check if next stage was changed by branching function
								if (self._pattern.state_change==True):
									if self._pattern.new_stage !=0:
										self._pattern._incremented_tasks[cur_task-1] += abs(cur_stage - self._pattern.new_stage) + 1
									else:
										self._pattern._incremented_tasks[cur_task-1] -= abs(cur_stage - self._pattern.pipeline_size)

									self._pattern.next_stage[cur_task-1] = self._pattern.new_stage
								else:
									self._pattern.next_stage[cur_task-1] +=1

								self._pattern.state_change = False
								self._pattern.new_stage = None

								# Terminate execution
								if self._pattern.next_stage[cur_task-1] == 0:
									self._logger.info("Branching function has set termination condition -- terminating pipeline {0}".format(cur_task))

								# Check if this is the last task of the stage
								if plugin.tot_fin_tasks[cur_stage-1] == self._pattern.ensemble_size:
									self._logger.info('All tasks in stage {0} have finished'.format(cur_stage))


								if ((self._pattern.next_stage[cur_task-1]<= self._pattern.pipeline_size)and(self._pattern.next_stage[cur_task-1] !=0)):
								
									stage =	 self._pattern.get_stage(stage=self._pattern.next_stage[cur_task-1])
									stage_instance_return = stage(cur_task)

									stage_monitor = None

									if type(stage_instance_return) == list:
										if len(stage_instance_return) == 2:
											for item in stage_instance_return:
												if type(item) == Kernel:
													stage_kernel = item
												elif ((type(item) == Monitor) and stage_monitor == None):
													stage_monitor = item
										else:
											stage_kernel = stage_instance_return[0]
									else:
										stage_kernel = stage_instance_return
									
									validated_kernel = self.validate_kernel(stage_kernel)

									plugin.set_workload(kernels=validated_kernel, monitor=stage_monitor)
									cud = plugin.create_tasks(record=record, pattern_name=self._pattern.name, iteration=self._pattern.cur_iteration, stage=self._pattern.next_stage[cur_task-1], instance=cur_task)				
									cus = plugin.execute_tasks(tasks=cud)

							except Exception, ex:
								self._logger.error('Failed to trigger next stage, error: {0}'.format(ex))
								raise

					#register callbacks
					task_manager.register_callback(unit_state_cb)

					# Get kernel from execution pattern
					stage =	 self._pattern.get_stage(stage=1)

					list_kernels_stage = list()

					# Validate user specified Kernel with KernelBase and return fully defined but resource-unbound kernel
					# Create instance key/vals for each stage
					
					instances = self._pattern.ensemble_size
					
					# Initialization
					stage_monitor = None

					for inst in range(1, instances+1):

						stage_instance_return = stage(inst)

						if type(stage_instance_return) == list:
							if len(stage_instance_return) == 2:
								for item in stage_instance_return:
									if type(item) == Kernel:
										stage_kernel = item
									elif ((type(item) == Monitor) and stage_monitor == None):
										stage_monitor = item
							else:
								stage_kernel = stage_instance_return[0]
						else:
							stage_kernel = stage_instance_return
									
						list_kernels_stage.append(self.validate_kernel(stage_kernel))

					# Pass resource-unbound kernels to execution plugin
					#print len(list_kernels_stage)
					plugin.set_workload(kernels=list_kernels_stage, monitor=stage_monitor)
					cus = plugin.create_tasks(record=record, pattern_name=self._pattern.name, iteration=self._pattern.cur_iteration, stage=1)
					cus = plugin.execute_tasks(tasks=cus)

					while(sum(plugin.tot_fin_tasks)!=(self._pattern.pipeline_size*self._pattern.ensemble_size + sum(self._pattern._incremented_tasks) )):
						task_manager.wait_units() 

				except Exception, ex:
					self._logger.error("EoP Pattern execution failed, error: {0}".format(ex))
					raise


		except Exception, ex:
			self._logger.error("App manager failed at workload execution, error: {0}".format(ex))
			raise
