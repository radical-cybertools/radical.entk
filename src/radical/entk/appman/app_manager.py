__author__    = "Vivek Balasubramanian <vivek.balasubramanian@rutgers.edu>"
__copyright__ = "Copyright 2016, http://radical.rutgers.edu"
__license__   = "MIT"

from radical.entk.exceptions import *
from radical.entk.kernel_plugins.kernel_base import KernelBase
from radical.entk.kernel_plugins.kernel import Kernel
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

			if user_kernel == None:
				return None

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

					if user_kernel.timeout != None:
						new_kernel.timeout = user_kernel.timeout

					new_kernel.cancel_tasks = user_kernel.cancel_tasks

					new_kernel.validate_arguments()

					self._logger.debug("Kernel {0} validated".format(new_kernel.name))

					return new_kernel

			if found==False:
				self._logger.error("Kernel {0} does not exist".format(user_kernel.name))
				raise Exception()

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


	def add_to_record(self, pattern_name, record, cus, iteration, stage, instance=None, monitor=False):

		try:

			# Differences between EoP and PoE
			if instance==None:
				#PoE
				inst=1
			else:
				#EoP
				inst=instance

			if type(cus) != list:
				cus = [cus]

			pat_key = "pat_{0}".format(pattern_name)

			# Add monitor details to record for PoE pattern
			if ((monitor == True)and(instance==None)):

				self._logger.debug('PoE: Adding Monitor to record')

				record[pat_key]["iter_{0}".format(iteration)]["stage_{0}".format(stage)]["monitor_1"] = dict()

				record[pat_key]["iter_{0}".format(iteration)]["stage_{0}".format(stage)]["monitor_1"]["output"] = cus[0].stdout
				record[pat_key]["iter_{0}".format(iteration)]["stage_{0}".format(stage)]["monitor_1"]["uid"] = cus[0].uid
				record[pat_key]["iter_{0}".format(iteration)]["stage_{0}".format(stage)]["monitor_1"]["path"] = cus[0].working_directory				

				record[pat_key]["iter_{0}".format(iteration)]["stage_{0}".format(stage)]['status'] = 'Done'

				self._logger.debug('EoP: Added Monitor to record')

				#self._logger.debug(record)
				return record

			# Add monitor details to record for EoP pattern
			if ((monitor==True)and(instance!=None)):

				self._logger.debug('EoP: Adding Monitor to record')

				record[pat_key]["iter_{0}".format(iteration)]["stage_{0}".format(stage)]["monitor_{0}".format(instance)] = dict()

				record[pat_key]["iter_{0}".format(iteration)]["stage_{0}".format(stage)]["monitor_{0}".format(instance)]["output"] = cus[0].stdout
				record[pat_key]["iter_{0}".format(iteration)]["stage_{0}".format(stage)]["monitor_{0}".format(instance)]["uid"] = cus[0].uid
				record[pat_key]["iter_{0}".format(iteration)]["stage_{0}".format(stage)]["monitor_{0}".format(instance)]["path"] = cus[0].working_directory				

				record[pat_key]["iter_{0}".format(iteration)]["stage_{0}".format(stage)]['status'] = 'Done'

				self._logger.debug('EoP: Added Monitor to record')

				return record

			self._logger.debug('Adding Tasks to record')

			for cu in cus:

				if "iter_{0}".format(iteration) not in record[pat_key]:
					record[pat_key]["iter_{0}".format(iteration)] = dict()

				if "stage_{0}".format(stage) not in record[pat_key]["iter_{0}".format(iteration)]:
					record[pat_key]["iter_{0}".format(iteration)]["stage_{0}".format(stage)] = dict()

				if "instance_{0}".format(inst) not in record[pat_key]["iter_{0}".format(iteration)]["stage_{0}".format(stage)]:
					record[pat_key]["iter_{0}".format(iteration)]["stage_{0}".format(stage)]["instance_{0}".format(inst)] = dict()
					record[pat_key]["iter_{0}".format(iteration)]["stage_{0}".format(stage)]['branch'] = record[pat_key]["iter_1"]["stage_{0}".format(stage)]['branch']
					record[pat_key]["iter_{0}".format(iteration)]["stage_{0}".format(stage)]['status'] = 'New'

				record[pat_key]["iter_{0}".format(iteration)]["stage_{0}".format(stage)]["instance_{0}".format(inst)]["output"] = cu.stdout
				record[pat_key]["iter_{0}".format(iteration)]["stage_{0}".format(stage)]["instance_{0}".format(inst)]["uid"] = cu.uid
				record[pat_key]["iter_{0}".format(iteration)]["stage_{0}".format(stage)]["instance_{0}".format(inst)]["path"] = cu.working_directory

				inst+=1

			stage_done=True

			if instance==None:
				inst=1
			else:
				inst=instance

			for cu in cus:
				val = record[pat_key]["iter_{0}".format(iteration)]["stage_{0}".format(stage)]["instance_{0}".format(inst)]['path']
				inst+=1
				if (val==None):
					stage_done=False
					break

			if stage_done:
				record[pat_key]["iter_{0}".format(iteration)]["stage_{0}".format(stage)]['status'] = 'Running'
				
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


	def run(self, resource, task_manager, rp_session):

		try:
			# Create dictionary for logging
			record = self.get_record()

			# For data transfer, inform pattern of the resource
			self._pattern.session_id = rp_session

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
					while(self._pattern.cur_iteration <= self._pattern.total_iterations):
			
						#for self._pattern.next_stage in range(1, self._pattern.pipeline_size+1):
						while ((self._pattern.next_stage<=self._pattern.pipeline_size)and(self._pattern.next_stage!=0)):

							# Get kernel from execution pattern
							stage =	 self._pattern.get_stage(stage=self._pattern.next_stage)

							validated_kernels = list()
							validated_monitors = list()

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
										stage_kernel = stage_instance_return[0]
										stage_monitor = stage_instance_return[1]
									else:
										stage_kernel = stage_instance_return[0]
										stage_monitor = None
								else:
									stage_kernel = stage_instance_return
									stage_monitor = None
									
								validated_kernels.append(self.validate_kernel(stage_kernel))
							validated_monitor = self.validate_kernel(stage_monitor)


							# Pass resource-unbound kernels to execution plugin
							#print len(list_kernels_stage)
							plugin.set_workload(kernels=validated_kernels, monitor=validated_monitor)
							cus = plugin.execute(record=record, pattern_name=self._pattern.name, iteration=self._pattern.cur_iteration, stage=1)

							# Update record
							record = self.add_to_record(record=record, cus=cus, pattern_name = self._pattern.name, iteration=self._pattern.cur_iteration, stage=self._pattern.next_stage)

							# Check if montior exists
							if plugin.monitor != None:
								cu = plugin.execute_monitor(record=record, tasks=cus, cur_pat=self._pattern.name, cur_iter=self._pattern.cur_iteration, cur_stage=self._pattern.next_stage)
								
								# Update record
								record = self.add_to_record(record=record, cus=cu, pattern_name = self._pattern.name, iteration=self._pattern.cur_iteration, stage=self._pattern.next_stage, monitor=True)

							self._pattern.pattern_dict = record["pat_{0}".format(self._pattern.name)] 

							#print record
							branch_function = None

							# Execute branch if it exists
							if (record["pat_{0}".format(self._pattern.name)]["iter_{0}".format(self._pattern.cur_iteration)]["stage_{0}".format(self._pattern.next_stage)]["branch"]):
								self._logger.info('Executing branch function branch_{0}'.format(self._pattern.next_stage))
								branch_function = self._pattern.get_branch(stage=self._pattern.next_stage)
								branch_function()

							#print self._pattern.stage_change
							if (self._pattern.stage_change==True):
								pass
							else:
								self._pattern.next_stage+=1

							self._pattern.stage_change = False

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

					# List of all CUs
					all_cus = []

				except Exception, ex:
					self._logger.error("Plugin setup failed, error: {0}".format(ex))
					raise


				try:


					#def handle_monitor(record=record, plugin=plugin, task=unit, cur_pat=self._pattern.name, cur_iter=self._pattern.cur_iteration, cur_stage=cur_stage, cur_task=cur_task):
					def handle_monitor(record, plugin, task, cur_pat, cur_iter, cur_stage, cur_task):
						cu = plugin.execute_monitor(record=record, task=task, cur_pat=cur_pat, cur_iter=cur_iter, cur_stage=cur_stage, cur_task=cur_task)
						
						# Update record
						self.add_to_record(record=record, cus=cu, pattern_name = cur_pat, iteration=cur_iter, stage=cur_stage, instance=cur_task, monitor=True)

						return


					def unit_state_cb (unit, state) :

						# Perform these operations only for tasks and not monitors

						if unit.name.startswith('stage'):

							self._logger.debug('Callback initiated for {0}, state: {1}'.format(unit.name, state))

							if state == rp.FAILED:
								cur_stage = int(unit.name.split('-')[1])
								cur_pipe = int(unit.name.split('-')[3])
								self._logger.error("Stage {0} of pipeline {1} failed: UID: {2}, STDERR: {3}, STDOUT: {4} LAST LOG: {5}".format(cur_stage, cur_pipe, unit.uid, unit.stderr, unit.stdout, unit.log[-1]))
								self._logger.error("Pattern execution FAILED.")
								sys.exit(1)

							if state == rp.AGENT_STAGING_INPUT_PENDING:

								try:
									cur_stage = int(unit.name.split('-')[1])
									cur_task = int(unit.name.split('-')[3])
									self._logger.debug("Unit directories created for pipe: {0}, stage: {1}".format(cur_task, cur_stage))
									record=self.get_record()
									self.add_to_record(record=record, cus=unit, pattern_name = self._pattern.name, iteration=self._pattern.cur_iteration[cur_task-1], stage=cur_stage, instance=cur_task)

									# Now that we have the unit diretories, we can start the monitor !
									if plugin.monitor[cur_task-1] != None:

										import threading

										thread = threading.Thread(target=handle_monitor, name='monitor_{0}'.format(cur_task-1),args=(record, plugin, unit, self._pattern.name, self._pattern.cur_iteration[cur_task-1], cur_stage, cur_task))
										plugin.monitor_thread[cur_task-1] = thread
										plugin.monitor_thread[cur_task-1].start()						
								
									#self._logger.info(record)

								except Exception, ex:
									self._logger.error("Monitor execution failed, error: {0}".format(ex))
									raise


							if ((state == rp.DONE)or(state==rp.CANCELED)):

								try:

									cur_stage = int(unit.name.split('-')[1])
									cur_task = int(unit.name.split('-')[3])

									# Close monitoring thread
									if plugin.monitor[cur_task-1] != None:
										plugin.monitor_thread[cur_task-1].join()									

										if plugin.monitor_thread[cur_task-1].is_alive() != True:
											self._logger.debug('Closing thread {0}'.format(plugin.monitor_thread[cur_task-1].name))

										plugin.monitor_thread[cur_task-1] = None


									record=self.get_record()

									self._logger.info('Stage {1} of pipeline {0} has finished'.format(cur_task,cur_stage))
									#-----------------------------------------------------------------------
									# Increment tasks list accordingly
									plugin.tot_fin_tasks[cur_stage-1]+=1

									record=self.add_to_record(record=record, cus=unit, pattern_name = self._pattern.name, iteration=self._pattern.cur_iteration[cur_task-1], stage=cur_stage, instance=cur_task)
									self._pattern.pattern_dict = record["pat_{0}".format(self._pattern.name)] 

									# Check for branch function for current stage
									branch_function = None

									# Execute branch if it exists
									if (record["pat_{0}".format(self._pattern.name)]["iter_{0}".format(self._pattern.cur_iteration[cur_task-1])]["stage_{0}".format(cur_stage)]["branch"]):
										self._logger.info('Executing branch function branch_{0}'.format(cur_stage))
										branch_function = self._pattern.get_branch(stage=cur_stage)
										branch_function(instance=cur_task)								

									# Check if next stage was changed by branching function
									if (self._pattern.stage_change==True):
										if self._pattern.new_stage !=0:
											if cur_stage < self._pattern.new_stage:
												self._pattern._incremented_tasks[cur_task-1] -= self._pattern.new_stage - cur_stage - 1
											elif cur_stage >= self._pattern.new_stage:
												self._pattern._incremented_tasks[cur_task-1] -= abs(cur_stage - self._pattern.pipeline_size)
												self._pattern._incremented_tasks[cur_task-1] += abs(self._pattern.pipeline_size - self._pattern.new_stage) + 1
											#self._pattern._incremented_tasks[cur_task-1] += abs(cur_stage - self._pattern.new_stage) + 1
										else:
											self._pattern._incremented_tasks[cur_task-1] -= abs(cur_stage - self._pattern.pipeline_size)

										if self._pattern.next_stage[cur_task-1] >= self._pattern.new_stage:
											self._pattern.cur_iteration[cur_task-1] += 1

										self._pattern.next_stage[cur_task-1] = self._pattern.new_stage
									else:
										self._pattern.next_stage[cur_task-1] +=1

									self._pattern.stage_change = False
									self._pattern.new_stage = None

									# Terminate execution
									if self._pattern.next_stage[cur_task-1] == 0:
										self._logger.info("Branching function has set termination condition -- terminating pipeline {0}".format(cur_task))

									# Check if this is the last task of the stage
									if plugin.tot_fin_tasks[cur_stage-1] == self._pattern.ensemble_size:
										self._logger.info('Stage {0} of all pipelines has finished'.format(cur_stage))


									if ((self._pattern.next_stage[cur_task-1]<= self._pattern.pipeline_size)and(self._pattern.next_stage[cur_task-1] !=0)):
								
										stage =	 self._pattern.get_stage(stage=self._pattern.next_stage[cur_task-1])
										stage_instance_return = stage(cur_task)

										stage_monitor = None

										if type(stage_instance_return) == list:

											if len(stage_instance_return) == 2:

												stage_kernel = stage_instance_return[0]
												stage_monitor = stage_instance_return[1]
									
											else:
												stage_kernel = stage_instance_return[0]
												stage_monitor = None

										else:
											stage_kernel = stage_instance_return
											stage_monitor = None
									
										validated_kernel = self.validate_kernel(stage_kernel)
										validated_monitor = self.validate_kernel(stage_monitor)


										plugin.set_workload(kernels=validated_kernel, monitor=validated_monitor, cur_task=cur_task)
										cud = plugin.create_tasks(record=record, pattern_name=self._pattern.name, iteration=self._pattern.cur_iteration[cur_task-1], stage=self._pattern.next_stage[cur_task-1], instance=cur_task)				
										cu = plugin.execute_tasks(tasks=cud)
										all_cus.append(cu)

								except Exception, ex:
									self._logger.error('Failed to trigger next stage, error: {0}'.format(ex))
									raise

					#register callbacks
					task_manager.register_callback(unit_state_cb)

					# Get kernel from execution pattern
					stage =	 self._pattern.get_stage(stage=1)

					validated_kernels = list()
					validated_monitors = list()

					# Validate user specified Kernel with KernelBase and return fully defined but resource-unbound kernel
					# Create instance key/vals for each stage
					
					instances = self._pattern.ensemble_size
					
					for inst in range(1, instances+1):

						stage_instance_return = stage(inst)

						if type(stage_instance_return) == list:
							if len(stage_instance_return) == 2:
								stage_kernel = stage_instance_return[0]
								stage_monitor = stage_instance_return[1]
							else:
								stage_kernel = stage_instance_return[0]
								stage_monitor = None
						else:
							stage_kernel = stage_instance_return
							stage_monitor = None
									
						validated_kernels.append(self.validate_kernel(stage_kernel))
						validated_monitors.append(self.validate_kernel(stage_monitor))


					# Pass resource-unbound kernels to execution plugin
					plugin.set_workload(kernels=validated_kernels, monitor=validated_monitors)
					cus = plugin.create_tasks(record=record, pattern_name=self._pattern.name, iteration=1, stage=1)
					cus = plugin.execute_tasks(tasks=cus)
					all_cus.extend(cus)

					while(sum(plugin.tot_fin_tasks)!=(self._pattern.pipeline_size*self._pattern.ensemble_size + sum(self._pattern._incremented_tasks) )):
					#while True:

						pending_cus = []
						done_cus = []
						for unit in all_cus:
							if ((unit.state!=rp.DONE) and (unit.state!=rp.CANCELED)):
								pending_cus.append(unit.uid)
							else:
								done_cus.append(unit)

						for unit in done_cus:
							all_cus.remove(unit)

						#self._logger.debug('All: {0}, Pending: {1}'.format(len(all_cus), len(pending_cus)))

						#if len(pending_cus)==0:
						#	break
						#else:
						task_manager.wait_units(pending_cus, timeout=60) 

				except Exception, ex:
					self._logger.error("EoP Pattern execution failed, error: {0}".format(ex))
					raise


		except Exception, ex:
			self._logger.error("App manager failed at workload execution, error: {0}".format(ex))
			raise
