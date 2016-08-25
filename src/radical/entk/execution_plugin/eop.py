__author__    = "Vivek Balasubramanian <vivek.balasubramanian@rutgers.edu>"
__copyright__ = "Copyright 2016, http://radical.rutgers.edu"
__license__   = "MIT"


from plugin_base import PluginBase
import radical.pilot as rp
import radical.utils as ru

import saga

_plugin_info = {
			'name': 'eop',
			'type': 'static'
		}

class PluginEoP(object):

	def __init__(self):

		self._executable_workload = None
		self._resource = None
		self._manager = None
		self._monitor = list()

		self._logger = ru.get_logger("radical.entk.plugin.eop")
		self._reporter = self._logger.report

		self._tot_fin_tasks=[0]

		self._logger.info("Plugin EoP created")


	def register_resource(self, resource):
		self._resource = resource
		self._logger.info("Registered resource {0} with execution plugin".format(resource))

	def get_resources(self):
		return self._resource

	@property
	def tot_fin_tasks(self):
		return self._tot_fin_tasks
	
	@tot_fin_tasks.setter
	def tot_fin_tasks(self, val):
		self._tot_fin_tasks = val

	def set_workload(self, kernels, monitor=None):

		if type(kernels) != list:
			self._executable_workload = [kernels]
		else:
			self._executable_workload = kernels

		self._logger.info("New workload assigned to plugin for execution")

		self._monitor = monitor
		if self._monitor is not None:
			self._logger.info("Monitor for workload assigned")

	def add_manager(self, manager):
		self._manager = manager
		self._logger.debug("Task execution manager (RP-Unit Manager) assigned to execution plugin")


	def create_tasks(self, record, pattern_name, iteration, stage, instance=None):

		try:

			from staging.input_data import get_input_data
			from staging.output_data import get_output_data

			if len(self._executable_workload) > 1:

				cuds = []

				inst=1

				for kernel in self._executable_workload:

					kernel._bind_to_resource(self._resource)
					rbound_kernel = kernel
					cud = rp.ComputeUnitDescription()
					cud.name = "stage-{0}-task-{1}".format(stage, inst)
					self._logger.debug('Creating task {0} of stage {1}'.format(inst,stage))

					cud.pre_exec       	= rbound_kernel.pre_exec
					cud.executable     	= rbound_kernel.executable
					cud.arguments      	= rbound_kernel.arguments
					cud.mpi            		= rbound_kernel.uses_mpi
					cud.cores 		= rbound_kernel.cores
					cud.input_staging  	= get_input_data(rbound_kernel, record, cur_pat = pattern_name, cur_iter= iteration, cur_stage = stage, cur_task=inst)
					cud.output_staging 	= get_output_data(rbound_kernel, record, cur_pat = pattern_name, cur_iter= iteration, cur_stage = stage, cur_task=inst)

					inst+=1

					cuds.append(cud)
					self._logger.debug("Kernel {0} converted into RP Compute Unit".format(kernel.name))

				return cuds

			else:

				kernel = self._executable_workload[0]

				cur_stage = stage
				cur_task = instance

				if len(self._tot_fin_tasks) < cur_stage:
					self._tot_fin_tasks.append(0)
					self._logger.info('\nStarting submission of stage {0} of all pipelines'.format(cur_stage))				
					
				self._logger.debug('Creating task {0} of stage {1}'.format(cur_task,cur_stage))

				kernel._bind_to_resource(self._resource)
				rbound_kernel = kernel

				cud = rp.ComputeUnitDescription()
				cud.name = "stage-{0}-task-{1}".format(cur_stage,cur_task)

				cud.pre_exec       	= rbound_kernel.pre_exec
				cud.executable     	= rbound_kernel.executable
				cud.arguments      	= rbound_kernel.arguments
				cud.mpi            		= rbound_kernel.uses_mpi
				cud.cores 		= rbound_kernel.cores
				cud.input_staging  	= get_input_data(rbound_kernel, record, cur_pat = pattern_name, cur_iter= iteration, cur_stage = cur_stage, cur_task=cur_task)
				cud.output_staging 	= get_output_data(rbound_kernel, record, cur_pat = pattern_name, cur_iter= iteration, cur_stage = cur_stage, cur_task=cur_task)

				return cud

		except Exception, ex:
			self._logger.error("Task creation failed, error: {0}".format(ex))
			raise


	def execute_tasks(self, tasks):

		try:
			if type(tasks) == list:
				cur_stage = int(tasks[0].name.split('-')[1])
				self._logger.info('Submitting stage {0} of all pipelines'.format(cur_stage))
				exec_cus = self._manager.submit_units(tasks)

			else:
				cur_stage = int(tasks.name.split('-')[1])
				cur_task = int(tasks.name.split('-')[3])
				self._logger.info('Submitting stage {1} of pipeline {0}'.format(cur_task,cur_stage))
				task = self._manager.submit_units(tasks)

		except Exception, ex:
			self._logger.error("Could not execute tasks, error : {1}".format(ex))
			raise


	def execute_monitor(self, record, tasks, cur_pat, cur_iter, cur_stage, cur_task):

		try:

			self._logger.info("Executing monitor for stage {0} of pipe {1}".format(cur_stage, cur_task))
			self._monitor[cur_task-1]._bind_to_resource(self._resource)
			rbound_kernel = self._monitor[cur_task-1]
			cud = rp.ComputeUnitDescription()
			cud.name = "monitor_{0}".format(rbound_kernel.name)

			cud.pre_exec       	= rbound_kernel.pre_exec
			cud.executable     	= rbound_kernel.executable
			cud.arguments      	= rbound_kernel.arguments
			cud.mpi            	= rbound_kernel.uses_mpi
			cud.cores 			= rbound_kernel.cores
			cud.scheduler_hint 	= {'partition': 'monitor'}
			cud.input_staging  	= get_input_data(rbound_kernel, record, cur_pat = cur_pat, cur_iter= cur_iter, cur_stage = cur_stage, cur_task=cur_task, nonfatal=True)
			cud.output_staging 	= get_output_data(rbound_kernel, record, cur_pat = cur_pat, cur_iter= cur_iter, cur_stage = cur_stage, cur_task=cur_task)

			self._logger.debug("Monitor {0} converted into RP Compute Unit".format(cud.name))

			self._logger.debug("Timeout: {0}".format(self._monitor[cur_task-1].timeout))

			monitor_handle = self._manager.submit_units(cud)

			task_list_A = tasks
			task_list_B = tasks

			while len(task_list_A) > 0 :

				task_uids = [cu.uid for cu in task_list_A]
				self._manager.wait_units(task_uids, timeout=self._monitor[cur_task-1].timeout)
				self._logger.debug("Timeout done...")


				if monitor_handle.state == rp.DONE:

					# Check if tasks need to be canceled
					if self._monitor[cur_task-1].cancel_tasks != None:

						# Get uids of tasks to be canceled
						c_tasks=[]
						for ind in self._monitor[cur_task-1].cancel_tasks:
							c_task_uid = record["pat_{0}".format(cur_pat)]["iter_{0}".format(cur_iter)]["stage_{0}".format(cur_stage)]["instance_{0}".format(ind)]["uid"]

							task_obj = self._manager.get_units(c_task_uid)
							if (task_obj.state != rp.DONE)and(task_obj.state != rp.FAILED)and(task_obj.state != rp.CANCELED):
								c_tasks.append(c_task_uid)

						if len(c_tasks)>0:
							self._logger.info("Canceling tasks: {0}".format(c_tasks))
							self._manager.cancel_units(c_tasks)
							self._logger.info("Task canceled: {0}, state: {1}".format(c_tasks,task_obj.state))

					monitor_handle = self._manager.submit_units(cud)
					self._logger.info("Monitor resubmitted")

				for unit in task_list_A:
					if (unit.state == rp.DONE)or(unit.state == rp.FAILED)or(unit.state == rp.CANCELED):
						task_list_B.remove(unit)
				task_list_A = task_list_B
				self._logger.debug("Number of tasks executing: {0}".format(len(task_list_A)))

			# Wait for pending monitor task to finish
			self._logger.debug("Waiting for residual monitor")
			if (monitor_handle.state != rp.DONE)or(monitor_handle.state != rp.FAILED)or(monitor_handle.state != rp.CANCELED):
				self._manager.wait_units(monitor_handle.uid)

			self._logger.debug("Stage {0} execution completed".format(cur_stage))

			return monitor_handle

		except Exception, ex:

			self._logger.error("Monitor execution failed, error: {0}".format(ex))