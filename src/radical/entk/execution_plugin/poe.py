__author__    = "Vivek Balasubramanian <vivek.balasubramanian@rutgers.edu>"
__copyright__ = "Copyright 2016, http://radical.rutgers.edu"
__license__   = "MIT"


from plugin_base import PluginBase
import radical.pilot as rp
import radical.utils as ru

from staging.input_data import get_input_data
from staging.output_data import get_output_data

import saga
import sys

_plugin_info = {
			'name': 'poe',
			'type': 'static'
		}

class PluginPoE(object):

	def __init__(self):

		self._executable_workload = list()
		self._resource = None
		self._manager = None
		self._monitor = None

		self._logger = ru.get_logger("radical.entk.plugin.poe")
		self._reporter = self._logger.report

		self._logger.info("Plugin PoE created")


	@property
	def monitor(self):
		return self._monitor
	
	@monitor.setter
	def monitor(self, val):
		self._monitor = val

	def register_resource(self, resource):
		self._resource = resource
		self._logger.info("Registered resource {0} with execution plugin".format(resource))

	def get_resources(self):
		return self._resource


	def set_workload(self, kernels, monitor=None):

		if type(kernels) != list:
			self._executable_workload = [kernels]
		else:
			self._executable_workload = kernels

		self._logger.info("New workload assigned to plugin for execution")

		self._monitor = monitor

		if self._monitor!=None:
			self._logger.info("Monitor for workload assigned")

	def add_workload(self, kernels):

		if type(kernels) != list:
			self._executable_workload.append(kernels)
		else:
			self._executable_workload.extend(kernels)

		self._logger.info("New workload added to plugin for execution")

	def add_manager(self, manager):
		self._manager = manager
		self._logger.debug("Task execution manager (RP-Unit Manager) assigned to execution plugin")


	def execute_monitor(self, record, tasks, cur_pat, cur_iter, cur_stage):

		try:

			self._logger.info("Executing monitor...")
			self._monitor._bind_to_resource(self._resource)
			rbound_kernel = self._monitor
			cud = rp.ComputeUnitDescription()
			cud.name = "monitor_{0}".format(rbound_kernel.name)

			cud.pre_exec       	= rbound_kernel.pre_exec
			cud.executable     	= rbound_kernel.executable
			cud.arguments      	= rbound_kernel.arguments
			cud.mpi            	= rbound_kernel.uses_mpi
			cud.cores 			= rbound_kernel.cores
			cud.scheduler_hint 	= {'partition': 'monitor'}
			cud.input_staging  	= get_input_data(rbound_kernel, record, cur_pat = cur_pat, cur_iter= cur_iter, cur_stage = cur_stage, cur_task=-1*cur_stage, nonfatal=True)
			cud.output_staging 	= get_output_data(rbound_kernel, record, cur_pat = cur_pat, cur_iter= cur_iter, cur_stage = cur_stage, cur_task=-1*cur_stage)

			self._logger.debug("Monitor {0} converted into RP Compute Unit".format(cud.name))

			self._logger.debug("Timeout: {0}".format(self._monitor.timeout))

			monitor_handle = self._manager.submit_units(cud)

			task_list_A = tasks
			task_list_B = tasks

			while len(task_list_A) > 0 :

				task_uids = [cu.uid for cu in task_list_A]
				self._manager.wait_units(task_uids, timeout=self._monitor.timeout)
				self._logger.debug("Timeout done...")


				if monitor_handle.state == rp.DONE:

					# Check if tasks need to be canceled
					if self._monitor.cancel_tasks != None:

						# Get uids of tasks to be canceled
						c_tasks=[]
						for ind in self._monitor.cancel_tasks:
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



	def execute(self, record, pattern_name, iteration, stage):

		def unit_state_cb (unit, state) :

			if state == rp.FAILED:
				self._logger.error("Task with ID {0} failed: STDERR: {1}, STDOUT: {2} LAST LOG: {3}".format(unit.uid, unit.stderr, unit.stdout, unit.log[-1]))
				self._logger.error("Pattern execution FAILED.")
				sys.exit(1)
		
		try:
			self._manager.register_callback(unit_state_cb)

			cus = []

			inst=1

			for kernel in self._executable_workload:

				kernel._bind_to_resource(self._resource)
				rbound_kernel = kernel
				cud = rp.ComputeUnitDescription()
				cud.name = "stage_{0}".format(kernel.name)

				cud.pre_exec       	= rbound_kernel.pre_exec
				cud.executable     	= rbound_kernel.executable
				cud.arguments      	= rbound_kernel.arguments
				cud.mpi            	= rbound_kernel.uses_mpi
				cud.cores 			= rbound_kernel.cores
				cud.input_staging  	= get_input_data(rbound_kernel, record, cur_pat = pattern_name, cur_iter= iteration, cur_stage = stage, cur_task=inst)
				cud.output_staging 	= get_output_data(rbound_kernel, record, cur_pat = pattern_name, cur_iter= iteration, cur_stage = stage, cur_task=inst)

				inst+=1

				cus.append(cud)
				self._logger.debug("Kernel {0} converted into RP Compute Unit".format(kernel.name))

			exec_cus = self._manager.submit_units(cus)

			copy_exec_cus_A = exec_cus
			copy_exec_cus_B = exec_cus

			if pattern_name == "None":
				self._logger.info("Submitted {0} tasks with kernel:{1} of iteration:{2}, stage:{3}".format(inst-1, rbound_kernel.name, iteration, stage))
			else:
				self._logger.info("Pattern {4}: Submitted {0} tasks with kernel:{1} of iteration:{2}, stage:{3}".format(inst-1, rbound_kernel.name, iteration, stage, pattern_name))

			
			# If there is no monitor, go ahead and wait for tasks to finish
			if self._monitor == None:
				exec_uids = [cu.uid for cu in exec_cus]

				self._logger.info("Waiting for completion of workload")
				self._manager.wait_units(exec_uids)
				self._logger.info("Workload execution successful")
				self._logger.debug("Stage {0} execution completed".format(stage))
			
			else:

				self._logger.debug("Waiting for unit directories to get created")
				exec_uids = [cu.uid for cu in exec_cus]
				self._manager.wait_units(exec_uids, state=rp.AGENT_STAGING_INPUT_PENDING)
				self._logger.debug("Unit directories created")

				for unit in exec_cus:
					self._logger.debug('path from plugin: {0}'.format(unit.working_directory))

			return exec_cus

		except Exception, ex:

			self._logger.error('Task execution failed, error: {0}'.format(ex))
