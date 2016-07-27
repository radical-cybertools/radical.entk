__author__    = "Vivek Balasubramanian <vivek.balasubramanian@rutgers.edu>"
__copyright__ = "Copyright 2016, http://radical.rutgers.edu"
__license__   = "MIT"


from plugin_base import PluginBase
import radical.pilot as rp
import radical.utils as ru

import saga

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
		if monitor is not None:
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


	def execute_monitor(self,record, cur_pat, cur_iter, cur_stage, cur_task):
		self._logger.info("Executing monitor...")
		if self._monitor.download_input_data is not None:
			input_data_list = get_input_data(self._monitor, record=record, cur_pat = cur_pat, cur_iter= cur_iter, cur_stage = cur_stage, cur_task=cur_task)

			remote_dir = saga.filesystem.Directory()



	def execute(self, record, pattern_name, iteration, stage):

		def unit_state_cb (unit, state) :

			if state == rp.FAILED:
				self._logger.error("Task with ID {0} failed: STDERR: {1}, STDOUT: {2} LAST LOG: {3}".format(unit.uid, unit.stderr, unit.stdout, unit.log[-1]))
				self._logger.error("Pattern execution FAILED.")
				sys.exit(1)
		
		try:
			self._manager.register_callback(unit_state_cb)

			from staging.input_data import get_input_data
			from staging.output_data import get_output_data

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
				cud.mpi            		= rbound_kernel.uses_mpi
				cud.cores 		= rbound_kernel.cores
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

			
			self._logger.info("Waiting for completion of workload")

			if self._monitor is not None:
				while len(copy_exec_cus_A) > 0 :
					
					print len(copy_exec_cus_A)
					exec_uids = [cu.uid for cu in copy_exec_cus_A]
					self._manager.wait_units(exec_uids, timeout=self._monitor.timeout)
					for unit in copy_exec_cus_A:
						if (unit.state == rp.DONE)or(unit.state == rp.FAILED)or(unit.state == rp.CANCELED):
							copy_exec_cus_B.remove(unit)
					copy_exec_cus_A = copy_exec_cus_B

					if len(copy_exec_cus_A) > 0:
						cancel_units = self.execute_monitor(record=record, cur_pat = pattern_name, cur_iter= iteration, cur_stage = stage, cur_task=inst)
					
			else:
				exec_uids = [cu.uid for cu in exec_cus]
				self._manager.wait_units(exec_uids)

			for unit in exec_cus:
				if unit.state != rp.DONE:
					self._logger.error("task {1} failed with an error: {0}\n".format(unit.stderr, unit.uid))

			self._logger.info("Workload execution successful")

			return exec_cus

		except Exception, ex:

			self._logger.error('Execution plugin failed: {0}'.format(ex))
