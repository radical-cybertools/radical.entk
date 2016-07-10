__author__    = "Vivek Balasubramanian <vivek.balasubramanian@rutgers.edu>"
__copyright__ = "Copyright 2016, http://radical.rutgers.edu"
__license__   = "MIT"


from plugin_base import PluginBase
import radical.pilot as rp
import radical.utils as ru
_plugin_info = {
			'name': 'poe',
			'type': 'static'
		}

class PluginPoE(object):

	def __init__(self):

		self._executable_workload = list()
		self._resource = None
		self._manager = None

		self._logger = ru.get_logger("radical.entk.plugin.poe")
		self._reporter = self._logger.report

		self._logger.info("Plugin PoE created")


	def register_resource(self, resource):
		self._resource = resource
		self._logger.info("Registered resource {0} with execution plugin".format(resource))

	def get_resources(self):
		return self._resource


	def set_workload(self, kernels):

		if type(kernels) != list:
			self._executable_workload = [kernels]
		else:
			self._executable_workload = kernels

		self._logger.info("New workload assigned to plugin for execution")

	def add_workload(self, kernels):

		if type(kernels) != list:
			self._executable_workload.append(kernels)
		else:
			self._executable_workload.extend(kernels)

		self._logger.info("New workload added to plugin for execution")

	def add_manager(self, manager):
		self._manager = manager
		self._logger.info("Task execution manager (RP-Unit Manager) assigned to execution plugin")

	def execute(self, record, iteration, stage):

		def unit_state_cb (unit, state) :

			if state == rp.FAILED:
				self._logger.error("Task with ID {0} failed: STDERR: {1}, STDOUT: {2} LAST LOG: {3}".format(unit.uid, unit.stderr, unit.stdout, unit.log[-1]))
				self._logger.error("Pattern execution FAILED.")
				sys.exit(1)
		
		try:
			self._manager.register_callback(unit_state_cb)

			from staging.input_data import get_input_data
			from staging.input_data import get_output_data

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
				cud.input_staging  	= get_input_data(rbound_kernel, record, cur_iter= iteration, cur_stage = stage, cur_task=inst)
				cud.output_staging 	= get_output_data(rbound_kernel, record, cur_iter= iteration, cur_stage = stage, cur_task=inst)

				inst+=1

				cus.append(cud)
				self._logger.debug("Kernel {0} converted into RP Compute Unit".format(kernel.name))

			exec_cus = self._manager.submit_units(cus)
			self._logger.info("Workload submitted for execution on resource")

			exec_uids = [cu.uid for cu in exec_cus]
			self._logger.info("Waiting for completion of workload execution")
			self._manager.wait_units(exec_uids)

			for unit in exec_cus:
				if unit.state != rp.DONE:
					self._logger.error("task {1} failed with an error: {0}\n".format(unit.stderr, unit.uid))

			self._logger.info("Workload execution successful")

			return exec_cus

		except Exception, ex:

			self._logger.error('Execution plugin failed: {0}'.format(ex))
