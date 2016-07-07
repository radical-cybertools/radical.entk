__author__    = "Vivek Balasubramanian <vivek.balasubramanian@rutgers.edu>"
__copyright__ = "Copyright 2016, http://radical.rutgers.edu"
__license__   = "MIT"


from plugin_base import PluginBase
import radical.pilot as rp

_plugin_info = {
			'name': 'poe',
			'type': 'static'
		}

class PluginPoE(object):

	def __init__(self):

		self._executable_workload = list()
		self._resource = None
		self._manager = None


	def register_resource(self, resource):
		self._resource = resource

	def get_resources(self):
		return self._resource


	def set_workload(self, kernels):

		if type(kernels) != list:
			self._executable_workload = [kernels]
		else:
			self._executable_workload = kernels

	def add_workload(self, kernels):

		if type(kernels) != list:
			self._executable_workload.append(kernels)
		else:
			self._executable_workload.extend(kernels)

	def add_manager(self, manager):
		self._manager = manager
		print 'added managervi'

	def execute(self):

		def unit_state_cb (unit, state) :

			if state == radical.pilot.FAILED:
				self.get_logger().error("Task with ID {0} failed: STDERR: {1}, STDOUT: {2} LAST LOG: {3}".format(unit.uid, unit.stderr, unit.stdout, unit.log[-1]))
				self.get_logger().error("Pattern execution FAILED.")
				sys.exit(1)
		
		self._manager.register_callback(unit_state_cb)

		print 'entered executer'

		submit_cus = []

		for kernel in self._executable_workload:

			rbound_kernel = kernel._bind_to_resource(self._resource)
			cud = rp.ComputeUnitDescription()
			cud.name = "stage_{0}".format(kernel.name)

			cud.pre_exec       	= rbound_kernel.pre_exec
			cud.executable     	= rbound_kernel.executable
			cud.arguments      	= rbound_kernel.arguments
			cud.mpi            		= rbound_kernel.uses_mpi
			cud.input_staging  	= None
			cud.output_staging 	= None

			submit_cus.append(cud)

		exec_cus = self._manager.submit_units(submit_cus)

		exec_uids = [cu.uid for cu in exec_cus]
		self._manager.wait_units(exec_uids)

		for unit in exec_cus:
			if unit.state != rp.DONE:
				print " * task failed with an error: {0}\n".format(unit.stderr)
			elif unit.state == rp.DONE:
				print " task {0} done".format(unit.uid)

