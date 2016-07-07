__author__    = "Vivek Balasubramanian <vivek.balasubramanian@rutgers.edu>"
__copyright__ = "Copyright 2016, http://radical.rutgers.edu"
__license__   = "MIT"

from radical.entk.exceptions import *
from radical.entk.kernel_plugins.kernel_base import KernelBase
from radical.entk.execution_pattern import ExecutionPattern
from radical.entk.unit_patterns.poe.poe import PoE

import sys

class AppManager():

	def __init__(self, name=None):

		self._name = name

		self._pattern = None
		self._loaded_kernels = list()
		self._loaded_plugins = list() 

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
		if type(kernel_class) == list:
			for item in kernel_class:
				if not hasattr(item, '__base__'):
					raise TypeError(expected_type="KernelBase", actual_type = type(item))					
				elif item.__base__ != Kernel:
					raise TypeError(expected_type="KernelBase", actual_type = type(item()))		

				if item in self._loaded_kernels:
					raise ExistsError(item='{0}'.format(item().name), parent = 'loaded_kernels')

				self._loaded_kernels.append(item)

		elif not hasattr(kernel_class,'__base__'):
			raise TypeError(expected_type="KernelBase", actual_type = type(kernel_class))

		elif kernel_class.__base__ != KernelBase:
			raise TypeError(expected_type="KernelBase", actual_type = type(kernel_class()))

		else:
			self._loaded_kernels.append(kernel_class)
		


	def list_kernels(self):

		registered_kernels = list()
		for item in self._loaded_kernels:
			registered_kernels.append(item().name)

		return registered_kernels


	def save(self, pattern):
		self._pattern = pattern
		self.sanity_pattern_check()

		# Convert pattern to JSON
		self.pattern_to_json(pattern)


	def pattern_to_json(self, pattern):
		pass


	def validate_kernel(self, user_kernel):

		for kernel in self._loaded_kernels:

			if kernel().name == user_kernel.name:

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

				return new_kernel

			else:

				raise Exception("Kernel {0} does not exist".format(user_kernel.name))


	def add_workload(self, pattern):
		self._pattern = pattern

	def run(self, resource, task_manager):

		# Create dictionary for logging
		self._pattern.create_record()

		if self._pattern.__class__.__base__ == PoE:
	
			# Based on the execution pattern, the app manager should choose the execution plugin
			from radical.entk.execution_plugin.poe import PluginPoE

			print 'app-1'
			plugin = PluginPoE()
			print 'app-2'
			plugin.register_resource(resource = resource)
			print 'app-3'
			plugin.add_manager(task_manager)
			print 'app-4'

			# Submit kernels stage by stage to execution plugin
			while((self._pattern.iterative==True)or(self._pattern.cur_iteration <= self._pattern.total_iterations)):
			
				for self._pattern.next_stage in range(1, self._pattern.pipeline_size+1):


					# Get kernel from execution pattern
					stage = self._pattern.get_stage(stage=self._pattern.next_stage)
					list_kernels_stage = list()

					print 'app-5'

					# Validate user specified Kernel with KernelBase and return fully defined but resource-unbound kernel
					# Create instance key/vals for each stage
					if type(self._pattern.ensemble_size) == int:
						instances = self._pattern.ensemble_size
					elif type(self._pattern.ensemble_size) == list:
						instances = self._pattern.ensemble_size[self._pattern.next_stage-1]

					print 'app-6'

					for inst in range(1, instances+1):
						list_kernels_stage.append(self.validate_kernel(stage(inst)))

					print 'app-7'

					# Pass resource-unbound kernels to execution plugin
					#print len(list_kernels_stage)
					plugin.set_workload(kernels=list_kernels_stage)
					print 'app-8'
					plugin.execute()
					print 'app-9'

					# Execute branch if it exists
					if (self._pattern.get_record()["iter_{0}".format(self._pattern.cur_iteration)]["stage_{0}".format(self._pattern.next_stage)]["branch"]):
						self._pattern.get_branch(stage=self._pattern.next_stage)()

					# Terminate execution
					if self._pattern.next_stage == 0:
						break

				# Terminate execution
				if self._pattern.next_stage == 0:
					break

				self._pattern.cur_iteration+=1
