__author__    = "Vivek Balasubramanian <vivek.balasubramanian@rutgers.edu>"
__copyright__ = "Copyright 2016, http://radical.rutgers.edu"
__license__   = "MIT"

from radical.entk.exceptions import *
from radical.entk.execution_pattern import ExecutionPattern
import pprint

class PoE(ExecutionPattern):

	def __init__(self, ensemble_size, pipeline_size, iterations = 1, type='unit', iterative = False, name=None):

		self._ensemble_size = ensemble_size
		self._pipeline_size = pipeline_size
		self._type = type
		self._total_iterations = iterations
		self._iterative = iterative

		if name!=None:
			self._name = name
		else:
			self._name = "None"

		# Internal parameters
		self._next_stage = 1
		self._cur_iteration = 1
		self.kill_instances = None
		self._pattern_status = 'New'
		self._state_change = False

		# Perform sanity check -- perform before proceeding
		self.sanity_check()

		self._kernel_dict = dict()

	def sanity_check(self):

		# Check type errors
		if type(self._pipeline_size) != int:
			raise TypeError(expected_type=int, actual_type=type(self._pipeline_size))

		if ((type(self._ensemble_size) != list) and (type(self._ensemble_size) != int)):
			raise TypeError(expected_type=list, actual_type=type(self._ensemble_size))

		if type(self._type) != str:
			raise TypeError(expected_type=str, actual_type=type(self._type))		

		if type(self._total_iterations) != int:
			raise TypeError(expected_type=int, actual_type=type(self._total_iterations))

		if type(self._iterative) != bool:
			raise TypeError(expected_type=bool, actual_type=type(self._iterative))


		# Check value errors
		if ((self._type != 'unit') and (self._type != 'complex')):
			raise ValueError(expected_value=['unit','complex'], actual_value=self._type)

	@property
	def ensemble_size(self):
		return self._ensemble_size
	
	@property
	def pipeline_size(self):
		return self._pipeline_size

	@property
	def total_iterations(self):
		return self._total_iterations
	

	@property
	def cur_iteration(self):
		return self._cur_iteration

	@cur_iteration.setter
	def cur_iteration(self, cur_iteration):
		self._cur_iteration = cur_iteration

	@property
	def next_stage(self):
		return self._next_stage
	
	@next_stage.setter
	def next_stage(self, next_stage):
		self._next_stage = next_stage
	
	@property
	def type(self):
		return self._type

	@property
	def iterative(self):
		return self._iterative
	
	
	def set_next_stage(self, stage):

		if stage <= self._pipeline_size:
			self._next_stage = stage
			self._state_change = True
		else:
			print 'Check next stage value'
			raise

	@property
	def state_change(self):
		return self._state_change

	@state_change.setter
	def state_change(self, value):
		self._state_change = value
	

	def get_stage(self, stage):

		stage_kernel = getattr(self, "stage_{0}".format(stage), None)

		if stage_kernel == None:
			raise Exception("Pattern does not have stage_{0}".format(stage))

		'''
		if instance == None:

			kernel_list = list()

			# Create instance key/vals for each stage
			if type(self._ensemble_size) == int:
				instances = self._ensemble_size
			elif type(self._ensemble_size) == list:
				instances = self._ensemble_size[stage-1]


			for inst in range(1, instances):

				kernel_list.append(stage_kernel(inst))

		else:

			return stage_kernel(instance)
		'''

		return stage_kernel


	def get_branch(self, stage):

		branch = getattr(self, "branch_{0}".format(stage), None)

		if branch == None:
			raise Exception("Pattern does not have branch_{0}".format(stage))

		return branch

	def get_output(self, iteration, stage, instance):

		return self._kernel_dict["iter_{0}".format(iteration)]["stage_{0}".format(stage)]["instance_{0}".format(instance)]["output"]