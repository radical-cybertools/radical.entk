__author__    = "Vivek Balasubramanian <vivek.balasubramanian@rutgers.edu>"
__copyright__ = "Copyright 2016, http://radical.rutgers.edu"
__license__   = "MIT"

from radical.entk.exceptions import *
from radical.entk.execution_pattern import ExecutionPattern
import radical.utils as ru
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
		self._session_id = None

		# Pattern specific dict
		self._pattern_dict = None

		self._logger = ru.get_logger("radical.entk.pattern.poe")
		self._reporter = self._logger.report

		# Perform sanity check -- perform before proceeding
		self.sanity_check()

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
	def session_id(self):
		return self._session_id
	
	@session_id.setter
	def session_id(self, val):
		self._session_id = val

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

	@property
	def pattern_dict(self):
		return self._pattern_dict

	@pattern_dict.setter
	def pattern_dict(self, record):
		self._pattern_dict = record
	
	
	
	def set_next_stage(self, stage):

		try:
			if stage <= self._pipeline_size:
				self._next_stage = stage
				self._state_change = True
		
		except Exception, ex:
			self._logger.error("Could not set next stage, error: {0}".format(ex))
			raise

	@property
	def state_change(self):
		return self._state_change

	@state_change.setter
	def state_change(self, value):
		self._state_change = value
	

	def get_stage(self, stage):

		try:
			stage_kernel = getattr(self, "stage_{0}".format(stage), None)

			if stage_kernel == None:
				self._logger.error("Pattern does not have stage_{0}".format(stage))
				raise

			return stage_kernel

		except Exception, ex:
			self._logger.error("Could not get stage, error: {0}".format(ex))
			raise


	def get_branch(self, stage):

		try:
			branch = getattr(self, "branch_{0}".format(stage), None)

			if branch == None:
				self._logger.error("Pattern does not have branch_{0}".format(stage))
				raise
			
			return branch

		except Exception, ex:

			self._logger.error("Could not get branch, error: {0}".format(ex))
			raise


	def get_monitor(self, stage):

		try:
			monitor = getattr(self, "monitor_{0}".format(stage), None)

			if monitor == None:
				self._logger.error("Pattern does not have monitor_{0}".format(stage))
				raise			
			return monitor

		except Exception, ex:

			self._logger.error("Could not get monitor, error: {0}".format(ex))
			raise


	def get_output(self, stage, instance, iteration=None):
		
		try:
			if iteration == None:
				iteration = self._cur_iteration

			return self._pattern_dict["iter_{0}".format(iteration)]["stage_{0}".format(stage)]["instance_{0}".format(instance)]["output"]

		except Exception, ex:
			self._logger.error("Could not get output of stage: {0}, instance: {1}".format(stage, instance))
			raise

	def get_file(self, stage, instance, filename, new_name=None, iteration=None):

		if iteration == None:
			iteration = self._cur_iteration

		directory = self._pattern_dict["iter_{0}".format(iteration)]["stage_{0}".format(stage)]["instance_{0}".format(instance)]["path"]
		file_url = directory + '/' + filename

		import saga,os
		remote_file = saga.filesystem.Directory(directory, session = self._session_id)
		if new_name is None:
			remote_file.copy(filename, os.getcwd())
		else:
			remote_file.copy(filename, os.getcwd() + '/' + new_name)