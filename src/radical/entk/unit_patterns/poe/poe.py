__author__    = "Vivek Balasubramanian <vivek.balasubramanian@rutgers.edu>"
__copyright__ = "Copyright 2016, http://radical.rutgers.edu"
__license__   = "MIT"

from radical.entk.exceptions import *
from radical.entk.execution_pattern import ExecutionPattern
import radical.utils as ru
import pprint

class PoE(ExecutionPattern):

	def __init__(self, ensemble_size, pipeline_size, iterations = 1, name=None):

		self._ensemble_size = ensemble_size
		self._pipeline_size = pipeline_size
		self._total_iterations = iterations
		self._name = name

		# Internal parameters
		self._next_stage = 1
		self._cur_iteration = 1
		self._pattern_status = 'New'
		self._stage_change = False
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

		if type(self._total_iterations) != int:
			raise TypeError(expected_type=int, actual_type=type(self._total_iterations))


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
	def pattern_dict(self):
		return self._pattern_dict

	@pattern_dict.setter
	def pattern_dict(self, record):
		self._pattern_dict = record
	
	
	
	def set_next_stage(self, stage):

		try:
			if stage <= self._pipeline_size:
				self._next_stage = stage
				self._stage_change = True
		
		except Exception, ex:
			self._logger.error("Could not set next stage, error: {0}".format(ex))
			raise

	@property
	def stage_change(self):
		return self._stage_change

	@stage_change.setter
	def stage_change(self, value):
		self._stage_change = value
	

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


	def get_output(self, stage, task=None, monitor=None, iteration=None):
		
		try:
			if iteration == None:
				iteration = self._cur_iteration

			if monitor!=None:
				return self._pattern_dict["iter_{0}".format(iteration)]["stage_{0}".format(stage)]["monitor_1"]["output"]

			return self._pattern_dict["iter_{0}".format(iteration)]["stage_{0}".format(stage)]["instance_{0}".format(task)]["output"]

		except Exception, ex:

			if monitor==None:
				self._logger.error("Could not get output of stage: {0}, instance: {1}".format(stage, task))
			else:
				self._logger.error("Could not get output of stage: {0}, monitor".format(stage))

			raise


	def get_file(self, stage, filename, task=None, monitor=None, new_name=None, iteration=None):

		try:
			if iteration == None:
				iteration = self._cur_iteration

			if monitor == None:
				directory = self._pattern_dict["iter_{0}".format(iteration)]["stage_{0}".format(stage)]["instance_{0}".format(task)]["path"]
			else:
				directory = self._pattern_dict["iter_{0}".format(iteration)]["stage_{0}".format(stage)]["monitor_1"]["path"]

			file_url = directory + '/' + filename

			import saga,os
			remote_file = saga.filesystem.Directory(directory, session = self._session_id)

			if new_name is None:
				remote_file.copy(filename, os.getcwd())
			else:
				remote_file.copy(filename, os.getcwd() + '/' + new_name)

		except Exception, ex:

			if monitor==None:
				self._logger.error("Could not get file of stage: {0}, instance: {1}, error: {2}".format(stage, task, ex))
			else:
				self._logger.error("Could not get file of stage: {0}, monitor, error: {1}".format(stage, ex))

			raise
