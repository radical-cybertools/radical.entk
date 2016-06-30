__author__    = "Vivek Balasubramanian <vivek.balasubramanian@rutgers.edu>"
__copyright__ = "Copyright 2016, http://radical.rutgers.edu"
__license__   = "MIT"

from radical.entk.exceptions import *
from radical.entk.execution_pattern import ExecutionPattern

class EoP(ExecutionPattern):

	def __init__(self, ensemble_size, pipeline_size, type='unit', iteration = False):

		self._ensemble_size = ensemble_size
		self._pipeline_size = pipeline_size
		self._type = type
		self._iteration = iteration

		# Perform sanity check -- perform before proceeding
		self.sanity_check()


	def sanity_check(self):

		# Check type errors
		if type(self._ensemble_size) != int:
			raise TypeError(expected_type=int, actual_type=type(self._ensemble_size))

		if type(self._pipeline_size) != int:
			raise TypeError(expected_type=int, actual_type=type(self._pipeline_size))
		elif ((type(self._pipeline_size) != list) and (type(self._pipeline_size) != int)):
			raise TypeError(expected_type=list, actual_type=type(self._pipeline_size))

		if type(self._type) != str:
			raise TypeError(expected_type=str, actual_type=type(self._type))		

		if type(self._iteration) != bool:
			raise TypeError(expected_type=bool, actual_type=type(self._iteration))


		# Check value errors
		if ((self._type != 'unit') and (self._type != 'complex')):
			raise ValueError(expected_value=['unit','complex'], actual_value=self._type)

		# Check match errors


	@property
	def ensemble_size(self):
		return self._ensemble_size
	
	@property
	def pipeline_size(self):
		return self._pipeline_size

	@property
	def iteration(self):
		return self._iteration
	
	@property
	def type(self):
		return self._type
	