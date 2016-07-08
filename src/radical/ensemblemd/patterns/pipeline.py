#!/usr/bin/env python

"""This module defines and implements the Pipeline class.
"""

__author__    = "Vivek Balasubramnian <vivek.balasubramanian@rutgers.edu>"
__copyright__ = "Copyright 2015, http://radical.rutgers.edu"
__license__   = "MIT"

from radical.ensemblemd.exceptions import NotImplementedError
from radical.ensemblemd.execution_pattern import ExecutionPattern

PATTERN_NAME = "Pipeline"


# ------------------------------------------------------------------------------
#
class Pipeline(ExecutionPattern):

	#---------------------------------------------------------------------------
	#
	def __init__(self, stages=1,tasks=1):
		"""Creates a new MTMS instance.
		"""
		self._tasks = tasks
		self._stages = stages

		super(Pipeline, self).__init__()


	#---------------------------------------------------------------------------
	#
	@property
	def tasks(self):
		"""Returns the number of tasks in MTMS.
		"""
		return self._tasks

	#---------------------------------------------------------------------------
	#
	@property
	def stages(self):
		"""Returns the number of stages in each task of the MTMS.
		"""
		return self._stages

	#---------------------------------------------------------------------------
	#
	@property
	def name(self):
		"""Returns the name of the pattern.
		"""
		return PATTERN_NAME
