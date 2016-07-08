#!/usr/bin/env python

"""This module defines and implements the BagofTasks class.
"""

__author__    = "Vivek Balasubramanian <vivek.balasubramanian@rutgers.edu>"
__copyright__ = "Copyright 2016, http://radical.rutgers.edu"
__license__   = "MIT"

from radical.ensemblemd.exceptions import NotImplementedError
from radical.ensemblemd.execution_pattern import ExecutionPattern

PATTERN_NAME = "BagofTasks"


# ------------------------------------------------------------------------------
#
class BagofTasks(ExecutionPattern):
	""" The pipeline pattern.

			.. image:: ../../images/pipeline_pattern.*
			   :width: 300pt

		The following placeholders can be used to reference the data during staging of files
		generated in previous steps and same instance:

		* ``$STEP_X`` - References the step X with the same instance number as the current instance.


	"""

	#---------------------------------------------------------------------------
	#
	def __init__(self, stages=1,instances=1):
		"""Creates a new BagofTasks instance.
		"""
		self._instances = instances
		self._stages = stages

		super(BagofTasks, self).__init__()


	#---------------------------------------------------------------------------
	#
	@property
	def instances(self):
		"""Returns the number of tasks.
		"""
		return self._instances

	#---------------------------------------------------------------------------
	#
	@property
	def stages(self):
		"""Returns the instances of the pipeline.
		"""
		return self._stages

	#---------------------------------------------------------------------------
	#
	@property
	def name(self):
		"""Returns the name of the pattern.
		"""
		return PATTERN_NAME

	