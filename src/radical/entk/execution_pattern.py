__author__    = "Vivek Balasubramanian <vivek.balasubramanian@rutgers.edu>"
__copyright__ = "Copyright 2016, http://radical.rutgers.edu"
__license__   = "MIT"

from radical.entk.exceptions import *

class ExecutionPattern(object):

	def __init__(self):
		self._name = None
		self._object_list = None

	def name(self):

		raise NotImplementedError(
			method_name="name",
			class_name=type(self)
			)

	def object_list(self):

		raise NotImplementedError(
			method_name="object_list",
			class_name=type(self)
			)

