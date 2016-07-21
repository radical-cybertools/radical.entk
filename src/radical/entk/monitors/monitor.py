__author__    = "Vivek Balasubramanian <vivek.balasubramanian@rutgers.edu>"
__copyright__ = "Copyright 2016, http://radical.rutgers.edu"
__license__   = "MIT"

from radical.entk.exceptions import ValueError, TypeError


class Monitor(object):

	def __init__(self, timeout=None, name=None, umgr=None):

		# Parameters required for all Monitors

		self._timeout = timeout
		self._umgr = umgr
		self._executable = None	# for now, simply do a os.system() call of this
		self._arguments = None
		self._name = name
		
		if type(self._timeout) != int:
			raise TypeError(expected_type="int", actual_type="{0}".format(type(self._timeout)))
		
		self._cancel_units = None
		self._output_units = None

	# ------------------------------------------------------------- ------------------------------------------------------------------------------
	@property
	def name(self):
		return self._name
	
	# ------------------------------------------------------------- ------------------------------------------------------------------------------
	# Methods to get via API

	
	# ------------------------------------------------------------- ------------------------------------------------------------------------------
	# ------------------------------------------------------------- ------------------------------------------------------------------------------
	# Methods to set monitor parameters via API

	def cancel_units(self, instances):
		self._cancel_units = instances


	@property
	def timeout(self):
		return self._timeout

	@property
	def output_units(self):
		return self._output_units
	

	@output_units.setter
	def output_units(self, instances):

		if type(instances) == list:
			raise TypeError(expected_type="list", actual_type="{0}".format(self.instances))

		self._output_units = instances


	@property
	def executable(self):
		return self._executable
	

	@executable.setter
	def executable(self, command):
		self._executable = command


	@property
	def arguments(self):
		return self._arguments
	

	@arguments.setter
	def arguments(self, args):
		self._arguments = args


