__author__    = "Vivek Balasubramanian <vivek.balasubramanian@rutgers.edu>"
__copyright__ = "Copyright 2016, http://radical.rutgers.edu"
__license__   = "MIT"

import radical.utils as ru
from radical.entk.exceptions import *

class Kernel(object):

	def __init__(self, name=None, ktype=None):

		self._name = name

		# Parameters required for any Kernel irrespective of RP
		self._pre_exec               	= None
		self._executable 				= None
		self._arguments       			= None
		self._uses_mpi               	= None
		self._cores                  	= 1 # If unspecified, number of cores is set to 1
		self._type						= ktype

		# Parameters specific to Monitor
		self._timeout = None
		self._cancel_tasks = None

		self._upload_input_data      	= None
		self._link_input_data        	= None
		self._download_input_data    	= None
		self._download_output_data   	= None
		self._copy_input_data        	= None
		self._copy_output_data       	= None

		self._logger = ru.get_logger("radical.entk.Kernel")


	#  ------------------------------------------------------------- ------------------------------------------------------------------------------
	
	def as_dict(self):
		"""Returns a dictionary representation of the kernel"""
	
		kernel_dict = {	"pre_exec": 	self._pre_exec,
			 	"executable": 	self._executable,
			 	"arguments": 	self._arguments,
			 	"uses_mpi": 	self._uses_mpi,
			 	"cores": 	self._cores
			 	}

		return kernel_dict


	# ------------------------------------------------------------- ------------------------------------------------------------------------------
	@property
	def name(self):
		return self._name
	
	def get_name():
		return self._name

	# ------------------------------------------------------------- ------------------------------------------------------------------------------
	# Methods to get via API

	
	# ------------------------------------------------------------- ------------------------------------------------------------------------------
	# ------------------------------------------------------------- ------------------------------------------------------------------------------
	# Methods to set kernel parameters via API

	# executable
	# pre_exec
	# uses_mpi
	# arguments
	# cores
	# upload_input_data
	# link_input_data
	# copy_input_data
	# download_output_data
	# copy_output_data
	
	#timeout

	# ------------------------------------------------------------- ------------------------------------------------------------------------------
	
	@property
	def executable(self):
		return self._executable

	@executable.setter
	def executable(self, executable):
		if type(executable) != str:
			raise TypeError(
				expected_type=bool,
				actual_type=type(executable))

		self._executable = executable
	# ------------------------------------------------------------- ------------------------------------------------------------------------------

	@property
	def pre_exec(self):
		return self._pre_exec

	@pre_exec.setter
	def pre_exec(self, pre_exec):

		if type(pre_exec) != list:
			raise TypeError(
				expected_type=bool,
				actual_type=type(pre_exec))

		self._pre_exec = pre_exec
	# ------------------------------------------------------------- ------------------------------------------------------------------------------

	@property
	def uses_mpi(self):
		return self._uses_mpi

	@uses_mpi.setter
	def uses_mpi(self, uses_mpi):

		if type(uses_mpi) != bool:
			raise TypeError(
				expected_type=bool,
				actual_type=type(uses_mpi))

		# Call the validate_args() method of the plug-in.
		self._uses_mpi = uses_mpi
	# ------------------------------------------------------------- ------------------------------------------------------------------------------

	@property
	def arguments(self):
		"""List of arguments to the kernel as defined by the kernel definition files"""
		return self._arguments

	@arguments.setter
	def arguments(self, args):
		"""Sets the arguments for the kernel.
		"""
		if type(args) != list:
			raise TypeError(
				expected_type=list,
				actual_type=type(args))

		self._arguments = args
	# ------------------------------------------------------------- ------------------------------------------------------------------------------

	@property
	def cores(self):
		"""The number of cores the kernel is using.
		"""
		return self._cores

	@cores.setter
	def cores(self, cores):

		if type(cores) != int:
			raise TypeError(
				expected_type=int,
				actual_type=type(cores))

		self._cores = cores
	# ------------------------------------------------------------- ------------------------------------------------------------------------------

	@property
	def upload_input_data(self):
		return self._upload_input_data

	@upload_input_data.setter
	def upload_input_data(self, data_directives):
		if type(data_directives) != list:
			data_directives = [data_directives]

		for dd in data_directives:
			if type(dd) != str:
				raise TypeError(
					expected_type=str,
					actual_type=type(dd))

		self._upload_input_data = data_directives
	# ------------------------------------------------------------- ------------------------------------------------------------------------------

	@property
	def link_input_data(self):
		return self._link_input_data

	@link_input_data.setter
	def link_input_data(self, data_directives):
		if type(data_directives) != list:
			data_directives = [data_directives]

		for dd in data_directives:
			if type(dd) != str:
				raise TypeError(
					expected_type=str,
					actual_type=type(dd))

		self._link_input_data = data_directives
	# ------------------------------------------------------------- ------------------------------------------------------------------------------

	@property
	def copy_input_data(self):
		return self._copy_input_data

	@copy_input_data.setter
	def copy_input_data(self, data_directives):

		if type(data_directives) != list:
			data_directives = [data_directives]

		for dd in data_directives:
			dd = str(dd)
			if type(dd) != str:
				raise TypeError(
					expected_type=str,
					actual_type=type(dd))

		self._copy_input_data = data_directives
	# ------------------------------------------------------------- ------------------------------------------------------------------------------

	@property
	def download_output_data(self):
		return self._download_output_data

	@download_output_data.setter
	def download_output_data(self, data_directives):

		if type(data_directives) != list:
			data_directives = [data_directives]

		for dd in data_directives:
			if type(dd) != str:
				raise TypeError(
					expected_type=str,
					actual_type=type(dd))

		self._download_output_data = data_directives
	# ------------------------------------------------------------- ------------------------------------------------------------------------------

	@property
	def copy_output_data(self):
		return self._copy_output_data

	@copy_output_data.setter
	def copy_output_data(self, data_directives):

		if type(data_directives) != list:
			data_directives = [data_directives]

		for dd in data_directives:
			if type(dd) != str:
				raise TypeError(
					expected_type=str,
					actual_type=type(dd))

		self._copy_output_data = data_directives
	# ------------------------------------------------------------- ------------------------------------------------------------------------------

	# ------------------------------------------------------------- ------------------------------------------------------------------------------

	@property
	def timeout(self):
		return self._timeout

	@timeout.setter
	def timeout(self, val):

		self._timeout = val
	# ------------------------------------------------------------- ------------------------------------------------------------------------------


	# ------------------------------------------------------------- --------------------------------

	@property
	def cancel_tasks(self):
		return self._cancel_tasks

	@cancel_tasks.setter
	def cancel_tasks(self, val):

		if type(val) == list:
			self._cancel_tasks = val
		else:
			raise TypeError(expected_type=list, actual_type=type(val))
	# ------------------------------------------------------------- --------------------------------