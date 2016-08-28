#!/usr/bin/env python

"""Defines and implements the abstract kernel base class.
"""

__author__    = "Vivek Balasubramanian <vivek.balasubramanian@rutgers.edu>"
__copyright__ = "Copyright 2016, http://radical.rutgers.edu"
__license__   = "MIT"

from copy import deepcopy

from radical.entk.exceptions import *
import radical.utils as ru
import gc

# ------------------------------------------------------------------------------ --------------------------------------------------------
# Kernel format
"""
_KERNEL_INFO = {
	"name":         "kernel name",
	"description":  "Description about kernel",
	"arguments":   {
				"--arg1=":
				{
					"mandatory": False,
					"description": "argument description"
				}
			},
	"machine_configs": 
			{
				"resource_name": {
					"pre_exec"      : [],
					"executable"    : "",
					"uses_mpi"      : False
				},
			}
	}
"""
#  ------------------------------------------------------------- ------------------------------------------------------------------------------

#  ------------------------------------------------------------- ------------------------------------------------------------------------------
# plugin base class
#
class KernelBase(object):

	#  ------------------------------------------------------------- ------------------------------------------------------------------------------
	
	def __init__ (self, kernel_info) :

		self._kernel_info     = kernel_info
		self._kernel_name     = kernel_info['name']

		if 'description' in kernel_info:
			self._kernel_description  = kernel_info['description']

		self._raw_args = kernel_info["arguments"]
		self._args = []

		# Parameters required for any Kernel irrespective of RP
		self._pre_exec               	= None
		self._executable 	= None
		self._arguments       	= None
		self._uses_mpi               = None
		self._cores                  	= 1 # If unspecified, number of cores is set to 1

		self._timeout = None
		self._cancel_tasks = None

		self._upload_input_data      	= []
		self._link_input_data        	= []
		self._download_input_data    	= []
		self._download_output_data   	= []
		self._copy_input_data        	= []
		self._copy_output_data       	= []

		self._logger = ru.get_logger("radical.entk.kernel_base.{0}".format(self._kernel_name))
		self._logger.debug("Kernel instantiated")
	#  ------------------------------------------------------------- ------------------------------------------------------------------------------
	
	def as_dict(self):
		"""Returns a dictionary representation of the kernel"""
	
		kernel_dict = {	"name": 	self._kernel_name,
				"pre_exec": 	self._pre_exec,
			 	"executable": 	self._executable,
			 	"arguments": 	self._arguments,
			 	"uses_mpi": 	self._uses_mpi,
			 	"cores": 	self._cores
			 	}

		return kernel_dict
	# ------------------------------------------------------------- ------------------------------------------------------------------------------
	
	@property
	def name(self):
		return self._kernel_name
	
	def get_name(self):
		return self._kernel_name
	# ------------------------------------------------------------- ------------------------------------------------------------------------------

	@property
	def kernel_info(self):
		return self._kernel_info
	
	def get_kernel_info (self) :
		return self._kernel_info
	# ------------------------------------------------------------- ------------------------------------------------------------------------------
	
	def get_arg(self, arg_name):
		"""Returns the value of the argument given by 'arg_name'.
		"""
		return self._args[arg_name]["_value"]
	# ------------------------------------------------------------- ------------------------------------------------------------------------------
	
	def get_raw_args(self):
		"""Returns all arguments as they were passed to the kernel.
		"""
		return self._raw_args
	# ------------------------------------------------------------- ------------------------------------------------------------------------------

	def validate_arguments(self):

		arg_details = dict()

		try:

			for arg_name, arg_info in self._raw_args.iteritems():
				self._raw_args[arg_name]["_is_set"] = False
				self._raw_args[arg_name]["_value"] = None

			for arg in self._arguments:
				arg_found = False
				for arg_name, arg_info in self._raw_args.iteritems():
					if arg.startswith(arg_name):
						arg_found = True
						self._raw_args[arg_name]["_is_set"] = True
						self._raw_args[arg_name]["_value"] = arg.replace(arg_name,'')

				if arg_found == False:
					raise ArgumentError(
                    					kernel_name=self._kernel_name,
                    					message="Unknown / malformed argument '{0}'".format(arg),
                    					valid_arguments_set=self._raw_args)

			for arg_name, arg_info in self._raw_args.iteritems():
				if ((arg_info["mandatory"] == True) and (arg_info["_is_set"] == False)):
					raise ArgumentError(
                    					kernel_name=self._kernel_name,
                    					message="Mandatory argument '{0}' missing".format(arg_name),
                    					valid_arguments_set=self._raw_args)

			self._args = self._raw_args
			self._logger.debug("Arguments validated for kernel {0}".format(self._kernel_name))

		except Exception, ex:
			self._logger.error('Kernel argument validation failed: {0}'.format(ex))
			raise

	# ------------------------------------------------------------- ------------------------------------------------------------------------------

	def _bind_to_resource(self, resource_key, pattern_name=None):
		"""Binds the kernel to a specific resource.
		"""
		raise NotImplementedError(
		  method_name="_get_kernel_description",
		  class_name=type(self))
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