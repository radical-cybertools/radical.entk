#!/usr/bin/env python

'''Kernel that prints a random value'''

__author__    = "Vivek <vivek.balasubramanian@rutgers.edu>"
__copyright__ = "Copyright 2014, http://radical.rutgers.edu"
__license__   = "MIT"

from copy import deepcopy

from radical.entk import NoKernelConfigurationError
from radical.entk import KernelBase

# ------------------------------------------------------------------------------
# 
_KERNEL_INFO = {
	"name":         "randval",
	"description":  "Creates a new file containing a single ramdom integer value between 0 and 'upperlimit'.",
	"arguments":   {	
				"--upperlimit=":     
						{
						"mandatory": True,
						"description": "The upper limit of the random value."
						},
					
			},
	"machine_configs": 
	{
		"*": {
			"environment"   : None,
			"pre_exec"      : None,
			"executable"    : "/bin/bash",
			"uses_mpi"      : False
		}
	}
}


# ------------------------------------------------------------------------------
# 
class rand_kernel(KernelBase):

	# --------------------------------------------------------------------------
	#
	def __init__(self):
		"""Le constructor.
		"""
		super(rand_kernel, self).__init__(_KERNEL_INFO)

	# --------------------------------------------------------------------------
	#
	def _bind_to_resource(self, resource_key):
		"""(PRIVATE) Implements parent class method. 
		"""
		if resource_key not in _KERNEL_INFO["machine_configs"]:
			if "*" in _KERNEL_INFO["machine_configs"]:
				# Fall-back to generic resource key
				resource_key = "*"
			else:
				raise NoKernelConfigurationError(kernel_name=_KERNEL_INFO["name"], resource_key=resource_key)

		cfg = _KERNEL_INFO["machine_configs"][resource_key]

		arguments  = ["-c","'echo $[ 1 + $[ RANDOM % {0} ]]'".format(self.get_arg("--upperlimit="))]

		self._executable  = cfg["executable"]
		self._arguments   = arguments
		self._environment = cfg["environment"]
		self._uses_mpi    = cfg["uses_mpi"]
		self._pre_exec    = None
