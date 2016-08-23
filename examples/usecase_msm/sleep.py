#!/usr/bin/env python

"""A kernel that writes Hello World to a file.
"""

__author__    = "Vivek <vivek.balasubramanian@rutgers.edu>"
__copyright__ = "Copyright 2014, http://radical.rutgers.edu"
__license__   = "MIT"

from copy import deepcopy

from radical.entk import NoKernelConfigurationError
from radical.entk import KernelBase

# ------------------------------------------------------------------------------
# 
_KERNEL_INFO = {
			"name":         "sleep",
			"description":  "Writes Hello World to a file",
			"arguments":   {"--duration=":     
						{
							"mandatory": True,
							"description": "The input file."
						},
					},
			"machine_configs": 
			{
				"*": {
					"environment"   : None,
					"pre_exec"      : None,
					"executable"    : "/bin/sleep",
					"uses_mpi"      : False
				}
			}
	}


# ------------------------------------------------------------------------------
# 
class sleep_kernel(KernelBase):

	# --------------------------------------------------------------------------
	#
	def __init__(self):
		"""Le constructor.
		"""
		super(sleep_kernel, self).__init__(_KERNEL_INFO)


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

		executable = cfg['executable']
		arguments  = ['{0}'.format(self.get_arg("--duration="))]

		self._executable  = executable
		self._arguments   = arguments
		self._environment = cfg["environment"]
		self._uses_mpi    = cfg["uses_mpi"]
		self._pre_exec    = cfg["pre_exec"]

