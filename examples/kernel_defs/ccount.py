#!/usr/bin/env python

"""A kernel that creates a new ASCII file with a given size and name.
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
    "name":         "ccount",
    "description":  "Counts the character frequency in an ASCII file.",
    "arguments":   {"--inputfile=":     
                        {
                        "mandatory": True,
                        "description": "The input ASCII file."
                        },
                    "--outputfile=":     
                        {
                        "mandatory": True,
                        "description": "The output file containing the character counts."
                        },
                    },
    "machine_configs": 
    {
        "*": {
            "environment"   : None,
            "pre_exec"      : None,
            "executable"    : "uniq",
            "uses_mpi"      : False
        }
    }
}


# ------------------------------------------------------------------------------
# 
class ccount_kernel(KernelBase):

    # --------------------------------------------------------------------------
    #
    def __init__(self):
        """Le constructor.
        """
        super(ccount_kernel, self).__init__(_KERNEL_INFO)

    # --------------------------------------------------------------------------
    #
    @staticmethod
    def get_name():
        return _KERNEL_INFO["name"]

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

        executable = "/bin/bash" 
        arguments  = ['-l', '-c', 'grep -o . {0} | sort | {1} -c > {2}'.format(
            self.get_arg("--inputfile="),
            cfg["executable"],
            self.get_arg("--outputfile="))
        ]

        self._executable  = executable
        self._arguments   = arguments
        self._environment = cfg["environment"]
        self._uses_mpi    = cfg["uses_mpi"]
        self._pre_exec    = None 

