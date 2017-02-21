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
    "name":         "chksum",
    "description":  "Calculates an SHA1 checksum for a given file.",
    "arguments":   {"--inputfile=":     
                        {
                        "mandatory": True,
                        "description": "The input file."
                        },
                    "--outputfile=":     
                        {
                        "mandatory": True,
                        "description": "The output file containing SHA1 sum."
                        },
                    },
    "machine_configs": 
    {
        "*": {
            "environment"   : None,
            "pre_exec"      : ["command -v sha1sum >/dev/null 2>&1 && export SHASUM=sha1sum  || export SHASUM=shasum"],
            "executable"    : "$SHASUM",
            "uses_mpi"      : False
        }
    }
}


# ------------------------------------------------------------------------------
# 
class chksum_kernel(KernelBase):

    # --------------------------------------------------------------------------
    #
    def __init__(self):
        """Le constructor.
        """
        super(chksum_kernel, self).__init__(_KERNEL_INFO)

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
        arguments  = ['-l', '-c', '{0} {1} > {2}'.format(
            cfg["executable"],
            self.get_arg("--inputfile="),
            self.get_arg("--outputfile="))
        ]

        self._executable  = executable
        self._arguments   = arguments
        self._environment = cfg["environment"]
        self._uses_mpi    = cfg["uses_mpi"]
        self._pre_exec    = cfg["pre_exec"] 
