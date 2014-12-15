#!/usr/bin/env python

"""A kernel that creates a new ASCII file with a given size and name.
"""

__author__    = "Ole Weider <ole.weidner@rutgers.edu>"
__copyright__ = "Copyright 2014, http://radical.rutgers.edu"
__license__   = "MIT"

from copy import deepcopy

from radical.ensemblemd.exceptions import ArgumentError
from radical.ensemblemd.exceptions import NoKernelConfigurationError
from radical.ensemblemd.kernel_plugins.kernel_base import KernelBase

# ------------------------------------------------------------------------------
#
_KERNEL_INFO = {
    "name":         "md.gromacs",
    "description":  "Creates a new file of given size and fills it with random ASCII characters.",
    "arguments":   {"--grompp=":
                        {
                            "mandatory": True,
                            "description": "Input parameter filename"
                        },
                    "--topol=":
                        {
                            "mandatory": True,
                            "description": "Input topology filename"
                        },
                    "--inputfile=":
                        {
                            "mandatory": True,
                            "description": "Input gro filename"
                        },
                    "--outputfile=":
                        {
                            "mandatory": True,
                            "description": "Output gro filename"
                        }
                    },
    "machine_configs":
    {
        "*": {
            "environment" : {},
            "pre_exec"    : [],
            "executable"  : "mdrun",
            "uses_mpi"    : False,
            "test_cmd"    : "mdrun --version"
        },

        "stampede":
        {
            "environment" : {},
            "pre_exec"    : ["module load gromacs python mpi4py"],
            "executable"  : "mdrun_mpi",
            "uses_mpi"    : True,
            "test_cmd"    : "mdrun --version"
        },

        "archer":
        {
            "environment" : {},
            "pre_exec"    : ["module load packages-archer","module load gromacs"],
            "executable"  : "mdrun_mpi",
            "uses_mpi"    : True,
            "test_cmd"    : "mdrun --version"
        }
    }
}


# ------------------------------------------------------------------------------
#
class Kernel(KernelBase):

    # --------------------------------------------------------------------------
    #
    def __init__(self):
        """Le constructor.
        """
        super(Kernel, self).__init__(_KERNEL_INFO)

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

        self._executable  = None
        self._arguments   = None
        self._environment = None
        self._uses_mpi    = None
        self._pre_exec    = None
        self._post_exec   = None


    # --------------------------------------------------------------------------
    #
    # def _bind_to_resource(self, resource_key):
    #     """(PRIVATE) Implements parent class method.
    #     """
    #     if resource_key not in _KERNEL_INFO["machine_configs"]:
    #         if "*" in _KERNEL_INFO["machine_configs"]:
    #             # Fall-back to generic resource key
    #             resource_key = "*"
    #         else:
    #             raise NoKernelConfigurationError(kernel_name=_KERNEL_INFO["name"], resource_key=resource_key)
    #
    #     cfg = _KERNEL_INFO["machine_configs"][resource_key]
    #
    #     executable = "/bin/bash"
    #     arguments = ['-l', '-c', '{0} run.sh {1} {2} {3} {4}'.format(cfg["executable"],
    #                                                                  self.get_arg("--grompp="),
    #                                                                  self.get_arg("--inputfile="),
    #                                                                  self.get_arg("--topol="),
    #                                                                  self.get_arg("--outputfile="))]
    #
    #     self._executable  = executable
    #     self._arguments   = arguments
    #     self._environment = cfg["environment"]
    #     self._uses_mpi    = cfg["uses_mpi"]
    #     self._pre_exec    = cfg["pre_exec"]
    #     self._post_exec   = None
