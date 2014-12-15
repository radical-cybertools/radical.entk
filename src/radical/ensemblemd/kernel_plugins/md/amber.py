#!/usr/bin/env python

"""The AMBER molecular dynamics package (http://ambermd.org/).
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
    "name":            "md.amber",
    "description":     "The AMBER molecular dynamics package (http://ambermd.org/).",
    "arguments":       "*",  # "*" means arguments are not evaluated and just passed through to the kernel.
    "machine_configs":
    {
        "*":
        {
            "environment" : {},
            "pre_exec"    : [],
            "executable"  : "pmemd.MPI",
            "uses_mpi"    : True,
            "test_cmd"    : "pmemd --version"
        },

        "stampede":
        {
            "environment" : {},
            "pre_exec"    : ["module load TACC && module load amber"],
            "executable"  : "pmemd.MPI",
            "uses_mpi"    : True,
            "test_cmd"    : "pmemd --version"
        },

        "archer":
        {
            "environment" : {},
            "pre_exec"    : ["module load packages-archer","module load amber"],
            "executable"  : "pmemd.MPI",
            "uses_mpi"    : True,
            "test_cmd"    : "pmemd --version"
        },

        "biou":
        {
            "environment" : {},
            "pre_exec"    : ["module load amber"],
            "executable"  : ["pmemd.MPI"],
            "uses_mpi"    : True,
            "test_cmd"    : "pmemd --version"

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
