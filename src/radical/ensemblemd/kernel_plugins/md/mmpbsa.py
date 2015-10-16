#!/usr/bin/env python

"""MMPBSA.py - End-State Free Energy Calculations (http://pubs.acs.org/doi/abs/10.1021/ct300418h).
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
    "name":            "md.mmpbsa",
    "description":     "MMPBSA.py - End-State Free Energy Calculations (http://pubs.acs.org/doi/abs/10.1021/ct300418h).",
    "arguments":       "*",  # "*" means arguments are not evaluated and just passed through to the kernel.
    "machine_configs": 
    {
        "*": {
            "environment"   : {"FOO": "bar"},
            "pre_exec"      : ["sleep 1"],
            "executable"    : "MMPBSA.py",
            "uses_mpi"      : "False"
        },

        "stampede.tacc.utexas.edu": {
            "environment"   : {"FOO": "bar"},
            "pre_exec"      : ["module load python mpi4py amber"],
            "executable"    : "/opt/apps/intel13/mvapich2_1_9/amber/12.0/bin/MMPBSA.py.MPI",
            "uses_mpi"      : "True"
        },

        "archer.ac.uk": {
            "environment"   : {"FOO": "bar"},
            "pre_exec"      : ["module load amber"],
            "executable"    : "//work/y07/y07/amber/12/bin/MMPBSA.py.MPI",
            "uses_mpi"      : "True"
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

        self._executable  = cfg["executable"]
        self._arguments   = self.get_raw_args()
        self._environment = cfg["environment"]
        self._uses_mpi    = cfg["uses_mpi"]
        self._pre_exec    = cfg["pre_exec"] 
