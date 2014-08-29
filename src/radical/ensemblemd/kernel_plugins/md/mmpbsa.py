#!/usr/bin/env python

"""MMPBSA.py - End-State Free Energy Calculations (http://pubs.acs.org/doi/abs/10.1021/ct300418h).
"""

__author__    = "Ole Weider <ole.weidner@rutgers.edu>"
__copyright__ = "Copyright 2014, http://radical.rutgers.edu"
__license__   = "MIT"

from copy import deepcopy

from radical.ensemblemd.exceptions import ArgumentError
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
            "pre_exec"      : None,
            "executable"    : "MMPBSA.py",
            "uses_mpi"      : "False"
        },

        "stampede.tacc.utexas.edu": {
            "pre_exec"      : ["module load python mpi4py amber"],
            "executable"    : "/opt/apps/intel13/mvapich2_1_9/amber/12.0/bin/MMPBSA.py.MPI",
            "uses_mpi"      : "True"
        },

        "archer.ac.uk": {
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
    def _get_kernel_description(self):
        """(PRIVATE) Implements parent class method. Returns the kernel
           description as a dictionary.
        """
        return {
            "environment" : None,
            "pre_exec"    : None,
            "post_exec"   : None,
            "executable"  : "MMBSA",
            "arguments"   : self.get_raw_args(),
            "use_mpi"     : False
        }
