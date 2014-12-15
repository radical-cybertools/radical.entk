#!/usr/bin/env python

"""The AMBER molecular dynamics package (http://ambermd.org/).
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
    def _get_kernel_description(self):
        """(PRIVATE) Implements parent class method. Returns the kernel
           description as a dictionary.
        """
        return {
            "environment" : None,
            "pre_exec"    : None,
            "post_exec"   : None,
            "executable"  : "AMBER",
            "arguments"   : self.get_raw_args(),
            "use_mpi"     : True
        }
