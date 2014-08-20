#!/usr/bin/env python

"""A kernel that creates a new ASCII file with a given size and name.
"""

__author__    = "Ole Weider <ole.weidner@rutgers.edu>"
__copyright__ = "Copyright 2014, http://radical.rutgers.edu"
__license__   = "MIT"

from copy import deepcopy

from radical.ensemblemd.exceptions import ArgumentError
from radical.ensemblemd.kernels.kernel_base import KernelBase

# ------------------------------------------------------------------------------
# 
_KERNEL_INFO = {
    "name":            "md.namd",
    "description":     "The NAMD molecular dynamics toolkit. (http://www.ks.uiuc.edu/Research/namd/)",
    "arguments":       "*",  # "*" means arguments are not evaluated and just passed through to the kernel.
    "machine_configs": 
    {
        "stampede.tacc.utexas.edu": {
            "pre_exec"      : ["module load TACC && module load namd/2.9"],
            "executable"    : "/opt/apps/intel13/mvapich2_1_9/namd/2.9/bin/namd2",
            "uses_mpi"      : "True"
        },
        "india.futuregrid.org": {
            "executable"    : "/N/u/marksant/software/bin/namd2",
            "uses_mpi"      : "True"
        },
        "archer.ac.uk": {
            "pre_exec"      : ["module load namd"],
            "executable"    : "/usr/local/packages/namd/namd-2.9/bin/namd2",
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
            "executable"  : "NAMD",
            "arguments"   : self.get_raw_args(),
            "use_mpi"     : False
        }
