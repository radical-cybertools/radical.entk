#!/usr/bin/env python

"""The NAMD molecular dynamics toolkit. (http://www.ks.uiuc.edu/Research/namd/).
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
    "name":            "md.namd",
    "description":     "The NAMD molecular dynamics toolkit (http://www.ks.uiuc.edu/Research/namd/)",
    "arguments":       "*",  # "*" means arguments are not evaluated and just passed through to the kernel.
    "machine_configs": 
    {
        "localhost": {
            "pre_exec"      : [],
            "executable"    : "namd2",
            "uses_mpi"      : "True"
        },
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
    def _get_kernel_description(self, resource_key):
        """(PRIVATE) Implements parent class method. Returns the kernel
           description as a dictionary.
        """
        if resource_key not in _KERNEL_INFO["machine_configs"]:
            raise NoKernelConfigurationError(kernel_name=_KERNEL_INFO["name"], resource_key=resource_key)

        return {
            "environment" : None,
            "pre_exec"    : None,
            "post_exec"   : None,
            "executable"  : "NAMD",
            "arguments"   : self.get_raw_args(),
            "use_mpi"     : False
        }
