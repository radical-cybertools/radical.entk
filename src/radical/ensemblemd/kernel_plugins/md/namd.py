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
        "*": {
            "environment"   : {"FOO": "bar"},
            "pre_exec"      : ["sleep 1"],
            "executable"    : "namd2",
            "uses_mpi"      : "False"
        },
        "xsede.stampede": {
            "environment"   : {"FOO": "bar"},
            "pre_exec"      : ["module load TACC && module load namd/2.9"],
            "executable"    : "/opt/apps/intel13/mvapich2_1_9/namd/2.9/bin/namd2",
            "uses_mpi"      : "True"
        },
        "epsrc.archer": {
            "environment"   : {"FOO": "bar"},
            "pre_exec"      : ["module load namd"],
            "executable"    : "/usr/local/packages/namd/namd-2.9/bin/namd2",
            "uses_mpi"      : "True"
        },
        "xsede.gordon": {
            "environment"   : {"FOO": "bar"},
            "pre_exec"      : ["module load namd/2.9"],
            "executable"    : "/opt/namd/2.9/bin/namd2",
            "uses_mpi"      : "True"
        },
        "supermuc.lrz.de": {
            "environment"   : {"FOO": "bar"},
            "pre_exec"      : ["source /etc/profile.d/modules.sh", "module load namd"],
            "executable"    : "/lrz/sys/applications/namd/2.9.1/mpi.ibm/NAMD_CVS-2013-11-11_Source/Linux-x86_64-icc/namd2",
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

