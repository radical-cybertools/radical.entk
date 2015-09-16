#!/usr/bin/env python

"""A kernel that creates a new ASCII file with a given size and name.
"""

__author__    = "Vivek <vivek.balasubramanian@rutgers.edu>"
__copyright__ = "Copyright 2014, http://radical.rutgers.edu"
__license__   = "MIT"

from copy import deepcopy

from radical.ensemblemd.exceptions import ArgumentError
from radical.ensemblemd.exceptions import NoKernelConfigurationError
from radical.ensemblemd.kernel_plugins.kernel_base import KernelBase

# ------------------------------------------------------------------------------
#
_KERNEL_INFO = {
    "name":         "md.pre_lsdmap",
    "description":  "Creates a new file of given size and fills it with random ASCII characters.",
    "arguments":   {"--numCUs=":
                        {
                            "mandatory": True,
                            "description": "No. of CUs"
                        }
                    },
    "machine_configs":
    {
        "*": {
            "environment"   : {"FOO": "bar"},
            "pre_exec"      : [],
            "executable"    : ".",
            "uses_mpi"      : True
        },

        "xsede.stampede":
        {
            "environment" : {},
            "pre_exec" : [
                            "module load intel/15.0.2",
                            "module load boost",
                            "module load python",
                            "export TACC_GROMACS_DIR=/opt/apps/intel13/mvapich2_1_9/gromacs/5.0.1",
                            "export TACC_GROMACS_LIB=$TACC_GROMACS_DIR/lib",
                            "export TACC_GROMACS_INC=$TACC_GROMACS_DIR/include",
                            "export TACC_GROMACS_BIN=$TACC_GROMACS_DIR/bin",
                            "export TACC_GROMACS_DOC=$TACC_GROMACS_DIR/share",
                            "export GMXLIB=/opt/apps/intel13/mvapich2_1_9/gromacs/5.0.1/share/gromacs/top",
                            "export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/opt/apps/intel/15/composer_xe_2015.2.164/mkl/lib/intel64/:/opt/apps/intel/15/composer_xe_2015.2.164/compiler/lib/intel64/:/opt/apps/intel15/python/2.7.9/lib/",
                            "export PATH=$TACC_GROMACS_BIN:$PATH"
                        ],
            "executable" : ["python"],
            "uses_mpi"   : False
        },

        "epsrc.archer":
        {
            "environment" : {},
            "pre_exec" : [
                            "module load packages-archer",
                            "module load gromacs/5.0.0",
                            "module load python-compute/2.7.6"
                        ],
            "executable" : ["python"],
            "uses_mpi"   : False
        },

        "futuregrid.india":
        {
            "environment" : {},
            "pre_exec" : [
                            "module load openmpi",
                            "module load python",
                            "export PATH=$PATH:/N/u/vivek91/modules/gromacs-5/bin:/N/u/vivek91/.local/bin"
                        ],
            "executable" : ["python"],
            "uses_mpi"   : False
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

        arguments = ['pre_analyze.py','{0}'.format(self.get_arg("--numCUs=")),'tmp.gro','.'] 

        self._executable  = cfg["executable"]
        self._arguments   = arguments
        self._environment = cfg["environment"]
        self._uses_mpi    = cfg["uses_mpi"]
        self._pre_exec    = cfg["pre_exec"]
