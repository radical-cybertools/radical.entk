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

    "name":         "md.amber",
    "description":  "Creates a new file of given size and fills it with random ASCII characters.",
    "arguments":   {"--mininfile=":
                        {
                            "mandatory": True,
                            "description": "Input parameter filename"
                        },
                    "--mdinfile=":
                        {
                            "mandatory": True,
                            "description": "Input parameter filename"
                        },
                    "--topfile=":
                        {
                            "mandatory": True,
                            "description": "Input topology filename"
                        },
                    "--cycle=":
                        {
                            "mandatory": True,
                            "description": "Cycle number"
                        },
                    },
    "machine_configs": 
    {
        "*": {
            "environment"   : {"FOO": "bar"},
            "pre_exec"      : [],
            "executable"    : ".",
            "uses_mpi"      : True
        },

        "stampede.tacc.utexas.edu":
        {
            "environment" : {},
            "pre_exec" : ["module load TACC && module load amber"],
            "executable" : ["/bin/bash"],
            "uses_mpi"   : True
        },

        "archer.ac.uk":
        {
            "environment" : {},
            "pre_exec" : ["module load packages-archer","module load amber"],
            "executable" : ["/bin/bash"],
            "uses_mpi"   : True
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

        executable = "/bin/bash"
        #change to pmemd.MPI by splitting into two kernels
        arguments = ['-l','-c','pmemd -O -i {0} -o min{2}.out -inf min{2}.inf -r md{2}.crd -p {1} -c min{2}.crd -ref min{2}.crd && pmemd -O -i {3} -o md{2}.out -inf md{2}.inf -x md{2}.ncdf -r md{2}.rst -p {1} -c md{2}.crd'.format(
                                                                     self.get_arg("--mininfile="),
                                                                     self.get_arg("--topfile="),
                                                                     self.get_arg("--cycle="),
                                                                     self.get_arg("--mdinfile="))]
       
        self._executable  = executable
        self._arguments   = arguments
        self._environment = cfg["environment"]
        self._uses_mpi    = cfg["uses_mpi"]
        self._pre_exec    = cfg["pre_exec"] 
        self._post_exec   = None

