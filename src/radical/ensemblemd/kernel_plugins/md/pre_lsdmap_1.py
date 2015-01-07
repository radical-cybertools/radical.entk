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
    "name":         "md.pre_lsdmap_1",
    "description":  "Creates a new file of given size and fills it with random ASCII characters.",
    "arguments":   {"--out=":
                        {
                            "mandatory": True,
                            "description": "Output filename"
                        },
                    "--numCUs=":
                        {
                            "mandatory": True,
                            "description": "No. of files to be concatenated"
                        }
                    },
    "machine_configs":
    {
        "stampede.tacc.utexas.edu": {
            "environment"   : {"FOO": "bar"},
            "pre_exec"      : ["module load python"],
            "executable"    : "python",
            "uses_mpi"      : False
        },
        "archer.ac.uk": {
            "environment"   : {"FOO": "bar"},
            "pre_exec"      : ["module load python"],
            "executable"    : "python",
            "uses_mpi"      : False
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

        arguments = ['pre_analyze.py','{0}'.format(self.get_arg("--numCUs=")), '{0}'.format(self.get_arg("--out=")),'.']

        self._executable  = cfg["executable"]
        self._arguments   = arguments
        self._environment = cfg["environment"]
        self._uses_mpi    = cfg["uses_mpi"]
        self._pre_exec    = cfg["pre_exec"]

    #Can I just split the file locally without doing any of the above RP stuff ??

