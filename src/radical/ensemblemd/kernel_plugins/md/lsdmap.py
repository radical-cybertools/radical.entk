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
    "name":         "md.lsdmap",
    "description":  "Creates a new file of given size and fills it with random ASCII characters.",
    "arguments":   {"--nnfile=":
                        {
                            "mandatory": True,
                            "description": "Nearest neighbor filename"
                        },
                    "--wfile=":
                        {
                            "mandatory": True,
                            "description": "Weight filename"
                        },
                    "--config=":
                        {
                            "mandatory": True,
                            "description": "LSDMap config filename"
                        },
                    "--inputfile=":
                        {
                            "mandatory": True,
                            "description": "Output gro filename"
                        }
                    },
    "machine_configs":
    {
        "*": {
            "environment" : {},
            "pre_exec"    : [],
            "executable"  : "mdrun",
            "uses_mpi"    : False,
            "test_cmd"    : ""
        },

        "stampede":
        {
            "environment" : {},
            "pre_exec"    : ["module load -intel +intel/14.0.1.106","module load python","export PYTHONPATH=/home1/03036/jp43/.local/lib/python2.7/site-packages:$PYTHONPATH","export PATH=/home1/03036/jp43/.local/bin:$PATH"],
            "executable"  : "lsdmap",
            "uses_mpi"    : True,
            "test_cmd"    : "python -c \"import lsdmap; print lsdmap\""
        },

        "archer":
        {
            "environment" : {},
            "pre_exec"    : ["module load python","module load numpy","module load scipy"," module load lsdmap","export PYTHONPATH=/work/y07/y07/cse/lsdmap/lib/python2.7/site-packages:$PYTHONPATH"],
            "executable"  : "lsdmap",
            "uses_mpi"    : True,
            "test_cmd"    : "python -c \"import lsdmap; print lsdmap\""
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
        arguments = ['-l', '-c', '{0} run_analyzer.sh {1} {2}'.format(cfg["executable"],
                                                                        self.get_args("--nnfile="),
                                                                        self.get_args("--wfile="))]

        self._executable  = executable
        self._arguments   = arguments
        self._environment = cfg["environment"]
        self._uses_mpi    = cfg["uses_mpi"]
        self._pre_exec    = cfg["pre_exec"]
        self._post_exec   = None
