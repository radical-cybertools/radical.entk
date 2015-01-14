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
    "name":         "md.post_lsdmap",
    "description":  "Creates a new file of given size and fills it with random ASCII characters.",
    "arguments":   {"--num_runs=":
                        {
                            "mandatory": True,
                            "description": "Number of runs to be generated in output file"
                        },
                    "--out=":
                        {
                            "mandatory": True,
                            "description": "Output filename"
                        },
                    "--cycle=":
                        {
                            "mandatory": True,
                            "description": "Current iteration"
                        },
                    "--max_dead_neighbors=":
                        {
                            "mandatory": True,
                            "description": "Max dead neighbors to be considered"
                        },
                    "--max_alive_neighbors=":
                        {
                            "mandatory": True,
                            "description": "Max alive neighbors to be considered"
                        },
                    "--numCUs=":
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

        "stampede.tacc.utexas.edu":
        {
            "environment" : {},
            "pre_exec" : ["module load python","export PYTHONPATH=/home1/03036/jp43/.local/lib/python2.7/site-packages:$PYTHONPATH","export PYTHONPATH=/home1/03036/jp43/.local/lib/python2.7/site-packages/lsdmap/rw:$PYTHONPATH","export PYTHONPATH=/home1/03036/jp43/.local/lib/python2.7/site-packages/util:$PYTHONPATH"],
            "executable" : ["python"],
            "uses_mpi"   : True
        },

        "archer.ac.uk":
        {
            "environment" : {},
            "pre_exec" : ["module load packages-archer","module load python"],
            "executable" : ["python"],
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

        arguments = ['post_analyze.py','{0}'.format(self.get_arg("--num_runs=")),'tmpha.ev','ncopies.nc','tmp.gro'
                     ,'out.nn','weight.w','{0}'.format(self.get_arg("--out="))
                     ,'{0}'.format(self.get_arg("--max_alive_neighbors=")),'{0}'.format(self.get_arg("--max_dead_neighbors="))
                     ,'input.gro','{0}'.format(self.get_arg("--cycle="),'{0}'.format(self.get_arg('--numCUs=')))
                     ]

        self._executable  = cfg["executable"]
        self._arguments   = arguments
        self._environment = cfg["environment"]
        self._uses_mpi    = cfg["uses_mpi"]
        self._pre_exec    = cfg["pre_exec"]
