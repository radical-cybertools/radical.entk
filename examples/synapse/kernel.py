#!/usr/bin/env python

"""A kernel that creates a new ASCII file with a given size and name.
"""

__author__    = "The ExTASY project <ardita.shkurti@nottingham.ac.uk>"
__copyright__ = "Copyright 2015, http://www.extasy-project.org/"
__license__   = "MIT"

from copy import deepcopy

from radical.ensemblemd.exceptions import ArgumentError
from radical.ensemblemd.exceptions import NoKernelConfigurationError
from radical.ensemblemd.engine import get_engine
from radical.ensemblemd.kernel_plugins.kernel_base import KernelBase

# ------------------------------------------------------------------------------
#
_KERNEL_INFO = {
    "name":         "synapse.sample",
    "description":  "kernel to use radical.synapse",
    "arguments":    {

                        "--path=":
                        {
                            "mandatory": True,
                            "description": "Path to the synapse VE"
                        },
                        "--mode=":
                        {
                            "mandatory": True,
                            "description": "Operation mode"
                        },
                        "--flops=":
                        {
                            "mandatory": True,
                            "description": "No. of flops to perform"
                        },
                        "--input=":
                        {
                            "mandatory": False,
                            "description": "No. of bytes to read"
                        },
                        "--output=":
                        {
                            "mandatory": False,
                            "description": "No. of bytes to write"
                        },
                        "--memory=":
                        {
                            "mandatory": False,
                            "description": "No. of bytes to allocate"
                        },
                        "--samples=":
                        {
                            "mandatory": True,
                            "description": "No. of samples to run with above configuration"
                        },

                    },
    "machine_configs":
    {
        "*": {
            "environment"   : {},
            "pre_exec"      : [],
            "executable"    : "radical-synapse-sample",
            "uses_mpi"      : False
        }               
    }
}


# ------------------------------------------------------------------------------
#
class sample_Kernel(KernelBase):

    # --------------------------------------------------------------------------
    #
    def __init__(self):
        """Le constructor.
        """
        super(sample_Kernel, self).__init__(_KERNEL_INFO)

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

        arguments = [   '-m','{0}'.format(self.get_arg('--mode=')),
                        '-f','{0}'.format(self.get_arg('--flops=')),
                        '-s','{0}'.format(self.get_arg('--samples=')),
                    ]
        
        if self.get_arg('--input=') is not None:
            arguments.extend('-i','{0}'.format(self.get_arg('--input=')))

        if self.get_arg('--output=') is not None:
            arguments.extend('-o','{0}'.format(self.get_arg('--output=')))

        if self.get_arg('--memory=') is not None:
            arguments.extend('-r','{0}'.format(self.get_arg('--memory=')))


        self._pre_exec    = [   'export PYTHONPATH={0}/lib/python2.7/site-packages'.format(self.get_arg("--path=")),
                                'export PATH=$PATH:{0}/bin'.format(self.get_arg("--path=")),
                            ]
        self._executable  = cfg['executable']
        self._arguments   = arguments
        self._environment = cfg["environment"]
        self._uses_mpi    = cfg["uses_mpi"]
        self._post_exec   = None
