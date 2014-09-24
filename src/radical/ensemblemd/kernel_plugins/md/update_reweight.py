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
import os

# ------------------------------------------------------------------------------
# 
_KERNEL_INFO = {
    "name":         "md.gromacs",
    "description":  "Creates a new file of given size and fills it with random ASCII characters.",
    "arguments":   {"--nruns=":
                        {
                            "mandatory": True,
                            "description": "Number of runs "
                        },
                    "--evfile=":
                        {
                            "mandatory": True,
                            "description": "Eigen vector filename"
                        },
                    "--clones=":
                        {
                            "mandatory": True,
                            "description": "Number of clones filename"
                        },
                    "--grofile=":
                        {
                            "mandatory": True,
                            "description": "Input gro filename"
                        },

                    "--nnfile=":
                        {
                            "mandatory": True,
                            "description": "Nearest neighbor filename"
                        },

                    "--wfile=":
                        {
                            "mandatory": True,
                            "description": "Weight filename"
                        },

                    "--outputfile=":
                        {
                            "mandatory": True,
                            "description": "Output gro filename"
                        }
                    },
    "machine_configs": 
    {
        "*": {
            "environment"   : {"FOO": "bar"},
            "pre_exec"      : [],
            "executable"    : ".",
            "uses_mpi"      : False
        },

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

        os.system('python select.py %s -s %s -o %s' %(self.get_args('--nruns='),self.get_args('--evfile='),self.get_args('--clones=')))
        #Update Boltzman weights

        os.system('python reweighting.py -c %s -n %s -s %s -w %s -o %s'  %(self.get_args('--grofile'),
                                                                           self.get_args('--nnfile='),
                                                                           self.get_args('--clones='),
                                                                           self.get_args('--wfile='),
                                                                           self.get_args('--outputfile='),
                                                                           )
                                                                            )

        '''
        executable = "/bin/bash"
        arguments = ['-l', '-c', '{0} run.sh {1} {2} {3} {4}'.format(cfg["executable"],
                                                                     self.get_arg("--grompp="),
                                                                     self.get_arg("--inputfile="),
                                                                     self.get_arg("--topol="),
                                                                     self.get_arg("--outputfile="))]
       
        self._executable  = executable
        self._arguments   = arguments
        self._environment = cfg["environment"]
        self._uses_mpi    = cfg["uses_mpi"]
        self._pre_exec    = cfg["pre_exec"] 
        self._post_exec   = None
        '''