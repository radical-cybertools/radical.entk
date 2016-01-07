#!/usr/bin/env python

"""A kernel that creates a new ASCII file with a given size and name.
"""

__author__    = "ExTASY project <ardita.shkurti@nottingham.ac.uk>"
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
    "name":         "custom.mdrun",
    "description":  "Molecular dynamics with the gromacs software package. http://www.gromacs.org/",
    "arguments":   {
                    "--size=":
                        {
                            "mandatory": True,
                            "description": "Number of threads that mdrun should use"
                        },
                    "--tpr=":   
                        {
                            "mandatory": True,
                            "description": "Input file as a portable binary run file - .tpr - containing the starting structure of the simulation, the molecular topology and all simulation parameters."
                        },
                    '--trr=':
                        {
                            "mandatory": False,
                            "description": "Output file"
                        },
                    "--edr=":
                        {
                            "mandatory": False,
                            "description": "Output file"
                        },
                    "--out=":
                        {
                            "mandatory": True,
                            "description": "Output coordinate file"
                        }

                    },
    "machine_configs":
    {
        "*": {
            "environment"   : {"FOO": "bar"},
            "pre_exec"      : [],
            "executable"    : "mdrun",
            "uses_mpi"      : True
        },

        "xsede.stampede":
        {
            "environment" : {},
            "pre_exec" : ["module reset","module load intel/15.0.2","module load boost","module load cxx11","module load gromacs"],
            "executable" : ["gmx mdrun"],
            "uses_mpi"   : False
        },

        "epsrc.archer":
        {
            "environment" : {},
            "pre_exec" : ["module load packages-archer","module load gromacs/5.0.0"],
            "executable" : ["gmx mdrun"],
            "uses_mpi"   : False
        },

    }
}


# ------------------------------------------------------------------------------
#
class mdrun_Kernel(KernelBase):

    # --------------------------------------------------------------------------
    #
    def __init__(self):
        """Le constructor.
        """
        super(mdrun_Kernel, self).__init__(_KERNEL_INFO)

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

        arguments = [   '-nt','{0}'.format(self.get_arg("--size=")),
                        '-s','{0}'.format(self.get_arg('--tpr=')),
                        '-c','{0}'.format(self.get_arg('--out='))
                    ]
        if self.get_arg('--trr=') is not None:
            arguments.extend(['-o','{0}'.format(self.get_arg('--trr='))])

        if self.get_arg('--edr=') is not None:
            arguments.extend(['-e','{0}'.format(self.get_arg('--edr='))])

        self._executable  = cfg["executable"]
        self._arguments   = arguments
        self._environment = cfg["environment"]
        self._uses_mpi    = cfg["uses_mpi"]
        self._pre_exec    = cfg["pre_exec"]
        self._post_exec   = None
