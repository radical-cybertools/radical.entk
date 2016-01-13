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
    "name":         "custom.grompp",
    "description":  "Performs the preprocessing necessary for the following MD simulation. ",
    "arguments":    {"--mdp=":
                        {
                            "mandatory": True,
                            "description": "Input file with simulation parameters"
                        },
                     "--gro=":   
                        {
                            "mandatory": True,
                            "description": "A coordinates file - a .gro file in our case."
                        },
                     "--ref=":   
                        {
                            "mandatory": False,
                            "description": "A coordinates file - a .gro file in our case."
                        },
                     "--top=":   
                        {
                            "mandatory": True,
                            "description": "A topology file "
                        },
                     "--tpr=":   
                        {
                            "mandatory": True,
                            "description": "Output file as a portable binary run file - .tpr - containing the starting structure of the simulation, the molecular topology and all simulation parameters."
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
        "xsede.stampede": {
            "environment"   : {"FOO": "bar"},
            "pre_exec"      : ["module reset","module load intel/15.0.2","module load boost","module load cxx11","module load gromacs"],
            "executable"    : "gmx grompp",
            "uses_mpi"      : False
        },
        "epsrc.archer": {
            "environment"   : {"FOO": "bar"},
            "pre_exec"      : ["module load packages-archer","module load gromacs/5.0.0"],
            "executable"    : "gmx grompp",
            "uses_mpi"      : False
        }                
    }
}


# ------------------------------------------------------------------------------
#
class grompp_Kernel(KernelBase):

    # --------------------------------------------------------------------------
    #
    def __init__(self):
        """Le constructor.
        """
        super(grompp_Kernel, self).__init__(_KERNEL_INFO)

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
        
        if self.get_arg("--ref=") is None:
            arguments = ['-f','{0}'.format(self.get_arg("--mdp=")),'-c','{0}'.format(self.get_arg("--gro=")),'-p','{0}'.format(self.get_arg("--top=")),'-o','{0}'.format(self.get_arg("--tpr="))]
        else:			   
            arguments = ['-f','{0}'.format(self.get_arg("--mdp=")),'-r','{0}'.format(self.get_arg("--ref=")),'-c','{0}'.format(self.get_arg("--gro=")),'-p','{0}'.format(self.get_arg("--top=")),'-o','{0}'.format(self.get_arg("--tpr="))]
        
        self._executable  = cfg['executable']
        self._arguments   = arguments
        self._environment = cfg["environment"]
        self._uses_mpi    = cfg["uses_mpi"]
        self._pre_exec    = cfg["pre_exec"]
        self._post_exec   = None
