#!/usr/bin/env python

"""Kernel for Amber Molecular Dynamics package http://ambermd.org/
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

    "name":         "md.amber",
    "description":  "Molecular Dynamics with Amber software package http://ambermd.org/",
    "arguments":   {"--mininfile=":
                        {
                            "mandatory": False,
                            "description": "Input parameter filename"
                        },
                    "--mdinfile=":
                        {
                            "mandatory": False,
                            "description": "Input parameter filename"
                        },
                    "--topfile=":
                        {
                            "mandatory": False,
                            "description": "Input topology filename"
                        },
                    "--cycle=":
                        {
                            "mandatory": False,
                            "description": "Cycle number"
                        },
                    "--outfile=":
                        {
                            "mandatory": False,
                            "description": "Output file"
                        },
                    "--params=":
                        {
                            "mandatory": False,
                            "description": "Parameters file"
                        },
                    "--coords=":
                        {
                            "mandatory": False,
                            "description": "Coordinates file"
                        },
                    "--nwcoords=":
                        {
                            "mandatory": False,
                            "description": "New coordinates file"
                        },
                    "--nwtraj=":
                        {
                            "mandatory": False,
                            "description": "New trajectory file"
                        },
                    "--nwinfo=":
                        {
                            "mandatory": False,
                            "description": "New info file"
                        }
                    },
    "machine_configs": 
    {
        "*": {
            "environment"   : {"FOO": "bar"},
            "pre_exec"      : [],
            "executable"    : "/bin/bash",
            "uses_mpi"      : True
        },
        "stampede.tacc.utexas.edu":
        {
            "environment" : {},
            "pre_exec" : ["module load TACC","module load amber"],
            "executable" : ["/bin/bash"],
            "uses_mpi"   : True
        },
        "xsede.stampede":
        {
                "environment" : {},
                "pre_exec"    : ["module load TACC", "module load amber/12.0"],
                "executable"  : ["/opt/apps/intel13/mvapich2_1_9/amber/12.0/bin/sander.MPI"],
                "uses_mpi"    : True
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
    def _bind_to_resource(self, resource_key, pattern_name=None):

        """(PRIVATE) Implements parent class method. 
        """
        
        if (pattern_name == None):        

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
        
        #----------------------------------------
        # below only for RE
        elif (pattern_name == 'ReplicaExchange'):
        # if (pattern_name == None):
            cfg = _KERNEL_INFO["machine_configs"][resource_key]

            if resource_key not in _KERNEL_INFO["machine_configs"]:
                if "*" in _KERNEL_INFO["machine_configs"]:
                    # Fall-back to generic resource key
                    resource_key = "*"
                else:
                    raise NoKernelConfigurationError(kernel_name=_KERNEL_INFO["name"], resource_key=resource_key)

            cfg = _KERNEL_INFO["machine_configs"][resource_key]

            self._executable  = cfg["executable"]
            self._arguments   = ["-O ",
                                 "-i ", self.get_arg("--mdinfile="),
                                 "-o ", self.get_arg("--outfile="),
                                 "-p ", self.get_arg("--params="),
                                 "-c ", self.get_arg("--coords="),
                                 "-r ", self.get_arg("--nwcoords="),
                                 "-x ", self.get_arg("--nwtraj="),
                                 "-inf ", self.get_arg("--nwinfo=")]
                                                                                              
            self._environment = cfg["environment"]
            self._uses_mpi    = cfg["uses_mpi"]
            self._pre_exec    = cfg["pre_exec"] 
      
