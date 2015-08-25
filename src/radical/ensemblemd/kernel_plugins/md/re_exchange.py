#!/usr/bin/env python

"""Exchange kernel for RE patterns.
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
    "name":         "md.re_exchange",
    "description":  "Calculates column of swap matrix for a given replica",
    "arguments":   {"--calculator=":     
                        {
                        "mandatory": True,
                        "description": "name of python calculator file"
                        },
                    "--replica_id=":     
                        {
                        "mandatory": False,
                        "description": "replica id"
                        },
                    "--replica_cycle=":     
                        {
                        "mandatory": True,
                        "description": "replica cycle"
                        },
                    "--replicas=":     
                        {
                        "mandatory": True,
                        "description": "number of replicas"
                        },
                    "--replica_basename=":     
                        {
                        "mandatory": False,
                        "description": "name of base file"
                        },
                    "--new_temperature=":
                        {
                        "mandatory": False,
                        "description": "temp"
                        },
                    },
    "machine_configs": 
    {
        "*": {
            "environment"   : None,
            "pre_exec"      : None,
            "executable"    : "python",
            "uses_mpi"      : False
        },
        "xsede.stampede":
        {
                "environment" : {},
                "pre_exec"    : ["module restore", "module load python"],
                "executable"  : ["python"],
                "uses_mpi"    : False
        },
        "xsede.comet":
        {
                "environment" : {},
                "pre_exec"    : ["module load amber", "module load python", "module load mpi4py"],
                "executable"  : ["python"],
                "uses_mpi"    : False
        },
        "lsu.supermic":
        {
                "environment" : {},
                "pre_exec"    : ["module load python"],
                "executable"  : ["python"],
                "uses_mpi"    : False
        }
    }
}

#-------------------------------------------------------------------------------
# 
class Kernel(KernelBase):

    #---------------------------------------------------------------------------
    #
    def __init__(self):
        """Le constructor.
        """
        super(Kernel, self).__init__(_KERNEL_INFO)

    #---------------------------------------------------------------------------
    #
    @staticmethod
    def get_name():
        return _KERNEL_INFO["name"]

    #---------------------------------------------------------------------------
    #
    def _bind_to_resource(self, resource_key):
        """(PRIVATE) Implements parent class method. 
        """
        if resource_key not in _KERNEL_INFO["machine_configs"]:
            if "*" in _KERNEL_INFO["machine_configs"]:
                # Fall-back to generic resource key
                resource_key = "*"
            else:
                raise NoKernelConfigurationError(kernel_name=_KERNEL_INFO["name"], \
                                                 resource_key=resource_key)

        cfg = _KERNEL_INFO["machine_configs"][resource_key]

        if self._subname == None:
            arguments  = [self.get_arg("--calculator="), 
                          self.get_arg("--replica_id="),
                          self.get_arg("--replica_cycle="),
                          self.get_arg("--replicas="),
                          self.get_arg("--replica_basename=")]
            self._executable  = cfg["executable"]
            self._arguments   = arguments
            self._environment = cfg["environment"]
            self._uses_mpi    = cfg["uses_mpi"]
            self._pre_exec    = cfg["pre_exec"] 
            self._post_exec   = None
        elif self._subname == "matrix_calculator_temp_ex":
            arguments  = [self.get_arg("--calculator="), 
                          self.get_arg("--replica_id="),
                          self.get_arg("--replica_cycle="),
                          self.get_arg("--replicas="),
                          self.get_arg("--replica_basename="),
                          self.get_arg("--new_temperature=")]
            self._executable  = cfg["executable"]
            self._arguments   = arguments
            self._environment = cfg["environment"]
            self._uses_mpi    = cfg["uses_mpi"]
            self._pre_exec    = cfg["pre_exec"] 
            self._post_exec   = None
        elif self._subname == "global_ex_calculator":
            arguments  = [self.get_arg("--calculator="), 
                          self.get_arg("--replica_cycle="),
                          self.get_arg("--replicas="),
                          self.get_arg("--replica_basename=")]
            self._executable  = cfg["executable"]
            self._arguments   = arguments
            self._environment = cfg["environment"]
            self._uses_mpi    = True
            self._pre_exec    = cfg["pre_exec"] 
            self._post_exec   = None

