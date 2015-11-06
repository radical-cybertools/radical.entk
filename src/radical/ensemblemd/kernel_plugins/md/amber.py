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
                    "--crdfile=":
                        {
                            "mandatory": False,
                            "description": "Input coordinate filename"
                        },
                    "--cycle=":
                        {
                            "mandatory": False,
                            "description": "Cycle number"
                        },
                    "--instance=":
                        {
                            "mandatory": False,
                            "description": "Instance number"
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
            "uses_mpi"      : False
        },
        "xsede.stampede":
        {
                "environment" : {},
                "pre_exec"    : ["module load TACC", "module load amber/12.0"],
                "executable"  : ["/opt/apps/intel13/mvapich2_1_9/amber/12.0/bin/sander"],
                "uses_mpi"    : False
        },
        "epsrc.archer":
        {
            "environment" : {},
            "pre_exec" : ["module load packages-archer","module load amber"],
            "executable" : ["pmemd"],
            "uses_mpi"   : True
        },
        "xsede.supermic":
        {
            "environment" : {},
            "pre_exec"    : ["module unload python/2.7.7-anaconda",
                             "module load  python/2.7.7/GCC-4.9.0",
                             "module load amber/14/INTEL-140-MVAPICH2-2.0"],
            "executable"  : ["sander.MPI"],
            "uses_mpi"    : True
        },
        "xsede.comet":
        {
                "environment" : {},
                "pre_exec"    : ["module load amber", "module load python"],
                "executable"  : ["/opt/amber/bin/sander"],
                "uses_mpi"    : False
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

            #change to pmemd.MPI by splitting into two kernels
            if self.get_arg("--mininfile=") is not None:
                arguments = [
                           '-O',
                            '-i',self.get_arg("--mininfile="),
                            '-o','min%s.out'%self.get_arg("--cycle="),
                            '-inf','min%s.inf'%self.get_arg("--cycle="),
                            '-r','md%s.crd'%self.get_arg("--cycle="),
                            '-p',self.get_arg("--topfile="),
                            '-c',self.get_arg("--crdfile="),
                            '-ref','min%s.crd'%self.get_arg("--cycle=")
                        ]
            else:
                arguments = [
                            '-O',
                            '-i',self.get_arg("--mdinfile="),
                            '-o','md%s.out'%self.get_arg("--cycle="),
                            '-inf','md%s.inf'%self.get_arg("--cycle="),
                            '-x','md%s.ncdf'%self.get_arg("--cycle="),
                            '-r','md%s.rst'%self.get_arg("--cycle="),
                            '-p',self.get_arg("--topfile="),
                            '-c','md%s.crd'%self.get_arg("--cycle="),
                        ]
       
            self._executable  = cfg["executable"]
            self._arguments   = arguments
            self._environment = cfg["environment"]
            self._uses_mpi    = False
            self._pre_exec    = cfg["pre_exec"] 
        
        #-----------------------------------------------------------------------
        # below only for RE
        elif (pattern_name == 'ReplicaExchange'):
        # if (pattern_name == None):
            cfg = _KERNEL_INFO["machine_configs"][resource_key]

            if resource_key not in _KERNEL_INFO["machine_configs"]:
                if "*" in _KERNEL_INFO["machine_configs"]:
                    # Fall-back to generic resource key
                    resource_key = "*"
                else:
                    raise NoKernelConfigurationError(kernel_name=KERNEL_INFO["name"], \
                                                     resource_key=resource_key)

            cfg = _KERNEL_INFO["machine_configs"][resource_key]

            if self._uses_mpi:
                self._executable = cfg["executable"] + ".MPI"
            else:
                self._executable = cfg["executable"]
                # by default MPI is false
                self._uses_mpi = cfg["uses_mpi"]
            self._arguments   = ["-O ",
                                 "-i ", self.get_arg("--mdinfile="),
                                 "-o ", self.get_arg("--outfile="),
                                 "-p ", self.get_arg("--params="),
                                 "-c ", self.get_arg("--coords="),
                                 "-r ", self.get_arg("--nwcoords="),
                                 "-x ", self.get_arg("--nwtraj="),
                                 "-inf ", self.get_arg("--nwinfo=")]
                                                                                              
            self._environment = cfg["environment"]

            if not self._pre_exec:
                self._pre_exec = cfg["pre_exec"]
            else:
                self._pre_exec = self._pre_exec + cfg["pre_exec"] 
      
