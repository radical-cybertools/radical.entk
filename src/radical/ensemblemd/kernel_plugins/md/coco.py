#!/usr/bin/env python

"""A kernel that creates a new ASCII file with a given size and name.
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

    "name":         "md.coco",
    "description":  "Creates a new file of given size and fills it with random ASCII characters.",
    "arguments":   {"--grid=":
                        {
                            "mandatory": True,
                            "description": "No. of grid points"
                        },
                    "--dims=":
                        {
                            "mandatory": True,
                            "description": "No. of dimensions"
                        },
                    "--frontpoints=":
                        {
                            "mandatory": True,
                            "description": "No. of frontpoints = No. of simulation CUs"
                        },
                    "--topfile=":
                        {
                            "mandatory": True,
                            "description": "Topology filename"
                        },
                    "--mdfile=":
                        {
                            "mandatory": True,
                            "description": "NetCDF filename"
                        },
                    "--output=":
                        {
                            "mandatory": True,
                            "description": "Output filename for postexec"
                        },
                    },
    "machine_configs": 
    {
        "*": {
            "environment"   : {"FOO": "bar"},
            "pre_exec"      : [],
            "executable"    : "pyCoCo",
            "uses_mpi"      : True
        },

        "xsede.stampede":
        {
            "environment" : {},
            "pre_exec" : ["module load intel/13.0.2.146","module load python/2.7.9","module load mpi4py","module load netcdf/4.3.2","module load hdf5/1.8.13","module load amber","export PYTHONPATH=/work/02998/ardi/coco-0.6_installation/lib/python2.7/site-packages:$PYTHONPATH","export PATH=/work/02998/ardi/coco-0.6_installation/bin:$PATH"],
            "executable" : ["pyCoCo"],
            "uses_mpi"   : True
        },

        "epsrc.archer":
        {
            "environment" : {},
            "pre_exec" : ["module load python","module load numpy","module load scipy","module load coco","module load netcdf4-python","module load amber"],
            "executable" : ["pyCoCo"],
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

        executable = cfg["executable"]
        arguments = [
                    '--grid','{0}'.format(self.get_arg("--grid=")),
                    '--dims','{0}'.format(self.get_arg("--dims=")),
                    '--frontpoints','{0}'.format(self.get_arg("--frontpoints=")),
                    '--topfile','{0}'.format(self.get_arg("--topfile=")),
                    '--mdfile','{0}'.format(self.get_arg("--mdfile=")),
                    '--output','{0}'.format(self.get_arg("--output="))
                    ]
                                                                     
       
        self._executable  = executable
        self._arguments   = arguments
        self._environment = cfg["environment"]
        self._uses_mpi    = cfg["uses_mpi"]
        self._pre_exec    = cfg["pre_exec"] 
        self._post_exec   = None
