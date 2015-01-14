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
                    "--cycle=":
                        {
                            "mandatory": True,
                            "description": "Output filename for postexec"
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
            "pre_exec" : ["module load intel/13.0.2.146","module load python","module load netcdf/4.3.2","module load hdf5/1.8.13","module load amber","export PYTHONPATH=/work/02998/ardi/coco_installation/lib/python2.7/site-packages:$PYTHONPATH","export PATH=/work/02998/ardi/coco_installation/bin:$PATH"],
            "executable" : ["/bin/bash"],
            "uses_mpi"   : True
        },

        "archer.ac.uk":
        {
            "environment" : {},
            "pre_exec" : ["module load python","module load numpy","module load scipy","module load coco","module load netcdf4-python","module load amber"],
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
        arguments = ['-l','-c','pyCoCo --grid {0} --dims {1} --frontpoints {2} --topfile {3} --mdfile {4} --output {5} && python postexec.py {2} {6}'.format(
                                                                     self.get_arg("--grid="),
                                                                     self.get_arg("--dims="),
                                                                     self.get_arg("--frontpoints="),
                                                                     self.get_arg("--topfile="),
                                                                     self.get_arg("--mdfile="),
                                                                     self.get_arg("--output="),
                                                                     self.get_arg("--cycle="))]
       
        self._executable  = executable
        self._arguments   = arguments
        self._environment = cfg["environment"]
        self._uses_mpi    = cfg["uses_mpi"]
        self._pre_exec    = cfg["pre_exec"] 
        self._post_exec   = None
