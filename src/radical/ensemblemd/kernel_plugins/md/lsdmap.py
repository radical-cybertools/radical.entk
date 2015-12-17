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
    "name":         "md.lsdmap",
    "description":  "Creates a new file of given size and fills it with random ASCII characters.",
    "arguments":   {"--config=":
                        {
                            "mandatory": True,
                            "description": "Config filename"
                        },
                    },
    "machine_configs":
    {
        "*": {
            "environment"   : {"FOO": "bar"},
            "pre_exec"      : [],
            "executable"    : "lsdmap",
            "uses_mpi"      : True
        },

        "xsede.stampede":
        {
            "environment" : {},
            "pre_exec" : [      
                            "module load TACC",
                            "module load -intel +intel/14.0.1.106",
                            "export PYTHONPATH=/home1/03036/jp43/.local/lib/python2.7/site-packages",
                            "module load python/2.7.6",
                            "export PATH=/home1/03036/jp43/.local/bin:$PATH"],
            #"executable" : ["/opt/apps/intel14/mvapich2_2_0/python/2.7.6/lib/python2.7/site-packages/mpi4py/bin/python-mpi"],
            "executable": ["lsdmap"],
            "uses_mpi"   : True
        },

        "epsrc.archer":
        {
            "environment" : {},
            "pre_exec" : [  "module load python-compute/2.7.6",
                            "module load pc-numpy/1.9.2-libsci",
                            "module load pc-scipy/0.15.1-libsci",
                            "module load lsdmap",
                            "export PYTHONPATH=/work/y07/y07/cse/lsdmap/lib/python2.7/site-packages:$PYTHONPATH"],
            "executable" : ["lsdmap"],
            "uses_mpi"   : True
        },

        "futuregrid.india":
        {
            "environment" : {},
            "pre_exec" : ["module load openmpi","module load python",
                        "export PATH=$PATH:/N/u/vivek91/.local/bin",
                        "export PYTHONPATH=$PYTHONPATH:/N/u/vivek91/.local/lib/python2.7/site-packages"
                        ],
            "executable" : ["python"],
            "uses_mpi"   : True
        },

        "ncsa.bw":
        {
            "environment" : {},
            "pre_exec" : ["source /projects/sciteam/gkd/virtenvs/lsdmap/20151210_OMPI20151210-DYN/bin/activate"],
            "executable" : ["lsdmap"],
            "uses_mpi"   : False
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

        arguments = ['-f','{0}'.format(self.get_arg("--config=")),'-c','tmpha.gro','-n','out.nn','-w','weight.w']

        self._executable  = cfg["executable"]
        self._arguments   = arguments
        self._environment = cfg["environment"]
        self._uses_mpi    = cfg["uses_mpi"]
        self._pre_exec    = cfg["pre_exec"]
