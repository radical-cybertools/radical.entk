#!/usr/bin/env python

"""A kernel that writes Hello World to a file.
"""

__author__    = "Vivek <vivek.balasubramanian@rutgers.edu>"
__copyright__ = "Copyright 2014, http://radical.rutgers.edu"
__license__   = "MIT"

from copy import deepcopy

from radical.entk import NoKernelConfigurationError
from radical.entk import KernelBase

# ------------------------------------------------------------------------------
# 
_KERNEL_INFO = {
            "name":         "sims",
            "description":  "Run specfem simulations",
            "arguments":   {},
            "machine_configs": 
            {
                "*": {
                    "environment"   : None,
                    "pre_exec"      : [
                                        'mkdir DATA',
                                            'mv Par_file DATA/',

                                        'mkdir DATABASE_MPI',

                                        'mkdir OUTPUT_FILES',
                                            'mv addressing.txt OUTPUT_FILES/',
                                            'mv values_from_mesher.h OUTPUT_FILES/',

                                        'mkdir run0001',
                                            'mkdir run0001/DATA',
                                                'cp CMTSOLUTION STATIONS run0001/DATA/',
                                            'cd run0001/',
                                            'ln -s ../DATABASE_MPI DATABASE_MPI',
                                            'cd ..',
                                            'mkdir run0001/OUTPUT_FILES',

                                        'mkdir bin',
                                            'chmod +x specfem_mockup',
                                            'cp specfem_mockup bin/'

                                        ],
                    "executable"    : #"/home/vivek/Research/repos/simpy/examples/solver_mockup/test_work_dir/bin/specfem_mockup",
                                        './bin/specfem_mockup',
                    "uses_mpi"      : True
                }
            }
    }


# ------------------------------------------------------------------------------
# 
class sims_kernel(KernelBase):

    # --------------------------------------------------------------------------
    #
    def __init__(self):
        """Le constructor.
        """
        super(sims_kernel, self).__init__(_KERNEL_INFO)


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

        executable = cfg['executable']
        arguments  = []

        self._executable  = executable
        self._arguments   = arguments
        self._environment = cfg["environment"]
        self._uses_mpi    = cfg["uses_mpi"]
        self._pre_exec    = cfg["pre_exec"]

