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
            "name":         "specfem",
            "description":  "Run specfem simulations",
            "arguments":   {},
            "machine_configs": 
            {
                "*": {
                    "environment"   : None,
                    "pre_exec"      : ['tar xf opdata.tar'],
                    "executable"    : #"/home/vivek/Research/repos/simpy/examples/solver_mockup/test_work_dir/bin/specfem_mockup",
                                        './bin/xspecfem3D',
                    "uses_mpi"      : True
                },

                "xsede.stampede": {
                    "environment"   : None,
                    "pre_exec"      : ['tar xf opdata.tar',
                                        # Modify the Par_file so the number of
                                       # runs is propoerly setup
                                        'sed -i "s:^NUMBER_OF_SIMULTANEOUS_RUNS.*:NUMBER_OF_SIMULTANEOUS_RUNS = 2:g" DATA/Par_file',

                                       # Main sub-dir for one earthquake
                                       'mkdir run0001',
                                        # Setup earthquake specific data
                                       'mkdir run0001/DATA',
                                       'cp DATA/CMTSOLUTION run0001/DATA/CMTSOLUTION',  # Actually an ad-hoc cmtsolution
                                       'cp DATA/STATIONS run0001/DATA/STATIONS',        # Actually an ad-hoc stations file
                                       'mkdir run0001/DATABASES_MPI',
                                       'mkdir run0001/OUTPUT_FILES',
                                       # files from mesher that are not read by every event
                                       'cp OUTPUT_FILES/addressing.txt run0001/OUTPUT_FILES',
                                       'cp OUTPUT_FILES/values_from_mesher.h run0001/OUTPUT_FILES',
                                       # run0001 should have access to the mesh
                                       'cd run0001/DATABASES_MPI/',
                                       'ln -s ../../DATABASES_MPI/* .',
                                       'cd ../..',

                                       # Main sub-dir for one earthquake
                                       'mkdir run0002',
                                        # Setup earthquake specific data
                                       'mkdir run0002/DATA',
                                       'cp DATA/CMTSOLUTION run0002/DATA/CMTSOLUTION',  # Actually an ad-hoc cmtsolution
                                       'cp DATA/STATIONS run0002/DATA/STATIONS',        # Actually an ad-hoc stations file
                                       'mkdir run0002/DATABASES_MPI',
                                       'mkdir run0002/OUTPUT_FILES',
                                       # files from mesher that are not read by every event
                                       'cp OUTPUT_FILES/addressing.txt run0002/OUTPUT_FILES',
                                       'cp OUTPUT_FILES/values_from_mesher.h run0002/OUTPUT_FILES'

                                      ],
                    "executable"    : #"/home/vivek/Research/repos/simpy/examples/solver_mockup/test_work_dir/bin/specfem_mockup",
                                        './bin/xspecfem3D',
                    "uses_mpi"      : True
                }
            }
    }


# ------------------------------------------------------------------------------
# 
class specfem_kernel(KernelBase):

    # --------------------------------------------------------------------------
    #
    def __init__(self):
        """Le constructor.
        """
        super(specfem_kernel, self).__init__(_KERNEL_INFO)


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

