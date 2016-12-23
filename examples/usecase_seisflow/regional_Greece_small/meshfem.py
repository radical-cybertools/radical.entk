#!/usr/bin/env python

__author__    = "Vivek <vivek.balasubramanian@rutgers.edu>"
__copyright__ = "Copyright 2014, http://radical.rutgers.edu"
__license__   = "MIT"

from copy import deepcopy

from radical.entk import NoKernelConfigurationError
from radical.entk import KernelBase

# ------------------------------------------------------------------------------
# 
_KERNEL_INFO = {
            "name":         "meshfem",
            "description":  "Run specfem simulations",
            "arguments":   {},
            "machine_configs": 
            {
                "*": {
                    "environment"   : None,
                    "pre_exec"      : [
                                        'tar xf ipdata.tar',
                                        'mkdir DATABASES_MPI',
                                        'mkdir OUTPUT_FILES'

                                        ],
                    "executable"    : #"/home/vivek/Research/repos/simpy/examples/solver_mockup/test_work_dir/bin/specfem_mockup",
                                        './bin/xmeshfem3D',
                    "uses_mpi"      : True,
                    "post_exec"     : ['tar cf opdata.tar DATA/ DATABASES_MPI/ OUTPUT_FILES/ bin/']
                },

                "xsede.stampede": {
                    "environment"   : None,
                    "pre_exec"      : [
                                        'tar xf ipdata.tar',
                                        'mkdir DATABASES_MPI',
                                        'mkdir OUTPUT_FILES',
                                        'cp /work/02734/vivek91/specfem3d_globe/bin . -fr',

                                        'mkdir run0001',
                                        'cp DATA/ run0001/DATA -rf',
                                        'rm run0001/DATA/Par_*',
                                        'cd run0001/',
                                            'cp ../DATABASES_MPI DATABASES_MPI -r',
                                            'cd ..',
                                        'mkdir run0001/OUTPUT_FILES',


                                        'mkdir run0002',
                                        'cp run0001/DATA run0002/ -rf',
                                        'cd run0002/',
                                            'cp ../DATABASES_MPI DATABASES_MPI -r',
                                            'cd ..',
                                        'mkdir run0002/OUTPUT_FILES'

                                        ],
                    "executable"    : #"/home/vivek/Research/repos/simpy/examples/solver_mockup/test_work_dir/bin/specfem_mockup",
                                        './bin/xmeshfem3D',
                    "uses_mpi"      : True,
                    "post_exec"     : ['tar cf opdata.tar DATA/ DATABASES_MPI/ OUTPUT_FILES/ bin/ run0001/ run0002/']
                },
            }
    }


# ------------------------------------------------------------------------------
# 
class meshfem_kernel(KernelBase):

    # --------------------------------------------------------------------------
    #
    def __init__(self):
        """Le constructor.
        """
        super(meshfem_kernel, self).__init__(_KERNEL_INFO)


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
        self._post_exec   = cfg["post_exec"]

