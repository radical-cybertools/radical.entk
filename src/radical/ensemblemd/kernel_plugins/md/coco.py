#!/usr/bin/env python

"""The CoCo ... .
"""

__author__    = "Ole Weider <ole.weidner@rutgers.edu>"
__copyright__ = "Copyright 2014, http://radical.rutgers.edu"
__license__   = "MIT"

from copy import deepcopy

from radical.ensemblemd.exceptions import ArgumentError
from radical.ensemblemd.kernel_plugins.kernel_base import KernelBase

# ------------------------------------------------------------------------------
#
_KERNEL_INFO = {
    "name":            "md.coco",
    "description":     "CoCo ('Complementary Coordinates') Tools (https://bitbucket.org/extasy-project/coco).",
    "arguments":       "*",  # "*" means arguments are not evaluated and just passed through to the kernel.
    "machine_configs":
    {
        "*":
        {
            "environment" : {},
            "pre_exec"    : [],
            "executable"  : "pyCoCo",
            "uses_mpi"    : True,
        },

        "stampede":
        {
            "environment" : {},
            "pre_exec"    : ["module load intel/13.0.2.146","module load python","module load mpi4py","module load netcdf/4.3.2","module load hdf5/1.8.13","module load amber","export PYTHONPATH=/work/02998/ardi/coco_installation/lib/python2.7/site-packages:$PYTHONPATH","export PATH=/work/02998/ardi/coco_installation/bin:$PATH"],
            "executable"  : "pyCoCo",
            "uses_mpi"    : True,
        },

        "archer":
        {
            "environment" : {},
            "pre_exec"    : ["module load python","module load numpy","module load scipy","module load coco/0.3","module load netcdf4-python","module load amber"],
            "executable"  : "pyCoCo",
            "uses_mpi"    : True,
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
    def _get_kernel_description(self):
        """(PRIVATE) Implements parent class method. Returns the kernel
           description as a dictionary.
        """
        return {
            "environment" : None,
            "pre_exec"    : None,
            "post_exec"   : None,
            "executable"  : "COCO",
            "arguments"   : self.get_raw_args(),
            "use_mpi"     : True
        }
