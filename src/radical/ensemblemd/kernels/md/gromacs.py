#!/usr/bin/env python

"""A kernel that creates a new ASCII file with a given size and name.
"""

__author__    = "Ole Weider <ole.weidner@rutgers.edu>"
__copyright__ = "Copyright 2014, http://radical.rutgers.edu"
__license__   = "MIT"

from copy import deepcopy

from radical.ensemblemd.exceptions import ArgumentError
from radical.ensemblemd.kernels.kernel_base import KernelBase

# ------------------------------------------------------------------------------
# 
_KERNEL_INFO = {
    "name":            "md.gromacs",
    "description":     "The GROMACS molecular dynamics toolkit.  (http://www.gromacs.org/).",
    "arguments":       "*",  # "*" means arguments are not evaluated and just passed through to the kernel.
    "machine_configs": 
    {
        "stampede.tacc.utexas.edu": 
        {
            "environment" : {},
            "pre_exec"    : ["module load TACC && module load gromacs"],
            "executable"  : ["/bin/bash"]
        },
        "sierra.futuregrid.org":
        {
            "environment" : {},
            "pre_exec"    : ["export PATH=$PATH:~marksant/bin"],
            "executable"  : ["/bin/bash"]
        },
        "trestles.sdsc.xsede.org":
        {
            "environment" : {},
            "pre_exec"    : ["(test -d $HOME/bin || mkdir $HOME/bin)","export PATH=$PATH:$HOME/bin","module load gromacs","ln -s /opt/gromacs/bin/grompp_mpi $HOME/bin/grompp && ln -s /opt/gromacs/bin/mdrun_mpi $HOME/bin/mdrun"],
            "executable"  : ["/bin/bash"]
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
            "executable"  : "GROMACS",
            "arguments"   : self.get_raw_args(),
            "use_mpi"     : False
        }
