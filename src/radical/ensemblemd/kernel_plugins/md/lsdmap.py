#!/usr/bin/env python

"""LSDMap is used to compute locally scaled diffusion maps (https://github.com/jp43/lsdmap).
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
    "name":            "md.lsdmap",
    "description":     "LSDMap is used to compute locally scaled diffusion maps (https://github.com/jp43/lsdmap).",
    "arguments":       "*",  # "*" means arguments are not evaluated and just passed through to the kernel.
    "machine_configs": 
    {
        "trestles.sdsc.xsede.org":
        {
            "environment" : {},
            "pre_exec" : ["module load python","module load scipy","module load mpi4py","(test -d $HOME/lsdmap || (git clone https://github.com/jp43/lsdmap.git $HOME/lsdmap && python $HOME/lsdmap/setup.py install --user))","export PATH=$PATH:$HOME/lsdmap/bin","chmod +x $HOME/lsdmap/bin/lsdmap"],
            "executable" : ["/bin/bash"]
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
            "executable"  : "LSDMAP",
            "arguments"   : self.get_raw_args(),
            "use_mpi"     : False
        }
