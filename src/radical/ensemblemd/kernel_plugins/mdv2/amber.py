#!/usr/bin/env python

"""The AMBER molecular dynamics package (http://ambermd.org/).
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
    "name":            "md.amber.simpleapi",
    "description":     "The AMBER molecular dynamics package (http://ambermd.org/) wrapped in a simple, MD command-line API.",
    "arguments":   {"--input=":
                        {
                            "mandatory": True,
                            "description": "Input filename"
                        }
                    },
    "machine_configs": 
    {
        "stampede.tacc.utexas.edu": 
        {
            "environment" : {},
            "pre_exec"    : ["module load TACC && module load amber"],
            "executable"  : ["/bin/bash"]
        },
        "archer.ac.uk":
        {
          "environment" : {},
          "pre_exec" : ["module load packages-archer","module load amber"],
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
            "executable"  : "AMBER",
            "arguments"   : self.get_raw_args(),
            "use_mpi"     : True
        }
