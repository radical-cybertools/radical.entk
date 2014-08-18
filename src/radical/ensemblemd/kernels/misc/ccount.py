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
    "name":         "misc.ccount",
    "description":  "Counts the character frequency in an ASCII file.",
    "arguments":   {"--inputfile=":     
                        {
                        "mandatory": True,
                        "description": "The input ASCII file."
                        },
                    "--outputfile=":     
                        {
                        "mandatory": True,
                        "description": "The output file containing the character counts."
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
    def _get_kernel_description(self):
        """(PRIVATE) Implements parent class method. Returns the kernel
           description as a dictionary.
        """
        executable = "/bin/bash" 
        arguments  = ["-c \"grep -o . {0} | sort | uniq -c > {1}\"".format(
            self.get_arg("--inputfile="),
            self.get_arg("--outputfile="))
        ]

        return {
            "environment" : None,
            "pre_exec"    : None,
            "post_exec"   : None,
            "executable"  : executable,
            "arguments"   : arguments,
            "use_mpi"     : False
        }

