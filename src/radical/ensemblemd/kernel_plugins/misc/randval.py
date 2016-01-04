#!/usr/bin/env python

"""A kernel that creates a new ASCII file with a given size and name.
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
    "name":         "misc.randval",
    "description":  "Creates a new file containing a single ramdom integer value between 0 and 'upperlimit'.",
    "arguments":   {"--upperlimit=":     
                        {
                        "mandatory": True,
                        "description": "The upper limit of the random value."
                        },
                    "--filename=": 
                        {
                        "mandatory": False,
                        "description": "Output filename."
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
        executable = "/bin/bash"
        if self.get_arg("--filename=")) is not None:
            arguments  = ["-c \"echo $[ 1 + $[ RANDOM % %{0} ]] > %{1}\"".format(
                self.get_arg("--upperlimit="),
                self.get_arg("--filename="))
                ]
        else:
            arguments  = ["-c \"echo $[ 1 + $[ RANDOM % %{0} ]]\"".format(
                self.get_arg("--upperlimit=")]

        return {
            "environment" : None,
            "pre_exec"    : None,
            "executable"  : executable,
            "arguments"   : arguments,
            "use_mpi"     : False
        }
