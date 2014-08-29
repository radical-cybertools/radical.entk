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
    "name":         "misc.mkfile",
    "description":  "Creates a new file of given size and fills it with random ASCII characters.",
    "arguments":   {"--size=":     
                        {
                        "mandatory": True,
                        "description": "File size in bytes."
                        },
                    "--filename=": 
                        {
                        "mandatory": True,
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
        arguments  = ["-c \"base64 /dev/urandom | head -c {0} > {1}\"".format(
            self.get_arg("--size="),
            self.get_arg("--filename="))
        ]

        return {
            "environment" : None,
            "pre_exec"    : None,
            "post_exec"   : None,
            "executable"  : executable,
            "arguments"   : arguments,
            "use_mpi"     : False
        }
