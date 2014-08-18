#!/usr/bin/env python

"""A kernel that creates a new ASCII file with a given size and name.
"""

__author__    = "Ole Weider <ole.weidner@rutgers.edu>"
__copyright__ = "Copyright 2014, http://radical.rutgers.edu"
__license__   = "MIT"

import sys
from copy import deepcopy

from radical.ensemblemd.exceptions import ArgumentError
from radical.ensemblemd.kernels.kernel_base import KernelBase

# ------------------------------------------------------------------------------
# 
_KERNEL_INFO = {
    "name":         "misc.chksum",
    "description":  "Calculates an SHA1 checksum for a given file.",
    "arguments":   {"--inputfile=":     
                        {
                        "mandatory": True,
                        "description": "The input file."
                        },
                    "--outputfile=":     
                        {
                        "mandatory": True,
                        "description": "The output file containing SHA1 sum."
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
        if sys.platform == 'darwin':
            # On OS X (all BSDs?) the tool is called shasum
            chksum_tool = 'shasum'
        else:
            # On Linux the tool is called sha1sum
            chksum_tool = 'sha1sum'

        executable = "/bin/bash"
        arguments  = ["-c \"{0} {1} > {2}\"".format(
            chksum_tool,
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
