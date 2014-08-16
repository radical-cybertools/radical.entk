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
    def create_cu_description(self, args):

        executable = "/bin/bash"
        arguments  = ["-c \"base64 /dev/urandom | head -c {0} > {1}\"".format(
            self.get_args("--size="),
            self.get_args("--filename=")
        )]

        self.get_logger().info("Created %s" % arguments)
