#!/usr/bin/env python

"""A kernel that creates a new ASCII file with a given size and name.
"""

__author__    = "Ole Weider <ole.weidner@rutgers.edu>"
__copyright__ = "Copyright 2014, http://radical.rutgers.edu"
__license__   = "MIT"

from radical.ensemblemd.kernels.kernel_base import KernelBase

# ------------------------------------------------------------------------------
# 
_KERNEL_INFO = {
    "name":         "utils.mkfile",
}

_KERNEL_CONFIG = {
    
    
}

# ------------------------------------------------------------------------------
# 
class Kernel(KernelBase):

    # --------------------------------------------------------------------------
    #
    def __init__(self):
        super(Kernel, self).__init__(_KERNEL_INFO, _KERNEL_CONFIG)

    # --------------------------------------------------------------------------
    #
    def kernel_stuff(self):
        self.get_logger().info("Doing Kernel stuff...")