#!/usr/bin/env python

"""Defines and implements the abstract kernel base class.
"""

__author__    = "Vivek Balasubramanian <vivek.balasubramanian@rutgers.edu>"
__copyright__ = "Copyright 2016, http://radical.rutgers.edu"
__license__   = "MIT"

from radical.entk.exceptions import *
import radical.utils as ru

# ------------------------------------------------------------------------------
# Kernel format
"""
_KERNEL_INFO = {
    "name":         "kernel name",
    "description":  "Description about kernel",
    "arguments":   {
                "--arg1=":
                {
                    "mandatory": False,
                    "description": "argument description"
                }
            },
    "machine_configs": 
            {
                "resource_name": {
                    "pre_exec"      : [],
                    "executable"    : "",
                    "uses_mpi"      : False
                },
            }
    }
"""
#  -------------------------------------------------------------

#  -------------------------------------------------------------
# plugin base class
#
class KernelBase(object):
    
    def __init__ (self, kernel_info) :

        self._kernel_info     = kernel_info
        self._kernel_name     = kernel_info['name']

        if 'description' in kernel_info:
            self._kernel_description  = kernel_info['description']

        self._raw_args          = kernel_info["arguments"]
        self._machine_configs   = kernel_info["machine_configs"]

        # Parameters required for any Kernel irrespective of RP
        self._logger = ru.get_logger("radical.entk.kernel_base.%s"%(self._kernel_name))
        self._logger.debug("KernelBase instantiated")
    #  -------------------------------------------------------------
    
    def as_dict(self):
        """Returns a dictionary representation of the kernel"""
    
        kernel_dict = {     "name":     self._kernel_name,
                            "pre_exec":     self._pre_exec,
                            "executable":     self._executable,
                            "arguments":     self._arguments,                 
                        }

        return kernel_dict
    # ------------------------------------------------------------    
    @property
    def kernel_info(self):
        return self._kernel_info
    
    def get_kernel_info (self) :
        return self._kernel_info
    # -------------------------------------------------------------
    @property
    def raw_args(self):
        """Returns all arguments as they were passed to the kernel.
        """
        return self._raw_args
    # -------------------------------------------------------------

    # -------------------------------------------------------------

    def _bind_to_resource(self, resource_key, pattern_name=None):
        """Binds the kernel to a specific resource.
        """
        raise NotImplementedError(
          method_name="_get_kernel_description",
          class_name=type(self))
    # -------------------------------------------------------------