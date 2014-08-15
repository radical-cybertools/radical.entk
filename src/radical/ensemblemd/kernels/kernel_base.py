#!/usr/bin/env python

"""Defines and implements the abstract kernel base class.
"""

__author__    = "Ole Weider <ole.weidner@rutgers.edu>"
__copyright__ = "Copyright 2014, http://radical.rutgers.edu"
__license__   = "MIT"

import radical.utils         as ru
import radical.utils.config  as ruc
import radical.utils.logger  as rul

from radical.ensemblemd.exceptions import NotImplementedError


# ------------------------------------------------------------------------------
# plugin base class
#
class KernelBase() :

    __metaclass__ = ru.Singleton
    
    # --------------------------------------------------------------------------
    #
    def __init__ (self, kernel_info, kernel_config=[]) :

        self._info    = kernel_info
        self._name    = kernel_info['name']
        self._conf    = kernel_config

        self._lock    = ru.RLock      (self._name)
        self._logger  = rul.getLogger ('radical.enmd.', self._name)

    # --------------------------------------------------------------------------
    #
    def register(self) :
        """ Kernel registration function. The engine calls this during startup
            to retrieve the adaptor information.
        """

        return self._info

    # --------------------------------------------------------------------------
    #
    def get_name (self) :
        return self._name

    # --------------------------------------------------------------------------
    #
    def get_info (self) :
        return self._info

    # --------------------------------------------------------------------------
    #
    def get_logger(self):
        return self._logger

    # --------------------------------------------------------------------------
    #
    def kernel_stuff(self):
        """Verify the pattern.
        """
        raise NotImplementedError(
          method_name="kernel_stuff",
          class_name=type(self))
