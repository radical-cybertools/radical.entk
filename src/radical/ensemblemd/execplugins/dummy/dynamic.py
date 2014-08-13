#!/usr/bin/env python

"""A static execution plugin for the 'dummy' pattern.
"""

__author__    = "Ole Weider <ole.weidner@rutgers.edu>"
__copyright__ = "Copyright 2014, http://radical.rutgers.edu"
__license__   = "MIT"

from radical.ensemblemd.execplugins.plugin_base import PluginBase

# ------------------------------------------------------------------------------
# 
_ADAPTOR_INFO = {
    "name":         "dummy.dynamic.default",
    "pattern":      "dummy",
    "context_type": "dynamic"
}

_ADAPTOR_OPTIONS = []


# ------------------------------------------------------------------------------
# 
class Adaptor(PluginBase):

    # --------------------------------------------------------------------------
    #
    def __init__(self):

        super(Adaptor, self).__init__(_ADAPTOR_INFO, _ADAPTOR_OPTIONS)
