#!/usr/bin/env python

"""A static execution plugin for the 'dummy' pattern.
"""

__author__    = "Ole Weider <ole.weidner@rutgers.edu>"
__copyright__ = "Copyright 2014, http://radical.rutgers.edu"
__license__   = "MIT"

from radical.ensemblemd.execplugins.plugin_base import PluginBase

# ------------------------------------------------------------------------------
# 
_PLUGIN_INFO = {
    "name":         "dummy.dynamic.default",
    "pattern":      "Dummy",
    "context_type": "Dynamic"
}

_PLUGIN_OPTIONS = []


# ------------------------------------------------------------------------------
# 
class Plugin(PluginBase):

    # --------------------------------------------------------------------------
    #
    def __init__(self):

        super(Plugin, self).__init__(_PLUGIN_INFO, _PLUGIN_OPTIONS)
