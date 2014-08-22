#!/usr/bin/env python

"""List of all registered execution plugins.

This registry is used to locate and load plugins. The entries must be
formatted in dotted python module notation.
"""

__author__    = "Ole Weider <ole.weidner@rutgers.edu>"
__copyright__ = "Copyright 2014, http://radical.rutgers.edu"
__license__   = "MIT"


plugin_registry = [ "radical.ensemblemd.execplugins.ensemble.static",
                    "radical.ensemblemd.execplugins.pipeline.static",
                    "radical.ensemblemd.execplugins.simulation_analysis.static"
                  ]
