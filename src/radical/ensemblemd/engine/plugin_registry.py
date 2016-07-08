#!/usr/bin/env python

"""List of all registered execution plugins.

This registry is used to locate and load plugins. The entries must be
formatted in dotted python module notation.
"""

__author__    = "Vivek Balasubramanian <vivek.balasubramanian@rutgers.edu>"
__copyright__ = "Copyright 2014, http://radical.rutgers.edu"
__license__   = "MIT"


plugin_registry = [ "radical.ensemblemd.exec_plugins.pipeline.static",
                    "radical.ensemblemd.exec_plugins.simulation_analysis_loop.static",
                    "radical.ensemblemd.exec_plugins.replica_exchange.static_pattern_1",
                    "radical.ensemblemd.exec_plugins.replica_exchange.static_pattern_2",
                    "radical.ensemblemd.exec_plugins.replica_exchange.static_pattern_3",
                    "radical.ensemblemd.exec_plugins.allpairs.static",
                    "radical.ensemblemd.exec_plugins.bag_of_tasks.static"
                  ]
