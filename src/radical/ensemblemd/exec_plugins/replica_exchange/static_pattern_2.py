#!/usr/bin/env python

"""A static execution plugin RE pattern 2
"""

__author__    = "Antons Treikalis <antons.treikalis@rutgers.edu>"
__copyright__ = "Copyright 2014, http://radical.rutgers.edu"
__license__   = "MIT"

import os 
import random
import radical.pilot

from radical.ensemblemd.exec_plugins.plugin_base import PluginBase

# ------------------------------------------------------------------------------
# 
_PLUGIN_INFO = {
    "name":         "replica_exchange.static_pattern_2",
    "pattern":      "ReplicaExchange",
    "context_type": "Static"
}

_PLUGIN_OPTIONS = []


# ------------------------------------------------------------------------------
# 
class Plugin(PluginBase):

    # --------------------------------------------------------------------------
    #
    def __init__(self):
        super(Plugin, self).__init__(_PLUGIN_INFO, _PLUGIN_OPTIONS)

    # --------------------------------------------------------------------------
    #
    def verify_pattern(self, pattern):
        """
        """
        self.get_logger().info("Pattern workload verification passed.")

    # --------------------------------------------------------------------------
    #
    def execute_pattern(self, pattern):

        # LAUNCHING A PILOT HERE...

        for i in range(pattern.nr_cycles):
            compute_replicas = []
            for r in replicas:
                comp_r = pattern.prepare_replica_for_md(r)
                compute_replicas.append( comp_r )

            self.get_logger().info("Performing MD step for replicas")
            # SUBMITTING TO UNIT MANAGER HERE...
            
            if (i != (pattern.nr_cycles-1)):
                #####################################################################
                # computing swap matrix
                #####################################################################
                exchange_replicas = []
                for r in replicas:
                    ex_r = pattern.prepare_replica_for_exchange(r)
                    exchange_replicas.append( ex_r )

                self.get_logger().info("Performing Exchange step for replicas")
                # SUBMITTING TO UNIT MANAGER HERE...

                #####################################################################
                # compose swap matrix from individual files
                #####################################################################
                self.get_logger().info("Composing swap matrix from individual files")
                swap_matrix = pattern.compose_swap_matrix(replicas)
            
                # this is actual exchnage
                for r_i in replicas:
                    #rj = random.choice( replicas )
                    r_j = pattern.exchange(r_i, replicas, swap_matrix)
                    if (r_j != r_i):
                        # swap temperatures                    
                        temperature = r_j.new_temperature
                        r_j.new_temperature = r_i.new_temperature
                        r_i.new_temperature = temperature
                        # record that swap was performed
                        r_i.swap = 1
                        r_j.swap = 1

