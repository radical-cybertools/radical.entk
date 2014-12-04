#!/usr/bin/env python

"""A static execution plugin RE pattern 2
For this pattern exchange is synchronous - all replicas must finish MD run before
an exchange can take place and all replicas must participate. Exchange is performed 
on compute
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
    def execute_pattern(self, pattern, resource):

        #####
        # launching pilot
        #####
        session = radical.pilot.Session()

        if resource._username is not None:
            # Add an ssh identity to the session.
            c = radical.pilot.Context('ssh')
            c.user_id = resource._username
            session.add_context(c)

        pmgr = radical.pilot.PilotManager(session=session)

        pdesc = radical.pilot.ComputePilotDescription()
        pdesc.resource = resource._resource_key
        pdesc.runtime  = resource._walltime
        pdesc.cores    = resource._cores
        pdesc.cleanup  = False

        if resource._allocation is not None:
            pdesc.project = resource._allocation

        pilot = pmgr.submit_pilots(pdesc)

        unit_manager = radical.pilot.UnitManager(session=session,scheduler=radical.pilot.SCHED_DIRECT_SUBMISSION)
        unit_manager.add_pilots(pilot)
        #####

        replicas = pattern.get_replicas()

        for i in range(pattern.nr_cycles):
            compute_replicas = []
            for r in replicas:
                self.get_logger().info("Building input files for replica %d" % r.id)
                pattern.build_input_file(r)
                self.get_logger().info("Preparing replica %d for MD run" % r.id)
                r_kernel = pattern.prepare_replica_for_md(r)
                r_kernel._bind_to_resource(resource._resource_key)

                cu                = radical.pilot.ComputeUnitDescription()
                cu.pre_exec       = r_kernel._cu_def_pre_exec
                cu.executable     = r_kernel._cu_def_executable
                cu.arguments      = r_kernel.arguments
                cu.mpi            = r_kernel.uses_mpi
                cu.cores          = r_kernel.cores
                cu.input_staging  = r_kernel._cu_def_input_data
                cu.output_staging = r_kernel._cu_def_output_data
                compute_replicas.append( cu )

            self.get_logger().info("Performing MD step for replicas")
            submitted_replicas = unit_manager.submit_units(compute_replicas)
            unit_manager.wait_units()
            
            if (i < (pattern.nr_cycles-1)):
                #####################################################################
                # computing swap matrix
                #####################################################################
                exchange_replicas = []
                for r in replicas:
                    self.get_logger().info("Preparing replica %d for Exchange run" % r.id)
                    ex_kernel = pattern.prepare_replica_for_exchange(r)
                    ex_kernel._bind_to_resource(resource._resource_key)

                    cu                = radical.pilot.ComputeUnitDescription()
                    cu.pre_exec       = ex_kernel._cu_def_pre_exec
                    cu.executable     = ex_kernel._cu_def_executable
                    cu.arguments      = ex_kernel.arguments
                    cu.mpi            = ex_kernel.uses_mpi
                    cu.cores          = ex_kernel.cores
                    cu.input_staging  = ex_kernel._cu_def_input_data

                    exchange_replicas.append( cu )

                self.get_logger().info("Performing Exchange step for replicas")
                submitted_replicas = unit_manager.submit_units(exchange_replicas)
                unit_manager.wait_units()
 
                matrix_columns = []
                for r in submitted_replicas:
                    d = str(r.stdout)
                    data = d.split()
                    matrix_columns.append(data)

                #####################################################################
                # computing swap matrix
                #####################################################################
                self.get_logger().info("Composing swap matrix")
                swap_matrix = pattern.get_swap_matrix(replicas, matrix_columns)
                            
                # this is actual exchange
                for r_i in replicas:
                    r_j = pattern.exchange(r_i, replicas, swap_matrix)
                    if (r_j != r_i):
                        self.get_logger().info("Performing exchange of parameters between replica %d and replica %d" % ( r_j.id, r_i.id ))
                        # swap parameters               
                        pattern.perform_swap(r_i, r_j)

        self.get_logger().info("Replica Exchange simulation finished successfully!")

