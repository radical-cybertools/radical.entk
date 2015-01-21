#!/usr/bin/env python

"""A static execution plugin RE pattern 1
For this pattern exchange is synchronous - all replicas must finish MD run before
an exchange can take place and all replicas must participate. Exchange is performed
in centralized way (not on compute)
"""

__author__    = "Antons Treikalis <antons.treikalis@rutgers.edu>"
__copyright__ = "Copyright 2014, http://radical.rutgers.edu"
__license__   = "MIT"

import os
import random
import datetime
import radical.pilot

from radical.ensemblemd.exceptions import NotImplementedError, EnsemblemdError
from radical.ensemblemd.exec_plugins.plugin_base import PluginBase

# ------------------------------------------------------------------------------
#
_PLUGIN_INFO = {
    "name":         "replica_exchange.static_pattern_1",
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
    def verify_pattern(self, pattern, resource):
        """
        """
        # THROW ERRROR IF PROFILING IS NOT IMPLEMENTED TO AVOID
        # FRUSTARTION AT THE NED
        #do_profile = os.getenv('RADICAL_ENMD_PROFILING', '0')

        #if do_profile != '0':
        #    # add profiling code here
        #    raise EnsemblemdError("RADICAL_ENMD_PROFILING set but profiling is not implemented for this pattern yet.")

    # --------------------------------------------------------------------------
    #
    def execute_pattern(self, pattern, resource):

        replicas = pattern.get_replicas()

        # performance data structure
        dictionary = {}

        for c in range(pattern.nr_cycles):

            dictionary["cycle_{0}".format(c+1)] = {}

            for r in replicas:

                dictionary["cycle_{0}".format(c+1)]["replica_{0}".format(r.id)] = {}
                current_entry = dictionary["cycle_{0}".format(c+1)]["replica_{0}".format(r.id)]

                self.get_logger().info("Building input files for replica %d" % r.id)
                pattern.build_input_file(r)
                self.get_logger().info("Preparing replica %d for MD run" % r.id)
                r_kernel = pattern.prepare_replica_for_md(r)
                r_kernel._bind_to_resource(resource._resource_key)

                cu = radical.pilot.ComputeUnitDescription()
                cu.pre_exec = r_kernel._cu_def_pre_exec
                cu.executable     = r_kernel._cu_def_executable
                cu.arguments      = r_kernel.arguments
                cu.mpi            = r_kernel.uses_mpi
                cu.cores          = r_kernel.cores
                cu.input_staging  = r_kernel._cu_def_input_data
                cu.output_staging = r_kernel._cu_def_output_data

                current_entry["unit_description"] = cu
                current_entry["compute_unit"] = None

                sub_replica = resource._umgr.submit_units(cu)

                replica_key = "replica_%s" % r.id
                cycle_key = "cycle_%s" % (c+1)
                dictionary[cycle_key][replica_key]["compute_unit"] = sub_replica

            self.get_logger().info("Performing MD step for replicas")
            resource._umgr.wait_units()

            if (c < (pattern.nr_cycles-1)):

                # computing swap matrix
                self.get_logger().info("Computing swap matrix")
                swap_matrix = pattern.get_swap_matrix(replicas)

                # this is actual exchange
                for r_i in replicas:
                    r_j = pattern.exchange(r_i, replicas, swap_matrix)
                    if (r_j != r_i):
                        # swap parameters
                        self.get_logger().info("Performing exchange of parameters between replica %d and replica %d" % ( r_j.id, r_i.id ))
                        pattern.perform_swap(r_i, r_j)

        self.get_logger().info("Replica Exchange simulation finished successfully!")
        self.get_logger().info("Deallocating resource.")
        resource.deallocate()
        
        # --------------------------------------------------------------------------
        # If profiling is enabled, we write the profiling data to a file

        do_profile = os.getenv('RADICAL_ENMD_PROFILING', '0')

        if do_profile != '0':

            outfile = "execution_profile_{time}.csv".format(time=datetime.datetime.now().isoformat())
            self.get_logger().info("Saving execution profile in {outfile}".format(outfile=outfile))

            with open(outfile, 'w+') as f:
                # General format of a profiling file is row based and follows the
                # structure <unit id>; <s_time>; <stop_t>; <tag1>; <tag2>; ...
                head = "cu_id; start_time; stop_time; cycle; replica_id"
                f.write("{row}\n".format(row=head))

                for cycle in dictionary.keys():
                    for replica in dictionary[cycle].keys():
                        data = dictionary[cycle][replica]
                        cu = data["compute_unit"]

                        row = "{uid}; {start_time}; {stop_time}; {tags}".format(
                            uid=cu.uid,
                            start_time=cu.start_time,
                            stop_time=cu.stop_time,
                            tags="{cycle}; {replica}".format(cycle=cycle.split('_')[1], replica=replica.split('_')[1])
                        )
                        f.write("{row}\n".format(row=row))
