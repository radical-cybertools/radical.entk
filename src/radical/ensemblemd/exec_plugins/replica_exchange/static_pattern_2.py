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
import datetime
import radical.pilot

from radical.ensemblemd.exceptions import NotImplementedError, EnsemblemdError
from radical.ensemblemd.exec_plugins.plugin_base import PluginBase

# ------------------------------------------------------------------------------
#
_PLUGIN_INFO = {
    "name":         "replica_exchange.static_pattern_2",
    "pattern":      "ReplicaExchange",
    "context_type": "Static"
}

_PLUGIN_OPTIONS = []

STAGING_AREA = 'staging_area'

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
        do_profile = os.getenv('RADICAL_ENDM_PROFILING', '0')

        if do_profile != '0':
            # add profiling code here
            raise EnsemblemdError("RADICAL_ENDM_PROFILING set but profiling is not implemented for this pattern yet.")

    # --------------------------------------------------------------------------
    #
    def execute_pattern(self, pattern, resource):
        try:
            # launching pilot
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

            if resource._queue is not None:
                pdesc.queue = resource._queue

            pdesc.cleanup  = resource._cleanup

            if resource._allocation is not None:
                pdesc.project = resource._allocation

            pilot = pmgr.submit_pilots(pdesc)

            # shared data
            pattern.prepare_shared_data()

            shared_input_file_urls = pattern.get_shared_urls()
            shared_input_files = pattern.get_shared_files()
            sd_shared_list = []

            for i in range(len(shared_input_files)):

                sd_pilot = {'source': shared_input_file_urls[i],
                            'target': 'staging:///%s' % shared_input_files[i],
                            'action': radical.pilot.TRANSFER
                }

                pilot.stage_in(sd_pilot)

                sd_shared = {'source': 'staging:///%s' % shared_input_files[i],
                             'target': shared_input_files[i],
                             'action': radical.pilot.LINK
                }
                sd_shared_list.append(sd_shared)

            unit_manager = radical.pilot.UnitManager(session=session,scheduler=radical.pilot.SCHED_BACKFILLING)
            unit_manager.add_pilots(pilot)

            replicas = pattern.get_replicas()

            # performance data structure
            dictionary = {}

            for c in range(pattern.nr_cycles):

                dictionary["cycle_{0}".format(c+1)] = {}
                for r in replicas:

                    dictionary["cycle_{0}".format(c+1)]["replica.md_{0}".format(r.id)] = {}
                    current_entry = dictionary["cycle_{0}".format(c+1)]["replica.md_{0}".format(r.id)]

                    self.get_logger().info("Cycle %d: Building input files for replica %d" % ((c+1), r.id) )
                    pattern.build_input_file(r)
                    self.get_logger().info("Cycle %d: Preparing replica %d for MD run" % ((c+1), r.id) )
                    r_kernel = pattern.prepare_replica_for_md(r)

                    if ((r_kernel._kernel.get_name()) == "md.amber"):
                        r_kernel._bind_to_resource(resource._resource_key, pattern.name)
                    else:
                        r_kernel._bind_to_resource(resource._resource_key)

                    cu                = radical.pilot.ComputeUnitDescription()
                    cu.pre_exec       = r_kernel._cu_def_pre_exec
                    cu.executable     = r_kernel._cu_def_executable
                    cu.arguments      = r_kernel.arguments
                    cu.mpi            = r_kernel.uses_mpi
                    cu.cores          = r_kernel.cores
                    cu.input_staging  = sd_shared_list + r_kernel._cu_def_input_data
                    cu.output_staging = r_kernel._cu_def_output_data

                    current_entry["unit_description"] = cu
                    current_entry["compute_unit"] = None

                    sub_replica = unit_manager.submit_units(cu)

                    replica_key = "replica.md_%s" % r.id
                    cycle_key = "cycle_%s" % (c+1)
                    dictionary[cycle_key][replica_key]["compute_unit"] = sub_replica

                self.get_logger().info("Cycle %d: Performing MD step for replicas" % (c+1) )
                unit_manager.wait_units()

                if (c < (pattern.nr_cycles-1)):

                    submitted_replicas = []
                    # computing swap matrix
                    for r in replicas:

                        dictionary["cycle_{0}".format(c+1)]["replica.ex_{0}".format(r.id)] = {}
                        current_entry = dictionary["cycle_{0}".format(c+1)]["replica.ex_{0}".format(r.id)]

                        self.get_logger().info("Cycle %d: Preparing replica %d for Exchange run" % ((c+1), r.id) )
                        ex_kernel = pattern.prepare_replica_for_exchange(r)
                        ex_kernel._bind_to_resource(resource._resource_key)

                        cu                = radical.pilot.ComputeUnitDescription()
                        cu.pre_exec       = ex_kernel._cu_def_pre_exec
                        cu.executable     = ex_kernel._cu_def_executable
                        cu.arguments      = ex_kernel.arguments
                        cu.mpi            = ex_kernel.uses_mpi
                        cu.cores          = ex_kernel.cores
                        cu.input_staging  = ex_kernel._cu_def_input_data
                        cu.output_staging = ex_kernel._cu_def_output_data

                        current_entry["unit_description"] = cu
                        current_entry["compute_unit"] = None

                        sub_replica = unit_manager.submit_units(cu)
                        submitted_replicas.append(sub_replica)

                        replica_key = "replica.ex_%s" % r.id
                        cycle_key = "cycle_%s" % (c+1)
                        dictionary[cycle_key][replica_key]["compute_unit"] = sub_replica

                    self.get_logger().info("Cycle %d: Performing Exchange step for replicas" % (i+1) )
                    unit_manager.wait_units()

                    matrix_columns = []
                    for r in submitted_replicas:
                        d = str(r.stdout)
                        data = d.split()
                        matrix_columns.append(data)

                    # computing swap matrix
                    self.get_logger().info("Cycle %d: Composing swap matrix" % (c+1) )
                    swap_matrix = pattern.get_swap_matrix(replicas, matrix_columns)

                    # this is actual exchange
                    for r_i in replicas:
                        r_j = pattern.exchange(r_i, replicas, swap_matrix)
                        if (r_j != r_i):
                            self.get_logger().info("Performing exchange of parameters between replica %d and replica %d" % ( r_j.id, r_i.id ))
                            # swap parameters
                            pattern.perform_swap(r_i, r_j)

        except Exception, ex:
            self.get_logger().exception("Fatal error during execution: {0}.".format(str(ex)))
            raise

        # --------------------------------------------------------------------------
        # If profiling is enabled, we write the profiling data to a file

        do_profile = os.getenv('RADICAL_ENMD_PROFILING', '0')

        if do_profile != '0':

            outfile = "execution_profile_{time}.csv".format(time=datetime.datetime.now().isoformat())
            self.get_logger().info("Saving execution profile in {outfile}".format(outfile=outfile))

            with open(outfile, 'w+') as f:
                # General format of a profiling file is row based and follows the
                # structure <unit id>; <s_time>; <stop_t>; <tag1>; <tag2>; ...
                head = "cu_id; start_time; stop_time; cycle; replica_md"
                f.write("{row}\n".format(row=head))

                # writing MD times
                for cycle in dictionary.keys():
                    for replica in dictionary[cycle].keys():
                        if replica.startswith('replica.md'):
                            data = dictionary[cycle][replica]
                            cu = data["compute_unit"]

                            row = "{uid}; {start_time}; {stop_time}; {tags}".format(
                                uid=cu.uid,
                                start_time=cu.start_time,
                                stop_time=cu.stop_time,
                                tags="{cycle}; {replica}".format(cycle=cycle.split('_')[1], replica=replica.split('_')[1])
                            )
                            f.write("{row}\n".format(row=row))

                head = "cu_id; start_time; stop_time; cycle; replica_ex"
                f.write("{row}\n".format(row=head))

                # writing exchange times
                for cycle in dictionary.keys():
                    for replica in dictionary[cycle].keys():
                        if replica.startswith('replica.ex'):
                            data = dictionary[cycle][replica]
                            cu = data["compute_unit"]

                            row = "{uid}; {start_time}; {stop_time}; {tags}".format(
                                uid=cu.uid,
                                start_time=cu.start_time,
                                stop_time=cu.stop_time,
                                tags="{cycle}; {replica}".format(cycle=cycle.split('_')[1], replica=replica.split('_')[1])
                            )
                            f.write("{row}\n".format(row=row))

        #---------------------------------------------------------------------------

        self.get_logger().info("Replica Exchange simulation finished successfully!")

        self.get_logger().info("closing session")
        session.close (cleanup=False)
