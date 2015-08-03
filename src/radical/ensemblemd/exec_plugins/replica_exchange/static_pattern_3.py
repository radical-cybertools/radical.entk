#!/usr/bin/env python

"""A static execution plugin RE pattern 2
For this pattern exchange is synchronous - all replicas must finish MD run 
before an exchange can take place and all replicas must participate. Exchange 
is performed on compute
"""

__author__    = "Antons Treikalis <antons.treikalis@rutgers.edu>"
__copyright__ = "Copyright 2014, http://radical.rutgers.edu"
__license__   = "MIT"

import os
import sys
import time
import random
import datetime
import radical.pilot
from radical.ensemblemd.utils import extract_timing_info
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
        pass

    # --------------------------------------------------------------------------
    #
    def execute_pattern(self, pattern, resource):
        try:
            try:
                cycles = pattern.nr_cycles+1
            except:
                self.get_logger().exception("Number of cycles (nr_cycles) \
                    must be defined for pattern ReplicaExchange!")
                raise

            do_profile = os.getenv('RADICAL_ENMD_PROFILING', '0')

            if do_profile == '1':
                pattern._execution_profile = []
                all_cus = []
 
            # shared data
            pattern.prepare_shared_data()

            shared_input_file_urls = pattern.shared_urls
            shared_input_files = pattern.shared_files
            sd_shared_list = []

            for i in range(len(shared_input_files)):

                sd_pilot = {'source': shared_input_file_urls[i],
                            'target': 'staging:///%s' % shared_input_files[i],
                            'action': radical.pilot.TRANSFER
                }

                resource._pilot.stage_in(sd_pilot)

                sd_shared = {'source': 'staging:///%s' % shared_input_files[i],
                             'target': shared_input_files[i],
                             'action': radical.pilot.COPY
                }
                sd_shared_list.append(sd_shared)

            # Pilot must be active
            resource._pmgr.wait_pilots(resource._pilot.uid,'Active')       
     
            if do_profile == '1':
                pattern_start_time = datetime.datetime.now()

            replicas = pattern.get_replicas()

            for c in range(1, cycles):
                if do_profile == '1':
                    step_timings = {
                        "name": "md_run_{0}".format(c),
                        "timings": {}
                    }
                    step_start_time_abs = datetime.datetime.now()

                md_units = []
                for r in replicas:

                    #self.get_logger().info("Cycle %d: Building input files for replica %d" % ((c), r.id) )
                    #pattern.build_input_file(r)
                    
                    self.get_logger().info("Cycle %d: Preparing replica %d for MD run" % ((c), r.id) )
                    r_kernel = pattern.prepare_replica_for_md(r)

                    if ((r_kernel._kernel.get_name()) == "md.amber"):
                        r_kernel._bind_to_resource(resource._resource_key, pattern.name)
                    else:
                        r_kernel._bind_to_resource(resource._resource_key)

                    # processing data directives
                    # need means to distinguish between copy and link
                    copy_out = []
                    
                    items_out = r_kernel._kernel._copy_output_data
                    # copy_output_data is not mandatory
                    if items_out:                    
                        for item in items_out:
                            i_out = {
                                'source': item,
                                'target': 'staging:///%s' % item,
                                'action': radical.pilot.COPY
                            }
                            copy_out.append(i_out)

                    cu                = radical.pilot.ComputeUnitDescription()
                    cu.name           = "md ;{cycle} ;{replica}"\
                                        .format(cycle=c, replica=r.id)
                    cu.pre_exec       = r_kernel._cu_def_pre_exec
                    cu.executable     = r_kernel._cu_def_executable
                    cu.arguments      = r_kernel.arguments
                    cu.mpi            = r_kernel.uses_mpi
                    cu.cores          = r_kernel.cores

                    in_list           = []
                    if r_kernel._cu_def_input_data:
                        in_list = in_list + r_kernel._cu_def_input_data
                    if sd_shared_list:
                        in_list = in_list + sd_shared_list
                    cu.input_staging  = in_list

                    out_list = []
                    if r_kernel._cu_def_output_data:
                        out_list = out_list + r_kernel._cu_def_output_data
                    if copy_out:
                        out_list = out_list + copy_out
                    cu.output_staging = out_list

                    sub_replica = resource._umgr.submit_units(cu)
                    md_units.append(sub_replica)                    

                if do_profile == '1':
                    all_cus.extend(md_units)
         
                self.get_logger().info("Cycle %d: Performing MD step for replicas" % (c) )
                resource._umgr.wait_units()

                if do_profile == '1':
                    step_end_time_abs = datetime.datetime.now()          
 
                failed_units = ""
                for unit in md_units:
                    if unit.state != radical.pilot.DONE:
                        failed_units += " * MD step: Unit {0} failed with an error: {1}\n".format(unit.uid, unit.stderr)

                if len(failed_units) > 0:
                    sys.exit()

                if do_profile == '1':
                    # Process CU information and append it to the dictionary
                    if isinstance(pattern_start_time, datetime.datetime):
                        if isinstance(step_start_time_abs, datetime.datetime):
                            if isinstance(step_end_time_abs, datetime.datetime):
                                tinfo = extract_timing_info(md_units, pattern_start_time, step_start_time_abs, step_end_time_abs)
                            else:
                                sys.exit("Ensemble MD Toolkit Error: step_end_time_abs for {0} is not datetime.datetime instance.".format(step_timings["name"]))
                        else:
                            sys.exit("Ensemble MD Toolkit Error: step_start_time_abs for {0} is not datetime.datetime instance.".format(step_timings["name"]))
                    else:
                        sys.exit("Ensemble MD Toolkit Error: pattern_start_time is not datetime.datetime instance.")

                    for key, val in tinfo.iteritems():
                        step_timings['timings'][key] = val

                    # Write the whole thing to the profiling dict
                    pattern._execution_profile.append(step_timings)
                #-----------------------------------------------------------

                if (c <= cycles):
                    if do_profile == '1':
                        step_timings = {
                            "name": "ex_run_{0}".format(c),
                            "timings": {}
                        }
                        step_start_time_abs = datetime.datetime.now()

                    ex_units = []

                    for r in replicas:
                        self.get_logger().info("Cycle %d: Preparing replica %d for Exchange run" % ((c), r.id) )
                        ex_kernel = pattern.prepare_replica_for_exchange(r)
                        ex_kernel._bind_to_resource(resource._resource_key)
                        
                        cu                = radical.pilot.ComputeUnitDescription()
                        cu.name           = "ex ;{cycle} ;{replica}".format(cycle=c, replica=r.id)
                        cu.pre_exec       = ex_kernel._cu_def_pre_exec
                        cu.executable     = ex_kernel._cu_def_executable
                        cu.arguments      = ex_kernel.arguments
                        cu.mpi            = ex_kernel.uses_mpi
                        cu.cores          = ex_kernel.cores
                        cu.input_staging  = ex_kernel._cu_def_input_data
                        cu.output_staging = ex_kernel._cu_def_output_data

                        sub_replica = resource._umgr.submit_units(cu)
                        ex_units.append(sub_replica)

                    self.get_logger().info("Cycle %d: Performing Exchange step for replicas" % (c) )
                    resource._umgr.wait_units()
 
                    if do_profile == '1':
                        step_end_time_abs = datetime.datetime.now()
                        all_cus.extend(ex_units)

                    failed_units = ""
                    for unit in ex_units:
                        if unit.state != radical.pilot.DONE:
                            failed_units += " * EX step: Unit {0} failed with an error: {1}\n".format(unit.uid, unit.stderr)

                    if len(failed_units) > 0:
                        sys.exit()

                    if do_profile == '1':
                        # Process CU information and append it to the dictionary
                        if isinstance(pattern_start_time, datetime.datetime):
                            if isinstance(step_start_time_abs, datetime.datetime):
                                if isinstance(step_end_time_abs, datetime.datetime):
                                    tinfo = extract_timing_info(ex_units, pattern_start_time, step_start_time_abs, step_end_time_abs)
                                else:
                                    sys.exit("Ensemble MD Toolkit Error: step_end_time_abs for {0} is not datetime.datetime instance.".format(step_timings["name"]))
                            else:
                                sys.exit("Ensemble MD Toolkit Error: step_start_time_abs for {0} is not datetime.datetime instance.".format(step_timings["name"]))
                        else:
                            sys.exit("Ensemble MD Toolkit Error: pattern_start_time is not datetime.datetime instance.")

                        for key, val in tinfo.iteritems():
                            step_timings['timings'][key] = val

                        # Write the whole thing to the profiling dict
                        pattern._execution_profile.append(step_timings)

                        step_timings = {
                            "name": "post_processing_{0}".format(c),
                            "timings": {}
                        }
                        step_start_time_abs = datetime.datetime.now()
                    #-----------------------------------------------------------
                    matrix_columns = pattern.build_swap_matrix(replicas)

                    #matrix_columns = []
                    #for r in ex_units:
                    #    d = str(r.stdout)
                    #    data = d.split()
                    #    matrix_columns.append(data)

                    # writing swap matrix out
                    sw_file = "matrix_columns_" + str(c)
                    try:
                        w_file = open( sw_file, "w")
                        for i in matrix_columns:
                            for j in i:
                                w_file.write("%s " % j)
                            w_file.write("\n")
                        w_file.close()
                    except IOError:
                        self.get_logger().info('Warning: unable to access file %s' % sw_file)

                    # computing swap matrix
                    self.get_logger().info("Cycle %d: Composing swap matrix" % (c) )

                    # this is actual exchange
                    for r_i in replicas:
                        r_j = pattern.exchange(r_i, replicas, matrix_columns)
                        if (r_j != r_i):
                            self.get_logger().info("Performing exchange of parameters between replica %d and replica %d" % ( r_j.id, r_i.id ))
                            # swap parameters
                            pattern.perform_swap(r_i, r_j)

                    if do_profile == '1':
                        step_end_time_abs = datetime.datetime.now()

                        # processing timings
                        step_start_time_rel = step_start_time_abs - pattern_start_time
                        step_end_time_rel = step_end_time_abs - pattern_start_time

                        tinfo = {
                                    "step_start_time": {
                                        "abs": step_start_time_abs,
                                        "rel": step_start_time_rel
                                    },
                                    "step_end_time": {
                                        "abs": step_end_time_abs,
                                        "rel": step_end_time_rel
                                    }
                                }

                        for key, val in tinfo.iteritems():
                            step_timings['timings'][key] = val

                        # Write the whole thing to the profiling dict
                        pattern._execution_profile.append(step_timings)
                    #------------------------------------------------------------------    
    
            # End of simulation loop
            #------------------------
            
        except Exception, ex:
            self.get_logger().exception("Fatal error during execution: {0}.".format(str(ex)))
            raise

        self.get_logger().info("Replica Exchange simulation finished successfully!")
        self.get_logger().info("Deallocating resource.")
        resource.deallocate()

