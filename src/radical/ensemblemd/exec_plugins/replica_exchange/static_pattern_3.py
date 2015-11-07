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
import traceback
import radical.pilot
from radical.ensemblemd.exceptions import NotImplementedError, EnsemblemdError
from radical.ensemblemd.exec_plugins.plugin_base import PluginBase

# ------------------------------------------------------------------------------
#
_PLUGIN_INFO = {
    "name":         "replica_exchange.static_pattern_3",
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

            #-------------------------------------------------------------------
            t4 = datetime.datetime.utcnow()
            outfile = pattern.workdir_local + "/enmd-core-overhead.csv"
            try:
                f = open(outfile, 'a')

                row = "ENMD-core-t-inside cluster.run() call: {0}".format(t4)
                f.write("{row}\n".format(row=row))

                f.close()
            except IOError:
                print 'Warning: unable to access file %s' % outfile
                raise
            #-------------------------------------------------------------------

            do_profile = os.getenv('RADICAL_ENMD_PROFILING', '0')

            if do_profile == '1':
                cu_performance_data = {}
                step_performance_data = {}
 
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
                pattern_start_time = datetime.datetime.utcnow()

            replicas = pattern.get_replicas()

            #-------------------------------------------------------------------
            # GL = 0: submit global calculator before
            # GL = 1: submit global calculator after
            GL = 1

            for c in range(1, cycles):
                if do_profile == '1':
                    step_start_time_abs = datetime.datetime.utcnow()

                    cu_performance_data['cycle_{0}'.format(c)] = {}
                    cu_performance_data['cycle_{0}'.format(c)]['md_step'] = {}

                    step_performance_data['cycle_{0}'.format(c)] = {}
                    step_performance_data['cycle_{0}'.format(c)]['md_step'] = {}
                    step_performance_data['cycle_{0}'.format(c)]['md_step']['step_start_time_abs']         = {}
                    step_performance_data['cycle_{0}'.format(c)]['md_step']['enmd_ov_step_start_time_abs'] = {}
                    step_performance_data['cycle_{0}'.format(c)]['md_step']['step_start_time_abs']         = step_start_time_abs
                    step_performance_data['cycle_{0}'.format(c)]['md_step']['enmd_ov_step_start_time_abs'] = step_start_time_abs

                md_units = []
                cus = []
                for r in replicas:

                    self.get_logger().info("Cycle %d: Preparing replica %d for MD-step" % ((c), r.id) )
                    r_kernel = pattern.prepare_replica_for_md(r)

                    if ((r_kernel._kernel.get_name()) == "md.amber"):
                        r_kernel._bind_to_resource(resource._resource_key, pattern.name)
                    else:
                        r_kernel._bind_to_resource(resource._resource_key)

                    # processing data directives
                    # need means to distinguish between copy and link
                    #-----------------------------------------------------------
                    copy_out = []
                    items_out = r_kernel._kernel._copy_output_data
                    if items_out:                    
                        for item in items_out:
                            i_out = {
                                'source': item,
                                'target': 'staging:///%s' % item,
                                'action': radical.pilot.COPY
                            }
                            copy_out.append(i_out)

                    #-----------------------------------------------------------
                    copy_in = []
                    items_in = r_kernel._kernel._copy_input_data
                    if items_in:                    
                        for item in items_in:
                            i_in = {
                                'source': 'staging:///%s' % item,
                                'target': item,
                                'action': radical.pilot.COPY
                            }
                            copy_in.append(i_in)
                            
                    #-----------------------------------------------------------
                    cu                = radical.pilot.ComputeUnitDescription()
                    cu.name           = "md ;{cycle} ;{replica}"\
                                        .format(cycle=c, replica=r.id)

                    cu.pre_exec       = r_kernel._cu_def_pre_exec
                    cu.executable     = r_kernel._cu_def_executable
                    cu.post_exec       = r_kernel._cu_def_post_exec
                    cu.arguments      = r_kernel.arguments
                    cu.mpi            = r_kernel.uses_mpi
                    cu.cores          = r_kernel.cores
                    #-----------------------------------------------------------
                    in_list = []
                    if r_kernel._cu_def_input_data:
                        in_list = in_list + r_kernel._cu_def_input_data
                    if copy_in:
                        in_list = in_list + copy_in
                    cu.input_staging  = in_list
                    #-----------------------------------------------------------
                    out_list = []
                    if r_kernel._cu_def_output_data:
                        out_list = out_list + r_kernel._cu_def_output_data
                    if copy_out:
                        out_list = out_list + copy_out
                    cu.output_staging = out_list
                    #-----------------------------------------------------------
                    cus.append(cu)
               
                if do_profile == '1':
                    enmd_ov_step_end_time_abs = datetime.datetime.utcnow()
                    step_performance_data['cycle_{0}'.format(c)]['md_step']['enmd_ov_step_end_time_abs'] = {}
                    step_performance_data['cycle_{0}'.format(c)]['md_step']['enmd_ov_step_end_time_abs'] = enmd_ov_step_end_time_abs
                    step_performance_data['cycle_{0}'.format(c)]['md_step']['enmd_ov_duration'] = {}
                    step_performance_data['cycle_{0}'.format(c)]['md_step']['enmd_ov_duration'] = (enmd_ov_step_end_time_abs - step_start_time_abs).total_seconds() 

                # bulk submission
                sub_replicas = resource._umgr.submit_units(cus)  
                for r in sub_replicas:
                    md_units.append(r)                 

                self.get_logger().info("Cycle %d: Performing MD-step for replicas" % (c) )

                resource._umgr.wait_units()
                #uids = [cu.uid for cu in cus]

                if do_profile == '1':
                    step_end_time_abs = datetime.datetime.utcnow()
                    step_performance_data['cycle_{0}'.format(c)]['md_step']['step_end_time_abs'] = {}
                    step_performance_data['cycle_{0}'.format(c)]['md_step']['step_end_time_abs'] =  step_end_time_abs
                    step_performance_data['cycle_{0}'.format(c)]['md_step']['duration'] = {}
                    step_performance_data['cycle_{0}'.format(c)]['md_step']['duration'] = (step_end_time_abs - step_start_time_abs).total_seconds() 

                    for cu in sub_replicas:    
                        cu_performance_data['cycle_{0}'.format(c)]['md_step']["cu.uid_{0}".format(cu.uid)] = cu 
 
                #---------------------------------------------------------------
                failed_units = ""
                for unit in md_units:
                    if unit.state != radical.pilot.DONE:
                        failed_units += " * MD step: Unit {0} failed with an error: {1}\n".format(unit.uid, unit.stderr)

                if len(failed_units) > 10:
                    sys.exit()

                #---------------------------------------------------------------
                # exchange 
                #---------------------------------------------------------------

                if do_profile == '1':
                    step_start_time_abs = datetime.datetime.utcnow()
                    cu_performance_data['cycle_{0}'.format(c)]['ex_step'] = {}
                    step_performance_data['cycle_{0}'.format(c)]['ex_step'] = {}
                    step_performance_data['cycle_{0}'.format(c)]['ex_step']['step_start_time_abs'] = {}
                    step_performance_data['cycle_{0}'.format(c)]['ex_step']['step_start_time_abs'] = step_start_time_abs
                    step_performance_data['cycle_{0}'.format(c)]['ex_step']['enmd_ov_step_start_time_abs'] = {}
                    step_performance_data['cycle_{0}'.format(c)]['ex_step']['enmd_ov_step_start_time_abs'] = step_start_time_abs
                
                ex_units = []

                self.get_logger().info("Cycle %d: Preparing replicas for Exchange-Step" % (c) )

                gl_ex_kernel = pattern.prepare_global_ex_calc(GL, c, \
                                                                  replicas)
                gl_ex_kernel._bind_to_resource(resource._resource_key)

                cu = radical.pilot.ComputeUnitDescription()

                #---------------------------------------------------------------
                copy_out = []
                    
                items_out = gl_ex_kernel._kernel._copy_output_data
                if items_out:                    
                    for item in items_out:
                        i_out = {
                            'source': item,
                            'target': 'staging:///%s' % item,
                            'action': radical.pilot.COPY
                        }
                        copy_out.append(i_out)
                #---------------------------------------------------------------
                copy_in = []
                    
                items_in = gl_ex_kernel._kernel._copy_input_data
                if items_in:                    
                    for item in items_in:
                        i_in = {
                            'source': 'staging:///%s' % item,
                            'target': item,
                            'action': radical.pilot.COPY
                        }
                        copy_in.append(i_in)
                #---------------------------------------------------------------
                in_list = []
                if gl_ex_kernel._cu_def_input_data:
                    in_list = in_list + gl_ex_kernel._cu_def_input_data
                if copy_in:
                    in_list = in_list + copy_in
                cu.input_staging  = in_list
                #---------------------------------------------------------------
                out_list = []
                if gl_ex_kernel._cu_def_output_data:
                    out_list = out_list + gl_ex_kernel._cu_def_output_data
                if copy_out:
                    out_list = out_list + copy_out
                cu.output_staging = out_list
                #---------------------------------------------------------------
                cu.name           = "gl_ex ;{cycle}".format(cycle=c)
                cu.pre_exec       = gl_ex_kernel._cu_def_pre_exec
                cu.executable     = gl_ex_kernel._cu_def_executable
                cu.post_exec      = gl_ex_kernel._cu_def_post_exec
                cu.arguments      = gl_ex_kernel.arguments
                cu.mpi            = gl_ex_kernel.uses_mpi
                cu.cores          = gl_ex_kernel.cores

                if do_profile == '1':
                    enmd_ov_step_end_time_abs = datetime.datetime.utcnow()
                    step_performance_data['cycle_{0}'.format(c)]['ex_step']['enmd_ov_step_end_time_abs'] = {}
                    step_performance_data['cycle_{0}'.format(c)]['ex_step']['enmd_ov_step_end_time_abs'] = enmd_ov_step_end_time_abs
                    step_performance_data['cycle_{0}'.format(c)]['ex_step']['enmd_ov_duration'] = {}
                    step_performance_data['cycle_{0}'.format(c)]['ex_step']['enmd_ov_duration'] = (enmd_ov_step_end_time_abs - step_start_time_abs).total_seconds()  

                sub_replica = resource._umgr.submit_units(cu)
                resource._umgr.wait_units(sub_replica.uid)

                ex_units.append(sub_replica)
                    
                if do_profile == '1':
                    step_end_time_abs = datetime.datetime.utcnow()
                    step_performance_data['cycle_{0}'.format(c)]['ex_step']['step_end_time_abs'] = {}
                    step_performance_data['cycle_{0}'.format(c)]['ex_step']['step_end_time_abs'] = step_end_time_abs
                    step_performance_data['cycle_{0}'.format(c)]['ex_step']['duration'] = {}
                    step_performance_data['cycle_{0}'.format(c)]['ex_step']['duration'] = (step_end_time_abs - step_start_time_abs).total_seconds()  

                    for cu in ex_units:    
                        cu_performance_data['cycle_{0}'.format(c)]['ex_step']["cu.uid_{0}".format(cu.uid)] = cu 
                    
                failed_units = ""
                for unit in ex_units:
                    if unit.state != radical.pilot.DONE:
                        failed_units += " * EX step: Unit {0} failed with an error: {1}\n".format(unit.uid, unit.stderr)

                #---------------------------------------------------------------
                # post processing
                #---------------------------------------------------------------
                 
                if do_profile == '1':
                    step_start_time_abs = datetime.datetime.utcnow()
                    step_performance_data['cycle_{0}'.format(c)]['pp_step'] = {}
                    step_performance_data['cycle_{0}'.format(c)]['pp_step']['step_start_time_abs'] = {}
                    step_performance_data['cycle_{0}'.format(c)]['pp_step']['step_start_time_abs'] = step_start_time_abs
                    step_performance_data['cycle_{0}'.format(c)]['pp_step']['enmd_ov_step_start_time_abs'] = {}
                    step_performance_data['cycle_{0}'.format(c)]['pp_step']['enmd_ov_step_start_time_abs'] = step_start_time_abs
                
                #---------------------------------------------------------------
                pattern.do_exchange(c, replicas)
                
                if do_profile == '1':
                    step_end_time_abs = datetime.datetime.utcnow()
                    step_performance_data['cycle_{0}'.format(c)]['pp_step']['step_end_time_abs'] = {}
                    step_performance_data['cycle_{0}'.format(c)]['pp_step']['step_end_time_abs'] = step_end_time_abs
                    step_performance_data['cycle_{0}'.format(c)]['pp_step']['enmd_ov_step_end_time_abs'] = {}
                    step_performance_data['cycle_{0}'.format(c)]['pp_step']['enmd_ov_step_end_time_abs'] = step_end_time_abs
                    step_performance_data['cycle_{0}'.format(c)]['pp_step']['duration'] = {}
                    step_performance_data['cycle_{0}'.format(c)]['pp_step']['duration'] = ( step_end_time_abs - step_start_time_abs ).total_seconds()
                    step_performance_data['cycle_{0}'.format(c)]['pp_step']['enmd_ov_duration'] = {}
                    step_performance_data['cycle_{0}'.format(c)]['pp_step']['enmd_ov_duration'] = ( step_end_time_abs - step_start_time_abs ).total_seconds()
                
                #---------------------------------------------------------------    
    
            #-------------------------------------------------------------------
            # End of simulation loop
            #-------------------------------------------------------------------
            outfile = "execution_profile_{mysession}.csv".format(mysession=resource._session.uid)
            with open(outfile, 'w+') as f:

                head = "Cycle; Step; Start; Stop; Duration"
                f.write("{row}\n".format(row=head))

                for cycle in step_performance_data:
                    for step in step_performance_data[cycle].keys():
                        dur = step_performance_data[cycle][step]['duration']
                        start = step_performance_data[cycle][step]['step_start_time_abs']
                        end = step_performance_data[cycle][step]['step_end_time_abs']
                        row = "{Cycle}; {Step}; {Start}; {End}; {Duration}".format(
                            Duration=dur,
                            Cycle=cycle,
                            Step=step,
                            Start=start,
                            End=end)

                        f.write("{r}\n".format(r=row))
                        #-------------------------------------------------------
                        # enmd overhead
                        dur = step_performance_data[cycle][step]['enmd_ov_duration']
                        start = step_performance_data[cycle][step]['enmd_ov_step_start_time_abs']
                        end = step_performance_data[cycle][step]['enmd_ov_step_end_time_abs']
                        row = "{Cycle}; {Step}; {Start}; {End}; {Duration}".format(
                            Duration=dur,
                            Cycle=cycle,
                            Step=step + "_enmd_overhead",
                            Start=start,
                            End=end)

                        f.write("{r}\n".format(r=row))

                #---------------------------------------------------------------
                head = "CU_ID; Scheduling; StagingInput; Allocating; Executing; StagingOutput; Done; Cycle; Step;"
                f.write("{row}\n".format(row=head))
            
                for cycle in cu_performance_data:
                    for step in cu_performance_data[cycle].keys():
                        for cid in cu_performance_data[cycle][step].keys():
                            cu = cu_performance_data[cycle][step][cid]
                            st_data = {}
                            for st in cu.state_history:
                                st_dict = st.as_dict()
                                st_data["{0}".format( st_dict["state"] )] = {}
                                st_data["{0}".format( st_dict["state"] )] = st_dict["timestamp"]

                            #print st_data
                            #start = step_performance_data[cycle][step]['step_start_time_abs']
                            if 'StagingOutput' not in st_data:
                                st_data['StagingOutput'] = st_data['Executing']

                            if 'Done' not in st_data:
                                st_data['Done'] = st_data['Executing']

                            row = "{uid}; {Scheduling}; {StagingInput}; {Allocating}; {Executing}; {StagingOutput}; {Done}; {Cycle}; {Step}".format(
                                uid=cu.uid,
                                Scheduling=(st_data['Scheduling']),
                                StagingInput=(st_data['StagingInput']),
                                Allocating=(st_data['Allocating']),
                                Executing=(st_data['Executing']),
                                StagingOutput=(st_data['StagingOutput']),
                                Done=(st_data['Done']),
                                Cycle=cycle,
                                Step=step)
                        
                            f.write("{r}\n".format(r=row))

        except KeyboardInterrupt:
            traceback.print_exc()

        self.get_logger().info("Replica Exchange simulation finished successfully!")
        self.get_logger().info("Deallocating resource.")
        resource.deallocate()

