#!/usr/bin/env python
from __future__ import division

"""A static execution plugin for the All Pairs Pattern.
"""

__author__    = "Ioannis Paraskevakos <i.paraskev@rutgers.edu>"
__copyright__ = "Copyright 2014, http://radical.rutgers.edu"
__license__   = "MIT"

import os
import ast
import traceback
import saga
import datetime
import radical.pilot
from radical.ensemblemd.exceptions import NotImplementedError
from radical.ensemblemd.exec_plugins.plugin_base import PluginBase

# ------------------------------------------------------------------------------
#
_PLUGIN_INFO = {
    "name":         "allpairs.static.default",
    "pattern":      "AllPairs",
    "context_type": "Static"
}

_PLUGIN_OPTIONS = []

#-------------------------------------------------------------------------------
#
class Plugin(PluginBase):

    #---------------------------------------------------------------------------
    # Class Construction
    def __init__(self):
        super(Plugin, self).__init__(_PLUGIN_INFO,_PLUGIN_OPTIONS)

    #---------------------------------------------------------------------------
    #
    def verify_pattern(self, pattern, resource):
        self.get_logger().info("Verifying pattern...")

    #---------------------------------------------------------------------------
    #Pattern Execution Method
    def execute_pattern(self, pattern, resource):

        
        pattern_start_time = datetime.datetime.now()

        self._reporter.ok('>>ok')
        
        def unit_state_cb(unit, state):
            # Callback function for the messages printed when
            # RADICAL_ENMD_VERBOSE = info
            if state == radical.pilot.DONE:
                self.get_logger().info("Task {0} state has finished succefully.".format(unit.uid))

            if state==radical.pilot.FAILED:
                # In Case the pilot fails report Error messsage
                self.get_logger().error("Task {0} FAILED.".format(unit.uid))
                self.get_logger().error("Error: {0}".format(unit.stderr))
                self.get_logger().error("Pattern execution FAILED.")
                sys.exit(1)

        def comparisons (set1,set2):
            ret = list ()
            if set2 == None:
                for i in range (1, (len(set1)+1)):
                    for j in range (1, len(set2)+1):
                        ret.append ([i, j])
            else:
                for i in range (1, (len(set1)+1)):
                    for j in range (i+1, len(set1)+1):
                        ret.append ([i, j])
            return ret

        #-----------------------------------------------------------------------
        # Starting Plugin Execution


        self.get_logger().debug("Set 1 is {0}".format(pattern.set1_elements()))
        self.get_logger().debug("Set 2 is {0}".format(pattern.set2_elements()))
        NumElementsSet1 = len(pattern.set1_elements())
        Permutations = pattern.permutations
        if pattern.set2_elements() is None:
            self.get_logger().info("Number of Elements {0}".format(NumElementsSet1))
            self.get_logger().info("Executing All Pairs Pattern on the set {0} with {1} cores on {2}".format( pattern.set1_elements() ,resource._cores,resource._resource_key))

            self._reporter.header("Executing All Pairs Pattern on the set {0} with {1} cores on {2}".format(pattern.set1_elements() ,resource._cores,resource._resource_key))
        else:
            NumElementsSet2 = len(pattern.set2_elements())
            self.get_logger().info("Number of Elements of the First Set {0}".format(NumElementsSet1))
            self.get_logger().info("Number of Elements of the First Set {0}".format(NumElementsSet2))
            self.get_logger().info("Executing All Pairs Pattern on the sets {0}-{1} with {2} cores on {3}"
            .format(pattern.set1_elements(),pattern.set2_elements(),resource._cores,resource._resource_key))

            self._reporter.header("Executing All Pairs Pattern on the set {0}-{1} with {2} cores on {3}".format(pattern.set1_elements(),pattern.set2_elements(),resource._cores,resource._resource_key))


        STAGING_AREA = 'staging:///'
        

        self.get_logger().info("Waiting for pilot on {0} to go Active".format(resource._resource_key))
        self._reporter.info("Job waiting on queue...".format(resource._resource_key))
        resource._pmgr.wait_pilots(resource._pilot.uid,'Active')
        self._reporter.ok("\nJob is now running !".format(resource._resource_key))

        try:
            
            resource._umgr.register_callback(unit_state_cb)
            CUDesc_list = list()
            self.get_logger().info("Creating the Elements of Set 1")
            
            for i in range(1,NumElementsSet1+1):
                kernel = pattern.set1element_initialization(element=i)
                link_out_data=kernel.get_arg("--filename=")
                kernel._bind_to_resource(resource._resource_key)
                self.get_logger().debug("Kernels : {0}, Name: {1}".format(kernel,dir(kernel)))
            #     #Output File Staging. The file after it is created in the folder of each CU, is moved to the folder defined in
            #     #the start of the script
                OUTPUT_FILE           = {'source':link_out_data,
                                         'target':os.path.join(STAGING_AREA,link_out_data),
                                         'action':radical.pilot.LINK}
                cudesc                = radical.pilot.ComputeUnitDescription()
                cudesc.pre_exec       = kernel._cu_def_pre_exec
                cudesc.executable     = kernel._cu_def_executable
                cudesc.arguments      = kernel.arguments
                cudesc.mpi            = kernel.uses_mpi
                cudesc.output_staging = [OUTPUT_FILE]
                #self.get_logger().info("Target {0} to : {0}".format(kernel._cu_def_output_data))
                self.get_logger().debug("Pre Exec: {0} Executable: {1} Arguments: {2} MPI: {3} Output: {4}".format(cudesc.pre_exec,
                    kernel._cu_def_executable,cudesc.arguments,cudesc.mpi,cudesc.output_staging))
                CUDesc_list.append(cudesc)
            
            if pattern.set2_elements() is not None:
                self.get_logger().info("Creating the Elements of Set 2")
            
                for i in range(1,NumElementsSet2+1):
                    kernel = pattern.set2element_initialization(element=i)
                    link_out_data=kernel.get_arg("--filename=")
                    kernel._bind_to_resource(resource._resource_key)
                    self.get_logger().debug("Kernels : {0}, Name: {1}".format(kernel,dir(kernel)))
                #     #Output File Staging. The file after it is created in the folder of each CU, is moved to the folder defined in
                #     #the start of the script
                    OUTPUT_FILE           = {'source':link_out_data,
                                             'target':os.path.join(STAGING_AREA,link_out_data),
                                             'action':radical.pilot.LINK}
                    cudesc                = radical.pilot.ComputeUnitDescription()
                    cudesc.pre_exec       = kernel._cu_def_pre_exec
                    cudesc.executable     = kernel._cu_def_executable
                    cudesc.arguments      = kernel.arguments
                    cudesc.mpi            = kernel.uses_mpi
                    cudesc.output_staging = [OUTPUT_FILE]
                    #self.get_logger().info("Target {0} to : {0}".format(kernel._cu_def_output_data))
                    self.get_logger().debug("Pre Exec: {0} Executable: {1} Arguments: {2} MPI: {3} Output: {4}".format(cudesc.pre_exec,
                        kernel._cu_def_executable,cudesc.arguments,cudesc.mpi,cudesc.output_staging))
                    CUDesc_list.append(cudesc)
            
            Units = resource._umgr.submit_units(CUDesc_list)
            
            self._reporter.info("\nWaiting to create the elements of set 2 ")
            uids = [cu.uid for cu in Units]
            resource._umgr.wait_units(uids)
            self._reporter.ok('>> done')
            CUDesc_list = list()
            all_cus = []
            
            windowsize1 = pattern._windowsize1
            windowsize2 = pattern._windowsize2
            #journal = dict()
            
            #step_timings = {
            #    "name": "AllPairs",
            #    "timings": {}
            #}
            
            step_start_time_abs = datetime.datetime.now()
            
            for i in range(1,NumElementsSet1+1,windowsize1):
                
                if pattern.set2_elements() is None:
                
                    for j in range(i,NumElementsSet1+1,windowsize1):
                        kernel = pattern.element_comparison(elements1=range(i,i+windowsize1), 
                            elements2=range(j,j+windowsize1))
                        try:
                            link_input1=ast.literal_eval(kernel.get_arg("--inputfile1="))
                        except:
                            link_input1=[kernel.get_arg("--inputfile1=")]
                        try:
                            link_input2=ast.literal_eval(kernel.get_arg("--inputfile2="))
                        except:
                            link_input2=[kernel.get_arg("--inputfile2=")]
                        link_output=kernel.get_arg("--outputfile=")
                        kernel._bind_to_resource(resource._resource_key)
                        self.get_logger().info("Kernels : {0}, Name: {1}".format(kernel,dir(kernel)))
                    #     #Output File Staging. The file after it is created in the folder of each CU, is moved to the folder defined in
                    #     #the start of the script
                        self.get_logger().debug("i = {0}, j = {1}, window size = {2}".format(i,j,windowsize1))
                        self.get_logger().debug("Link Input 1 = {0}".format(link_input1))
                        self.get_logger().debug("Link Input 2 = {0}".format(link_input2))
                        INPUT_FILE1           = [{'source': os.path.join(STAGING_AREA,link_input1[k-1]),
                                                'target' : link_input1[k-1],
                                                'action' : radical.pilot.LINK} for k in range(1,windowsize1+1)]
                
                        if i != j:
                            INPUT_FILE2           = [{'source': os.path.join(STAGING_AREA, link_input2[k-1]),
                                                    'target' : link_input2[k-1],
                                                    'action' : radical.pilot.LINK} for k in range(1,windowsize1+1)]
                        else:
                            INPUT_FILE2       = []
                        cudesc                = radical.pilot.ComputeUnitDescription()
                        cudesc.name           = "comp; {el11};{el21}".format(el11=i,el21=j)
                        cudesc.pre_exec       = kernel._cu_def_pre_exec
                        cudesc.executable     = kernel._cu_def_executable
                        cudesc.arguments      = kernel.arguments
                        cudesc.mpi            = kernel.uses_mpi
                        self.get_logger().debug("Input File 1: {0}".format(INPUT_FILE1))
                        self.get_logger().debug("Input File 2: {0}".format(INPUT_FILE2))
                
                        if kernel._cu_def_input_data is None:
                            self.get_logger().debug("Input Staging without Kernel CU DEF Input Data")
                            cudesc.input_staging  = INPUT_FILE1+INPUT_FILE2
                        else:
                            self.get_logger().debug("Input Staging with Kernel CU DEF Input Data")
                            cudesc.input_staging  = kernel._cu_def_input_data+INPUT_FILE1+INPUT_FILE2
                        cudesc.output_staging = [link_output]
                        self.get_logger().debug("Pre Exec: {0} Executable: {1} Arguments: {2} MPI: {3} Input: {4} Output: {5}".format(cudesc.pre_exec,
                            kernel._cu_def_executable,cudesc.arguments,cudesc.mpi,cudesc.input_staging,cudesc.output_staging))
                        all_cus.append(cudesc)
            
                else:
            
                    for j in range(1,NumElementsSet2+1,windowsize2):
                        kernel = pattern.element_comparison(elements1=range(i,i+windowsize1), 
                            elements2=range(j,j+windowsize2))
                        try:
                            link_input1=ast.literal_eval(kernel.get_arg("--inputfile1="))
                        except:
                            link_input1=[kernel.get_arg("--inputfile1=")]
                        try:
                            link_input2=ast.literal_eval(kernel.get_arg("--inputfile2="))
                        except:
                            link_input2=[kernel.get_arg("--inputfile2=")]
                        link_output=kernel.get_arg("--outputfile=")
                        kernel._bind_to_resource(resource._resource_key)
                        self.get_logger().info("Kernels : {0}, Name: {1}".format(kernel,dir(kernel)))
                    #     #Output File Staging. The file after it is created in the folder of each CU, is moved to the folder defined in
                    #     #the start of the script
                        INPUT_FILE1           = [{'source': os.path.join(STAGING_AREA,link_input1[k-1]),
                                                'target' : link_input1[k-1],
                                                'action' : radical.pilot.LINK} for k in range(1,windowsize1+1)]
                        INPUT_FILE2           = [{'source': os.path.join(STAGING_AREA, link_input2[k-1]),
                                                'target' : link_input2[k-1],
                                                'action' : radical.pilot.LINK} for k in range(1,windowsize2+1)]
                        cudesc                = radical.pilot.ComputeUnitDescription()
                        cudesc.name           = "comp; {el11};{el21}".format(el11=i,el21=j)
                        cudesc.pre_exec       = kernel._cu_def_pre_exec
                        cudesc.executable     = kernel._cu_def_executable
                        cudesc.arguments      = kernel.arguments
                        cudesc.mpi            = kernel.uses_mpi
            
                        if kernel._cu_def_input_data is None:
                            self.get_logger().debug("Input Staging without Kernel CU DEF Input Data")
                            cudesc.input_staging=INPUT_FILE1+INPUT_FILE2
                        else:
                            self.get_logger().debug("Input Staging with Kernel CU DEF Input Data")
                            cudesc.input_staging  = kernel._cu_def_input_data+INPUT_FILE1+INPUT_FILE2
                        cudesc.output_staging = [link_output]
                        self.get_logger().debug("Pre Exec: {0} Executable: {1} Arguments: {2} MPI: {3} Input: {4} Output: {5}".format(cudesc.pre_exec,
                            kernel._cu_def_executable,cudesc.arguments,cudesc.mpi,cudesc.input_staging,cudesc.output_staging))
                        all_cus.append(cudesc)
            
            sub_unit=resource._umgr.submit_units(all_cus)
            
            #self.get_logger().debug(sub_unit)
            self._reporter.info("\nWaiting for analysis step to complete.")
            uids = [cu.uid for cu in sub_unit]
            resource._umgr.wait_units(uids)
            self._reporter.ok('>> done')

            step_end_time_abs = datetime.datetime.now()

            self.get_logger().info("Pattern execution successful.")
            self._reporter.header('Pattern execution successfully finished')
            
            
            ## Process CU information and append it to the dictionary
            #tinfo = extract_timing_info(sub_unit, pattern_start_time, step_start_time_abs, step_end_time_abs)
            #self.get_logger().debug("Extracted timings Information")
            
            #mean_unit_time = 0
            
            #for unit in sub_unit:
            #    mean_unit_time =mean_unit_time+(unit.stop_time-unit.start_time).total_seconds()
            #mean_unit_time = mean_unit_time/len(sub_unit)
            #self.get_logger().debug("Mean CU execution time is %f"%mean_unit_time)
            
            #for key, val in tinfo.iteritems():
            #    step_timings['timings'][key] = val
            #self.get_logger().debug("Created step timings")
            
            ## Write the whole thing to the profiling dict
            #pattern._execution_profile.append(step_timings)
            #self.get_logger().debug("Wrote the whole thing to the profiling dict")
        except KeyboardInterrupt:
            traceback.print_exc()



            

