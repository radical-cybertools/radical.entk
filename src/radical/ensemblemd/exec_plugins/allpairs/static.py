#!/usr/bin/env python

"""A static execution plugin for the All Pairs Pattern.
"""

__author__    = "Ioannis Paraskevakos <i.paraskev@rutgers.edu>"
__copyright__ = "Copyright 2014, http://radical.rutgers.edu"
__license__   = "MIT"

import os 
import saga
import radical.pilot as rp
from radical.ensemblemd.exceptions import NotImplementedError
from radical.ensemblemd.exec_plugins.plugin_base import PluginBase

# ------------------------------------------------------------------------------
# 
_PLUGIN_INFO = {
    "name":         "allpairs.static.default",
    "pattern":      "AllPairsPattern",
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
    def verify_pattern(self, pattern):
        self.get_logger().info("Verifying pattern...") 

    #---------------------------------------------------------------------------
    #Pattern Execution Method
    def execute_pattern(self, pattern, resource):

        def pilot_state_cb(pilot,state):
            # Callback function for the messages printed when 
            # RADICAL_ENMD_VERBOSE = info
            self.get_logger().info("Pilot {0} in {1} state has changed to {2}.".format(pilot.uid,resource._resource_key,state))
            
            if state==rp.FAILED:
                # In Case the pilot fails report Error messsage
                self.get_logger().error("Pilot {0} FAILED at {1}.".format(pilot.uid,resource._resource_key))
                self.get_logger().error("Error: {0}".format(pilot.log))

        def unit_state_cb(unit, state):
            # Callback function for the messages printed when 
            # RADICAL_ENMD_VERBOSE = info
            if state == rp.DONE:
                self.get_logger().info("Task {0} state has finished succefully.".format(unit.uid))
            
            if state==rp.FAILED:
                # In Case the pilot fails report Error messsage
                self.get_logger().error("Task {0} FAILED.".format(unit.uid))
                self.get_logger().error("Error: {0}".format(unit.stderr))

        #-----------------------------------------------------------------------
        # Starting Plugin Execution

        NumElements = pattern.setelements
        Permutations = pattern.permutations

        self.get_logger().info("Executing All Pairs Pattern on the set {0} with {1} cores on {2}"
            .format(NumElements,resource._cores,resource._resource_key))

        STAGING_AREA = 'staging:///'

        try:
            session = rp.Session()

            if resource._username is not None:
                c         = rp.Context('ssh') # Connection type to the remote target machine
                c.user_id = resource._username #The user name for the remote target machine
                session.add_context(c)

            pmgr = rp.PilotManager(session=session)
            pmgr.register_callback(pilot_state_cb)

            PilotDescr = rp.ComputePilotDescription()
            PilotDescr.resource = resource._resource_key
            PilotDescr.runtime  = resource._walltime #Always in minutes
            PilotDescr.cores    = resource._cores
            PilotDescr.cleanup  = False

            if resource._allocation is not None:
                PilotDescr.project  = resource._allocation

            self.get_logger().info("Allocating {0} cores on {1}".format(PilotDescr.cores,
                PilotDescr.resource))
            Pilot = pmgr.submit_pilots(PilotDescr)
            
            umgr = rp.UnitManager(session=session,
                scheduler=rp.SCHED_BACKFILLING)
            umgr.register_callback(unit_state_cb)

            umgr.add_pilots(Pilot)
            self.get_logger().info("Pilot launched on {0}".format(PilotDescr.resource))

            CUDesc_list = list()
            for i in range(1,Permutations+1):
                #Output File Staging. The file after it is created in the folder of each CU, is moved to the folder defined in
                #the start of the script
                #OUTPUT_FILE           = {'source':'asciifile-{0}.dat'.format(i),
                #                        'target':os.path.join(STAGING_AREA,"asciifile-{0}.dat".format(i)),
                #                        'action':rp.MOVE}
                cudesc                = rp.ComputeUnitDescription()
                #cudesc.executable     = '/bin/bash' # The executable that will be executed by the CU
                #cudesc.output_staging = [OUTPUT_FILE]
                # The arguments of the executable
                #cudesc.arguments      = ['-l', '-c', 'base64 /dev/urandom | head -c 1048576 > %s'%OUTPUT_FILE['source']]
                # The number of Cores the CU will use.
                #cudesc.cores          = 1

                cudesc.pre_exec       = kernel._cu_def_pre_exec
                cudesc.executable     = kernel._cu_def_executable
                cudesc.arguments      = kernel.arguments
                cudesc.mpi            = kernel.uses_mpi
                cudesc.input_staging  = kernel._cu_def_input_data
                cudesc.output_staging = kernel._cu_def_output_data
    
                CUDesc_list.append(cudesc)

                Units = umgr.submit_units(CUDesc_list)
                umgr.wait_units()

                self.get_logger().info("Pattern execution successful.")

        except Exception, ex:
            self.get_logger().error("Fatal error during execution: {0}.".format(str(ex)))

        finally:
            self.get_logger().info("Deallocating resource.")
            session.close()