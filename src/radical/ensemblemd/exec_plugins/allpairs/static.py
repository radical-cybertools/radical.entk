#!/usr/bin/env python

"""A static execution plugin for the All Pairs Pattern.
"""

__author__    = "Ioannis Paraskevakos <i.paraskev@rutgers.edu>"
__copyright__ = "Copyright 2014, http://radical.rutgers.edu"
__license__   = "MIT"

import os
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
    def verify_pattern(self, pattern):
        self.get_logger().info("Verifying pattern...")

    #---------------------------------------------------------------------------
    #Pattern Execution Method
    def execute_pattern(self, pattern, resource):

        def pilot_state_cb(pilot,state):
            # Callback function for the messages printed when
            # RADICAL_ENMD_VERBOSE = info
            self.get_logger().info("Pilot {0} in {1} state has changed to {2}.".format(pilot.uid,resource._resource_key,state))

            if state==radical.pilot.FAILED:
                # In Case the pilot fails report Error messsage
                self.get_logger().error("Pilot {0} FAILED at {1}.".format(pilot.uid,resource._resource_key))
                self.get_logger().error("Error: {0}".format(pilot.log))

        def unit_state_cb(unit, state):
            # Callback function for the messages printed when
            # RADICAL_ENMD_VERBOSE = info
            if state == radical.pilot.DONE:
                self.get_logger().info("Task {0} state has finished succefully.".format(unit.uid))

            if state==radical.pilot.FAILED:
                # In Case the pilot fails report Error messsage
                self.get_logger().error("Task {0} FAILED.".format(unit.uid))
                self.get_logger().error("Error: {0}".format(unit.stderr))

        def comparisons (n) :
            ret = list ()
            for i in range (1, n+1) :
                for j in range (i+1, n+1) :
                    ret.append ([i, j])
            return ret

        #-----------------------------------------------------------------------
        # Starting Plugin Execution

        NumElements = pattern._size
        Permutations = pattern.permutations
        self.get_logger().info("Number of Elements {0}".format(NumElements))
        self.get_logger().info("Executing All Pairs Pattern on the set {0} with {1} cores on {2}"
            .format(NumElements,resource._cores,resource._resource_key))

        STAGING_AREA = 'staging:///'

        try:

            session = radical.pilot.Session()

            if resource._username is not None:
                c         = radical.pilot.Context('ssh') # Connection type to the remote target machine
                c.user_id = resource._username #The user name for the remote target machine
                session.add_context(c)

            pmgr = radical.pilot.PilotManager(session=session)
            pmgr.register_callback(pilot_state_cb)

            PilotDescr = radical.pilot.ComputePilotDescription()
            PilotDescr.resource = resource._resource_key
            PilotDescr.runtime  = resource._walltime #Always in minutes
            PilotDescr.cores    = resource._cores
            PilotDescr.cleanup  = False

            if resource._allocation is not None:
                PilotDescr.project  = resource._allocation

            self.get_logger().info("Allocating {0} cores on {1}".format(PilotDescr.cores,
                PilotDescr.resource))
            Pilot = pmgr.submit_pilots(PilotDescr)

            umgr = radical.pilot.UnitManager(session=session,
                scheduler=radical.pilot.SCHED_BACKFILLING)
            umgr.register_callback(unit_state_cb)

            umgr.add_pilots(Pilot)
            self.get_logger().info("Pilot launched on {0}".format(PilotDescr.resource))

            CUDesc_list = list()
            for i in range(1,NumElements+1):
                kernel = pattern.element_initialization(element=i)
                link_out_data=kernel.get_arg("--filename=")
                kernel._bind_to_resource(resource._resource_key)
                self.get_logger().info("Kernels : {0}, Name: {1}".format(kernel,dir(kernel)))
            #     #Output File Staging. The file after it is created in the folder of each CU, is moved to the folder defined in
            #     #the start of the script

                OUTPUT_FILE           = {'source':link_out_data,
                                         'target':os.path.join(STAGING_AREA,link_out_data),
                                         'action':radical.pilot.LINK}
                cudesc                = radical.pilot.ComputeUnitDescription()
            #     #cudesc.executable     = '/bin/bash' # The executable that will be executed by the CU
            #     #cudesc.output_staging = [OUTPUT_FILE]
            #     # The arguments of the executable
            #     #cudesc.arguments      = ['-l', '-c', 'base64 /dev/urandom | head -c 1048576 > %s'%OUTPUT_FILE['source']]
            #     # The number of Cores the CU will use.
            #     #cudesc.cores          = 1

                cudesc.pre_exec       = kernel._cu_def_pre_exec
                cudesc.executable     = kernel._cu_def_executable
                cudesc.arguments      = kernel.arguments
                cudesc.mpi            = kernel.uses_mpi
                cudesc.output_staging = [OUTPUT_FILE]
                #self.get_logger().info("Target {0} to : {0}".format(kernel._cu_def_output_data))
                self.get_logger().info("Pre Exec: {0} Executable: {1} Arguments: {2} MPI: {3} Output: {4}".format(cudesc.pre_exec,
                    kernel._cu_def_executable,cudesc.arguments,cudesc.mpi,cudesc.output_staging))


                CUDesc_list.append(cudesc)

            Units = umgr.submit_units(CUDesc_list)
            umgr.wait_units()

            CUDesc_list = list()
            journal = {}

            for i,j in comparisons(NumElements):
                kernel = pattern.element_comparison(element1=i, element2=j)
                link_input1=kernel.get_arg("--inputfile1=")
                link_input2=kernel.get_arg("--inputfile2=")
                link_output=kernel.get_arg("--outputfile=")
                kernel._bind_to_resource(resource._resource_key)
                self.get_logger().info("Kernels : {0}, Name: {1}".format(kernel,dir(kernel)))
            #     #Output File Staging. The file after it is created in the folder of each CU, is moved to the folder defined in
            #     #the start of the script
                INPUT_FILE1           = {'source': os.path.join(STAGING_AREA,link_input1),
                                        'target' : link_input1,
                                        'action' : radical.pilot.LINK}
                INPUT_FILE2           = {'source': os.path.join(STAGING_AREA, link_input2),
                                        'target' : link_input2,
                                        'action' : radical.pilot.LINK}
                cudesc                = radical.pilot.ComputeUnitDescription()
            #     #cudesc.executable     = '/bin/bash' # The executable that will be executed by the CU
            #     #cudesc.output_staging = [OUTPUT_FILE]
            #     # The arguments of the executable
            #     #cudesc.arguments      = ['-l', '-c', 'base64 /dev/urandom | head -c 1048576 > %s'%OUTPUT_FILE['source']]
            #     # The number of Cores the CU will use.
            #     #cudesc.cores          = 1

                cudesc.pre_exec       = kernel._cu_def_pre_exec
                cudesc.executable     = kernel._cu_def_executable
                cudesc.arguments      = kernel.arguments
                cudesc.mpi            = kernel.uses_mpi
                cudesc.input_staging  = [INPUT_FILE1, INPUT_FILE2]
                cudesc.output_staging = [link_output]
                self.get_logger().info("Pre Exec: {0} Executable: {1} Arguments: {2} MPI: {3} Input: {4} Output: {5}".format(cudesc.pre_exec,
                    kernel._cu_def_executable,cudesc.arguments,cudesc.mpi,cudesc.input_staging,cudesc.output_staging))

                journal["{p1}-{p2}".format(p1=i, p2=j)] = {
                    "p1": i,
                    "p2": j,
                    "unit_description": cudesc,
                    "compute_unit": umgr.submit_units(cudesc)
                }

            umgr.wait_units()
            self.get_logger().info("Pattern execution successful.")

        except Exception, ex:
            self.get_logger().error("Fatal error during execution: {0}.".format(str(ex)))

        finally:
            self.get_logger().info("Deallocating resource.")
            session.close()

        # -----------------------------------------------------------------
        # At this point, we have executed the pattern succesfully. Now,
        # if profiling is enabled, we can write the profiling data to
        # a file.
        do_profile = os.getenv('RADICAL_ENDM_PROFILING', '0')

        if do_profile != 0:

            outfile = "execution_profile_{time}.csv".format(time=datetime.datetime.now().isoformat())
            self.get_logger().info("Saving execution profile in {outfile}".format(outfile=outfile))

            with open(outfile, 'w+') as f:
                # General format of a profiling file is row based and follows the
                # structure <unit id>; <s_time>; <stop_t>; <tag1>; <tag2>; ...
                head = "task; start_time; stop_time; p1; p2"
                f.write("{row}\n".format(row=head))

                for pair in journal.keys():
                    data = journal[pair]
                    cu = data["compute_unit"]

                    row = "{uid}; {start_time}; {stop_time}; {tags}".format(
                        uid=cu.uid,
                        start_time=cu.start_time,
                        stop_time=cu.stop_time,
                        tags="{p1}; {p2}".format(p1=data["p1"], p2=data["p2"])
                    )
                    f.write("{row}\n".format(row=row))
