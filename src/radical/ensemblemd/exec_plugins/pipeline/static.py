#!/usr/bin/env python

"""A static execution plugin for single tasks.
"""

__author__    = "Ole Weider <ole.weidner@rutgers.edu>"
__copyright__ = "Copyright 2014, http://radical.rutgers.edu"
__license__   = "MIT"

import os 
import saga
import radical.pilot 

from radical.ensemblemd.exceptions import NotImplementedError
from radical.ensemblemd.exec_plugins.plugin_base import PluginBase

# ------------------------------------------------------------------------------
# 
_PLUGIN_INFO = {
    "name":         "pipeline.static.default",
    "pattern":      "Pipeline",
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
        self.get_logger().info("Verifying pattern...")

    # --------------------------------------------------------------------------
    #
    def execute_pattern(self, pattern, resource):

        #-----------------------------------------------------------------------
        #
        def pilot_state_cb (pilot, state) :
            self.get_logger().info("Resource {0} state has changed to {1}".format(
                resource._resource_key, state))

            if state == radical.pilot.FAILED:
                self.get_logger().error("Resource error: {0}".format(pilot.log))
                self.get_logger().error("Pattern execution FAILED.")

                # Try to get some information here... 
                if os.getenv("RADICAL_ENMD_TRAVIS_DEBUG") is not None:

                    sb = saga.Url(pilot.sandbox).path
                    agent_stderr = "{0}/AGENT.STDERR".format(sb)
                    agent_stdout = "{0}/AGENT.STDOUT".format(sb)
                    agent_log = "{0}/AGENT.LOG".format(sb)

                    self.get_logger().error(agent_stderr)
                    os.system("cat {0}".format(agent_stderr))
                    self.get_logger().error(agent_stdout)
                    os.system("cat {0}".format(agent_stdout))
                    self.get_logger().error(agent_log)
                    os.system("cat {0}".format(agent_log))

        #-----------------------------------------------------------------------
        #
        def unit_state_cb (unit, state) :

            if state == radical.pilot.FAILED:
                self.get_logger().error("ComputeUnit error: STDERR: {0}, STDOUT: {0}".format(unit.stderr, unit.stdout))
                self.get_logger().error("Pattern execution FAILED.") 

        pipeline_instances = pattern.instances

        self.get_logger().info("Executing {0} pipeline instances on {1} allocated core(s) on '{2}'".format(
            pipeline_instances, resource._cores, resource._resource_key))

        try:

            session = radical.pilot.Session()

            if resource._username is not None:
                # Add an ssh identity to the session.
                c = radical.pilot.Context('ssh')
                c.user_id = resource._username
                session.add_context(c)

            pmgr = radical.pilot.PilotManager(session=session)
            pmgr.register_callback(pilot_state_cb)

            pdesc = radical.pilot.ComputePilotDescription()
            pdesc.resource = resource._resource_key
            pdesc.runtime  = resource._walltime
            pdesc.cores    = resource._cores
            pdesc.cleanup  = True

            if resource._allocation is not None:
                pdesc.project = resource._allocation

            self.get_logger().info("Requesting resources on {0}".format(resource._resource_key))

            pilot = pmgr.submit_pilots(pdesc)

            umgr = radical.pilot.UnitManager(
                session=session,
                scheduler=radical.pilot.SCHED_DIRECT_SUBMISSION)
            umgr.register_callback(unit_state_cb)

            umgr.add_pilots(pilot)

            self.get_logger().info("Launched {0}-core pilot on {1}.".format(resource._cores, resource._resource_key))

            working_dirs = {}

            for step in range(1, 64):
                s_meth = getattr(pattern, 'step_{0}'.format(step))

                # Build up the working dir bookkeeping structure.
                working_dirs["step_{0}".format(step)] = {}

                try:
                    kernel = s_meth(0)
                except NotImplementedError, ex:
                    # Not implemented means there are no further steps.
                    break

                step_cus = list()
                for instance in range(0, pipeline_instances):

                    kernel = s_meth(instance)
                    kernel._bind_to_resource(resource._resource_key)

                    if kernel._kernel._link_input_data is not None:

                        for directive in kernel._kernel._link_input_data:

                            if "$STEP_1" in directive:
                                kernel._kernel._link_input_data.remove(directive)
                                expanded_directive = directive.replace("$STEP_1", working_dirs["step_1"]["inst_{0}".format(instance+1)])
                                kernel._kernel._link_input_data.append(expanded_directive)

                            if "$STEP_2" in directive:
                                kernel._kernel._link_input_data.remove(directive)
                                expanded_directive = directive.replace("$STEP_2", working_dirs["step_2"]["inst_{0}".format(instance+1)])
                                kernel._kernel._link_input_data.append(expanded_directive)

                            if "$STEP_3" in directive:
                                kernel._kernel._link_input_data.remove(directive)
                                expanded_directive = directive.replace("$STEP_3", working_dirs["step_3"]["inst_{0}".format(instance+1)])
                                kernel._kernel._link_input_data.append(expanded_directive)

                    cu = radical.pilot.ComputeUnitDescription()

                    cu.pre_exec       = kernel._cu_def_pre_exec
                    cu.executable     = kernel._cu_def_executable
                    cu.arguments      = kernel.arguments
                    cu.mpi            = kernel.uses_mpi
                    cu.input_staging  = kernel._cu_def_input_data
                    cu.output_staging = kernel._cu_def_output_data

                    step_cus.append(cu)
                    self.get_logger().debug("Created step_1 CU ({0}/{1}): {2}.".format(instance+1, pipeline_instances, cu.as_dict()))

                self.get_logger().info("Created {0} ComputeUnits for pipeline step {1}.".format(pipeline_instances, step))

                units = umgr.submit_units(step_cus)

                # TODO: ensure working_dir <-> instance mapping
                instance = 0
                for unit in units:
                    instance += 1
                    working_dirs["step_{0}".format(step)]["inst_{0}".format(instance)] = saga.Url(unit.working_directory).path

                self.get_logger().info("Submitted ComputeUnits for pipeline step {0}.".format(step))
                self.get_logger().info("Waiting for ComputeUnits in pipeline step {0} to complete.".format(step))
                finished_units = umgr.wait_units()


            self.get_logger().info("Pattern execution successful.")

        except Exception, ex:
            self.get_logger().error("Fatal error during execution: {0}.".format(str(ex)))

        finally:
            self.get_logger().info("Deallocating resource.")
            session.close()
