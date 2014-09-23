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

        pipeline_width = pattern.get_width()

        self.get_logger().info("Executing pipeline of width {0} on {1} allocated core(s) on '{2}'".format(
            pipeline_width, resource._cores, resource._resource_key))

        session = radical.pilot.Session()
        pmgr = radical.pilot.PilotManager(session=session)

        pdesc = radical.pilot.ComputePilotDescription()
        pdesc.resource = resource._resource_key
        pdesc.runtime  = resource._walltime
        pdesc.cores    = resource._cores
        pdesc.cleanup  = True

        pilot = pmgr.submit_pilots(pdesc)

        umgr = radical.pilot.UnitManager(
            session=session,
            scheduler=radical.pilot.SCHED_DIRECT_SUBMISSION)

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
            for instance in range(0, pipeline_width):

                kernel = s_meth(instance)
                kernel._bind_to_resource(resource._resource_key)

                # Expand "link_input_data" here
                input_data = kernel._kernel._link_input_data
                if type(input_data) != list:
                    input_data = [input_data]

                link = []

                for directive in input_data:
                    if directive is None:
                        break

                    # replace placeholders
                    if "$STEP_1" in directive:
                            directive = directive.replace("$STEP_1", working_dirs["step_1"]["inst_{0}".format(instance+1)])
                    if "$STEP_2" in directive:
                            directive = directive.replace("$STEP_2", working_dirs["step_2"]["inst_{0}".format(instance+1)])

                    dl = directive.split(">")

                    if len(dl) == 1:
                        ln_cmd = "ln -s {0} .".format(dl[0].strip())
                        link.append(ln_cmd)

                    elif len(dl) == 2:
                        ln_cmd = "ln -s {0} {1}".format(dl[0].strip(), dl[1].strip())
                        link.append(ln_cmd)

                    else:
                        # error
                        raise Exception("Invalid transfer directive %s" % download)

                cu = radical.pilot.ComputeUnitDescription()

                print "=============== WARNING - LINKING NOT IMPLEMENTED ================"

                cu.pre_exec       = kernel._cu_def_pre_exec
                cu.executable     = kernel._cu_def_executable
                cu.arguments      = kernel.arguments
                cu.mpi            = kernel.uses_mpi
                cu.input_staging  = kernel._cu_def_input_data
                cu.output_staging = kernel._cu_def_output_data

                step_cus.append(cu)
                self.get_logger().debug("Created step_1 CU ({0}/{1}): {2}.".format(instance+1, pipeline_width, cu.as_dict()))

            self.get_logger().info("Created {0} ComputeUnits for pipeline step {1}.".format(pipeline_width, step))

            units = umgr.submit_units(step_cus)

            # TODO: ensure working_dir <-> instance mapping
            instance = 0
            for unit in units:
                instance += 1
                working_dirs["step_{0}".format(step)]["inst_{0}".format(instance)] = saga.Url(unit.working_directory).path

            self.get_logger().info("Submitted ComputeUnits for pipeline step {0}.".format(step))
            self.get_logger().info("Waiting for ComputeUnits in pipeline step {0} to complete.".format(step))
            umgr.wait_units()

        self.get_logger().info("Pipeline finished.")
        session.close()
