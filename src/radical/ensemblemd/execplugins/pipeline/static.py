#!/usr/bin/env python

"""A static execution plugin for single tasks.
"""

__author__    = "Ole Weider <ole.weidner@rutgers.edu>"
__copyright__ = "Copyright 2014, http://radical.rutgers.edu"
__license__   = "MIT"

import os 
import radical.pilot

from radical.ensemblemd.task import Task
from radical.ensemblemd.batch import Batch

from radical.ensemblemd.execplugins.plugin_base import PluginBase

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
        workload = pattern._get_pattern_workload()

        # TODO: implement proper verification
        if True:
            self.get_logger().info("Pattern workload verification passed.")
        else:
            self.get_logger().error("Pattern workload verification failed.")

    # --------------------------------------------------------------------------
    #
    def execute_pattern(self, pattern):

        # DBURL = os.getenv("RADICAL_PILOT_DBURL")
        # if DBURL is None:
        #     print "ERROR: RADICAL_PILOT_DBURL (MongoDB server URL) is not defined."
        #     sys.exit(1)

        # Start a pilot 
        # pdesc = radical.pilot.ComputePilotDescription()
        # pdesc.resource = "localhost"
        # pdesc.runtime  = 15 # minutes
        # pdesc.cores    = 1

        # session = radical.pilot.Session(database_url=DBURL)

        # pmgr = radical.pilot.PilotManager(session=session)
        # pilot = pmgr.submit_pilots(pdesc)

        # umgr = radical.pilot.UnitManager(
        #     session=session,
        #     scheduler=radical.pilot.SCHED_DIRECT_SUBMISSION)

        steps = pattern._get_pattern_workload()

        # each entry in 'workload' is a sequential 'step' that consists either
        # of a single task or a batch. 
        total_steps = len(steps)
        step_count = 1
        for step in steps:
            self.get_logger().info("Executing pipeline step {0}/{1} with {2} task(s)".format(
                step_count,
                total_steps,
                step.size()))
            step_count += 1

            # Get task for this step, create a CU description and execute it.

            if type(step) == Task:
                self.get_logger().info(" > {0}".format(step._get_task_description()))

            if type(step) == Batch:
                for task in step._get_batch_description():
                    self.get_logger().info(" > {0}".format(task))


        # Close automatically cancels the pilot(s).
        # session.close()
