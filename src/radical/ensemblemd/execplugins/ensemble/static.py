#!/usr/bin/env python

"""A static execution plugin for single tasks.
"""

__author__    = "Ole Weider <ole.weidner@rutgers.edu>"
__copyright__ = "Copyright 2014, http://radical.rutgers.edu"
__license__   = "MIT"

import os 
import radical.pilot

from radical.ensemblemd.task import Task
from radical.ensemblemd.ensemble import Ensemble

from radical.ensemblemd.execplugins.plugin_base import PluginBase

# ------------------------------------------------------------------------------
# 
_PLUGIN_INFO = {
    "name":         "ensemble.static.default",
    "pattern":      "Ensemble",
    "context_type": "Static",
    "description":  "Executes single ensembles of tasks in a static execution context."
}

_PLUGIN_OPTIONS = []


# ------------------------------------------------------------------------------
# 
class Plugin(PluginBase):
    """The static execution plug-in for single ensembles. It works roughly in 
    the following way:

    TODO: describe plug-in operation
    """

    # --------------------------------------------------------------------------
    #
    def __init__(self):
        super(Plugin, self).__init__(_PLUGIN_INFO, _PLUGIN_OPTIONS)

    # --------------------------------------------------------------------------
    #
    def verify_pattern(self, pattern):
        workload = pattern._get_ensemble_description()

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

        steps = pattern._get_ensemble_description()
        self.get_logger().info("Executing ensemble with {0} task(s)".format(
            pattern.size()
        ))
        for step in steps:
            self.get_logger().info(" > {0}".format(
                step
        ))


