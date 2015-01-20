#!/usr/bin/env python

"""TODO: Docstring.
"""

__author__    = "Ole Weider <ole.weidner@rutgers.edu>"
__copyright__ = "Copyright 2014, http://radical.rutgers.edu"
__license__   = "MIT"

import os
import radical.pilot
import radical.utils.logger  as rul
from radical.ensemblemd.engine import Engine
from radical.ensemblemd.exceptions import EnsemblemdError, TypeError
from radical.ensemblemd.execution_pattern import ExecutionPattern
from radical.ensemblemd.execution_context import ExecutionContext

CONTEXT_NAME = "Static"


#-------------------------------------------------------------------------------
#
class SingleClusterEnvironment(ExecutionContext):
    """A static execution context provides a fixed set of computational
       resources.
    """

    #---------------------------------------------------------------------------
    #
    def __init__(self, resource, cores, walltime, queue=None, username=None, allocation=None, cleanup=False):
        """Creates a new ExecutionContext instance.
        """
        self._allocate_called = False
        self._umgr = None
        self._session = None
        self._pilot = None

        self._resource_key = resource
        self._queue = queue
        self._cores = cores
        self._walltime = walltime
        self._username = username
        self._allocation = allocation
        self._cleanup = cleanup

        self._logger  = rul.getLogger ('radical.enmd', 'SingleClusterEnvironment')

        super(SingleClusterEnvironment, self).__init__()

    # --------------------------------------------------------------------------
    #
    def get_logger(self):
        return self._logger

    #---------------------------------------------------------------------------
    #
    @property
    def name(self):
        """Returns the name of the execution context.
        """
        return CONTEXT_NAME

    #---------------------------------------------------------------------------
    #
    def deallocate(self):
        """Deallocates the resources.
        """
        self._session.close()

    #---------------------------------------------------------------------------
    #
    def allocate(self):
        """Allocates the requested resources.
        """
        #-----------------------------------------------------------------------
        #
        def pilot_state_cb (pilot, state) :
            self.get_logger().info("Resource {0} state has changed to {1}".format(
                self._resource_key, state))

            if state == radical.pilot.FAILED:
                self.get_logger().error("Resource error: {0}".format(pilot.log[-1]))
                self.get_logger().error("Pattern execution FAILED.")

                # Try to get some information here...
                if os.getenv("RADICAL_ENMD_TRAVIS_DEBUG") is not None:

                    sb = saga.Url(pilot.sandbox).path
                    agent_stderr = "{0}/AGENT.STDERR".format(sb)
                    agent_stdout = "{0}/AGENT.STDOUT".format(sb)
                    agent_log = "{0}/AGENT.LOG".format(sb)
                    pip_cmd = "{0}/virtualenv/bin/pip".format(sb)

                    self.get_logger().error(pip_cmd)
                    os.system("head {0}".format(pip_cmd))
                    self.get_logger().error(agent_stderr)
                    os.system("cat {0}".format(agent_stderr))
                    self.get_logger().error(agent_stdout)
                    os.system("cat {0}".format(agent_stdout))
                    self.get_logger().error(agent_log)
                    os.system("cat {0}".format(agent_log))


        self._allocate_called = True

        # Here we start the pilot(s).
        try:
            self._session = radical.pilot.Session()

            if self._username is not None:
                # Add an ssh identity to the session.
                c = radical.pilot.Context('ssh')
                c.user_id = self._username
                self._session.add_context(c)

            pmgr = radical.pilot.PilotManager(session=self._session)
            pmgr.register_callback(pilot_state_cb)

            pdesc = radical.pilot.ComputePilotDescription()
            pdesc.resource = self._resource_key
            pdesc.runtime  = self._walltime
            pdesc.cores    = self._cores

            if self._queue is not None:
                pdesc.queue = self._queue

            pdesc.cleanup  = True

            if self._allocation is not None:
                pdesc.project = self._allocation

            self.get_logger().info("Requesting resources on {0}".format(self._resource_key))

            self._pilot = pmgr.submit_pilots(pdesc)

            self._umgr = radical.pilot.UnitManager(
                session=self._session,
                scheduler=radical.pilot.SCHED_DIRECT_SUBMISSION)

            self._umgr.add_pilots(self._pilot)

            self.get_logger().info("Launched {0}-core pilot on {1}.".format(self._cores, self._resource_key))

        except Exception, ex:
            self.get_logger().exception("Fatal error during resource allocation: {0}.".format(str(ex)))
            raise

    #---------------------------------------------------------------------------
    #
    def run(self, pattern, force_plugin=None):
        """Creates a new SingleClusterEnvironment instance.
        """
        # Make sure resources were allocated.
        if self._allocate_called is False:
            raise EnsemblemdError(
                msg="Resource(s) not allocated. Call allocate() first."
            )

        # Some basic type checks.
        if not isinstance(pattern, ExecutionPattern):
            raise TypeError(
              expected_type=ExecutionPattern,
              actual_type=type(pattern))

        self._engine = Engine()
        plugin = self._engine.get_execution_plugin_for_pattern(
            pattern_name=pattern.name,
            context_name=self.name,
            plugin_name=force_plugin)

        plugin.verify_pattern(pattern, self)
        plugin.execute_pattern(pattern, self)
