
import radical.utils as ru


# ------------------------------------------------------------------------------
#
class WorkflowManager(object):

    # --------------------------------------------------------------------------
    #
    def __init__(self, rmq_url=None, uid=None, rtype=None):

        self._rtype     = rtype or 'radical.pilot'
        self._backend   = rtype.create_backend  # ...
        self._workflows = dict()                # uid - Workflow
        self._cb        = None
        self._uid       = ru.generate_id('wfmgr')


    # --------------------------------------------------------------------------
    #
    @property
    def uid(self):

        return self._uid


    # --------------------------------------------------------------------------
    #
    def acquire_resource(self, descr):
        # returns resource id (rid)

        self._backend.acquire_resource(descr)


    def list_resources(self):
        # return map: {rid: descr}

        return self._backend.list_resources()


    def release_resource(self, rid):

        return self._backend.release_resource(rid)


    def stage_to_resource(self, rid, data):

        return self._backend.stage_to_resource(rid, data)


    def stage_from_resource(self, rid, data):

        return self._backend.stage_from_resource(rid, data)


    # --------------------------------------------------------------------------
    #
    def submit_workflow(self, workflow):
        # returns none

        workflow._wfmgr = weakref(self)
        self._workflows[workflow.uid] = workflow
        self._backend.submit(workflow)


    def list_workflows(self):
        # return map {uid: workflow}

        return self._backend.list()


    def get_workflow(self, uid):

        return self._backend.get(uid)


    def cancel_workflow(self, uid):

        return self._backend.cancel(uid)


# ------------------------------------------------------------------------------
# pylint: disable=protected-access

