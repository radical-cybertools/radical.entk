

# ------------------------------------------------------------------------------
#
class AppManager(object):

    # --------------------------------------------------------------------------
    #
    def __init__(self, rmq_url=None, uid=None, rtype=None):

        self._rtype     = rtype or 'radical.pilot'
        self._backend   = rtype.create_backend  # ...
        self._workflows = dict()                # uid - Workflow
        self._cb        = None


    # --------------------------------------------------------------------------
    #
    @property
    def uid(self):

        return self._uid


    # --------------------------------------------------------------------------
    #
    def acquire_resource(self, descr):

        self._backend.acquire_resource(descr)


    def list_resources(self):

        return self._backend.list_resources()


    def release_resource(self, rid):

        return self._backend.release_resource(rid)


    # --------------------------------------------------------------------------
    #
    def submit(self, workflow):

        self._workflows[workflow.uid] = workflow


    def list_workflows(self):

        return self._workflows.keys()


    def get_workflow(self, uid):

        return self._workflows[uid]


    def cancel(self, workflow_id):

        pass


    def add_callback(self, cb):
        self._cb = cb

    # goes to workflow
  # def shared_data(self):
  # def outputs(self):



# ------------------------------------------------------------------------------
# pylint: disable=protected-access

