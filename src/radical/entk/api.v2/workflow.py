
import radical.utils as ru

from .pipeline import Pipeline


# ------------------------------------------------------------------------------
#
class Workflow(object):

    def __init__(self, descr):

        self._descr = descr
        self._state = 'NEW'
        self._pipes = dict()
        self._cbs   = list()
        self._uid   = ru.generate_uid('wf')
        self._wfmgr = None


    @property
    def state(self):

        return self._state


    def cancel(self):

        self._wfmgr._cancel(self)


    def add_callback(self, cb):
        self._cbs.append(cb)


    # --------------------------------------------------------------------------
    #
    def add_pipeline(self, pipe):

        self._pipes[pipe.uid] = pipe
        pipe._wf = weakref(self)


    def list_pipelines(self):

        return self._pipes


    def cancel_pipelines(self, uids):

        for uid in uids:
            self._pipes[uid].cancel()


# ------------------------------------------------------------------------------

