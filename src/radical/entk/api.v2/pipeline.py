
import radical.utils as ru

from .stage import Stage


# ------------------------------------------------------------------------------
#
class Pipeline(object):

    def __init__(self, descr):

        self._descr  = descr
        self._state  = 'NEW'
        self._stages = dict()
        self._cbs    = list()
        self._uid    = ru.generate_uid('pipe')
        self._wf     = None


    @property
    def state(self):

        return self._state


    def cancel(self):

        self._wf._wfmgr.cancel(self)
        pass


    def add_callback(self, cb):

        self._cbs.append(cb)


    # --------------------------------------------------------------------------
    #
    def add_stage(self, stage):

        self._stages[stage.uid] = stage
        stage._pipe = weakref(self)


    def list_stages(self):

        return self._stages


    def cancel_stages(self, uids):

        for uid in uids:
            self._stages[uid].cancel()


# ------------------------------------------------------------------------------


