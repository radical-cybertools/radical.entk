
import radical.utils as ru


# ------------------------------------------------------------------------------
#
class Task(object):

    def __init__(self, descr):

        self._descr  = descr
        self._state  = 'NEW'
        self._cbs    = list()
        self._uid    = ru.generate_uid('task')
        self._stage  = None


    @property
    def state(self):

        return self._state

    def cancel(self):

        self._stage._pipe._wf._wfmgr.cancel(self)


    def add_callback(self, cb):

        self._cbs.append(cb)


# ------------------------------------------------------------------------------

