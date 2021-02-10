
import radical.utils as ru

from .task import Task


# ------------------------------------------------------------------------------
#
class Stage(object):

    def __init__(self, descr):

        self._descr  = descr
        self._state  = 'NEW'
        self._tasks  = dict()
        self._cbs    = list()
        self._uid    = ru.generate_uid('stage')
        self._pipe   = None


    @property
    def state(self):

        return self._state


    def cancel(self):

        self._pipe._wf._wfmgr.cancel(self)


    def add_callback(self, cb):

        self._cbs.append(cb)


    # --------------------------------------------------------------------------
    #
    def add_task(self, task):

        self._tasks[task.uid] = task
        task._stage = weakref(self)


    def list_tasks(self):

        return self._tasks


    def cancel_tasks(self, uids):

        for uid in uids:
            self._tasks[uid].cancel()


# ------------------------------------------------------------------------------

