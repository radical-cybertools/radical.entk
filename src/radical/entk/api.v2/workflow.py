

from .pipeline import Pipeline


# ------------------------------------------------------------------------------
#
class Workflow(object):

    def __init__(self, descr):

        self._pipelines = [Pipeline(pd) for pd in descr]
        self._state     = 'NEW'
        self._cb        = None


    @property
    def state(self):
        return self._state

    def cancel(self):
        pass

    def add_callback(self, cb):
        self._cb = cb

    def _advance(self, state):

        self._state = state
        if self._cb:
            self._cb()


# ------------------------------------------------------------------------------

