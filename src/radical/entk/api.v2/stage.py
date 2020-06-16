
from .task import Task


# ------------------------------------------------------------------------------
#
class Stage(object):

    def __init__(self, descr):

        self._tasks    = [Task(td) for td in descr]
        self._state    = 'NEW'
        self._cb       = None

        self._pipeline = None  # pipeline uid
        self._workflow = None  # workflow uid


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

