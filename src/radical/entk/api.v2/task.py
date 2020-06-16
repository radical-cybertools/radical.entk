

# ------------------------------------------------------------------------------
#
class Task(object):

    def __init__(self, descr):

        self._state    = 'NEW'
        self._descr    = descr
        self._cb       = None

        self._stage    = None  # stage    uid
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

