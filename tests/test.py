class Stage(object):
    def __init__(self):
        self._post_exec = None

    @property
    def post_exec(self):
        print self._post_exec

    @post_exec.setter
    def post_exec(self, val):
        self._post_exec = val

