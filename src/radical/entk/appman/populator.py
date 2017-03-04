__copyright__   = "Copyright 2017-2018, http://radical.rutgers.edu"
__author__      = "Vivek Balasubramanian <vivek.balasubramaniana@rutgers.edu>"
__license__     = "MIT"

import radical.utils as ru
from radical.entk.exceptions import *
import threading
from Queue import Queue
import radical.entk.states as states


class Populator(object):

    def __init__(self, workload):

        self._uid           = ru.generate_id('radical.entk.populator')
        self._workload      = workload
        self._logger        = ru.get_logger('radical.entk.populator')

        self._executable_queue = Queue()


    def start_population(self):

        extract_thread = threading.Thread(target=self.extract_function)
        return self._executable_queue


    def extract_function(self):

        if not pipe.is_set() for pipe in self._workload:
            self._workload.remove(pipe)

        while any(self._workload):

            for pipe in self._workload:

                with pipe.stage_lock:

                    if pipe.stages[pipe.current_stage].state is in states.INITAL:

                        executable_stage = pipe.stages[pipe.current_stage]
                        executable_tasks = executable_stage.tasks

                        self._executable_queue.put(executable_tasks)

                        pipe.stages[pipe.current_stage].set_task_state(states.QUEUED)
                        pipe.stages[pipe.current_stage].state == states.QUEUED





