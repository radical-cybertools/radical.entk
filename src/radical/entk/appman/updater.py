__copyright__   = "Copyright 2017-2018, http://radical.rutgers.edu"
__author__      = "Vivek Balasubramanian <vivek.balasubramaniana@rutgers.edu>"
__license__     = "MIT"

import radical.utils as ru
from radical.entk.exceptions import *
import threading
from Queue import Queue
import radical.entk.states as states

class Updater(object):

    def __init__(self, workload, executed_queue):

        self._uid           = ru.generate_id('radical.entk.updater')
        self._logger        = ru.get_logger('radical.entk.updater')

        if not isinstance(executed_queue,Queue):
            raise TypeError(expected_type="Queue", actual_type=type(executed_queue))

        self._executed_queue = executed_queue
        self._workload = workload

    def start_update(self):

        # This method starts the update function in a separate thread
        update_thread = threading.Thread(target=self.update_function)

    def update_function(self):

        # This function gets tasks from the 'executed_queue' that have finished 
        # executing. It then updates the various objects in the workload to their
        # appropriate states.

        try:

            task = self._executed_queue.get()

            for pipe in self._workload:

                if task.parent_pipeline == pipe.uid:

                    for stage in pipe:

                        if task.parent_stage == stage.uid:
                                task.state = states.DONE

                        if stage.check_tasks_status():
                            stage.state == states.DONE
                            pipe.increment_stage()



        except Exception, ex:
            self._logger.error('Unknown error in thread: %s'%ex)
            raise UnknownError(text=ex)

        except KeyboardInterrupt:

            self._logger.error('Execution interrupted by user (you probably hit Ctrl+C), '+
                            'trying to exit gracefully...')
            sys.exit(1)