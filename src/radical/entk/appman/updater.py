__copyright__   = "Copyright 2017-2018, http://radical.rutgers.edu"
__author__      = "Vivek Balasubramanian <vivek.balasubramaniana@rutgers.edu>"
__license__     = "MIT"

import radical.utils as ru
from radical.entk.exceptions import *
import threading
from Queue import Queue
from radical.entk import states

class Updater(object):

    def __init__(self, workload, executed_queue):

        self._uid           = ru.generate_id('radical.entk.updater')
        self._logger        = ru.get_logger('radical.entk.updater')

        if not isinstance(executed_queue,Queue):
            raise TypeError(expected_type="Queue", actual_type=type(executed_queue))

        self._executed_queue    = executed_queue
        self._workload          = workload

        self._terminate     = threading.Event()

        self._logger.info('Created updater object: %s'%self._uid)

    def start_update(self):

        # This method starts the update function in a separate thread
        self._logger.info('Starting updater thread')
        update_thread = threading.Thread(target=self.update_function)
        self._logger.debug('Updater thread started')

    def update_function(self):

        # This function gets tasks from the 'executed_queue' that have finished 
        # executing. It then updates the various objects in the workload to their
        # appropriate states.

        try:

            while self._terminate.is_set():

                task = self._executed_queue.get()
                self._logger.debug('Got finished task %s from queue'%(task.uid))

                for pipe in self._workload:

                    if task.parent_pipeline == pipe.uid:

                        with pipe.stage_lock:

                            for stage in pipe:

                                if task.parent_stage == stage.uid:
                                    self._logger.debug('Found parent stage: %s and pipeline: %s'%(
                                                                                stage.uid,
                                                                                pipe.uid))
                                    task.state = states.DONE
                                    self._logger.debug('Set task %s status to %s'%(task.uid,
                                                                            states.DONE))


                                if stage.check_tasks_status():
                                    self._logger.info('All tasks of stage %s finished' %(stage.uid))
                                    stage.state == states.DONE
                                    pipe.increment_stage()


        except KeyboardInterrupt:

            self._logger.error('Execution interrupted by user (you probably hit Ctrl+C), '+
                            'trying to exit gracefully...')
            sys.exit(1)

        except Exception, ex:
            self._logger.error('Unknown error in thread: %s'%ex)
            raise UnknownError(text=ex)

        
    def terminate(self):

        if not self._terminate.is_set():
            self._terminate.set()