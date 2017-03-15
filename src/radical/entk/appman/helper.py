__copyright__   = "Copyright 2017-2018, http://radical.rutgers.edu"
__author__      = "Vivek Balasubramanian <vivek.balasubramaniana@rutgers.edu>"
__license__     = "MIT"

import radical.utils as ru
from radical.entk.exceptions import *
import threading
import Queue
from radical.entk import states
import time

# TEMPORARY FILE TILL AN EXECUTION PLUGIN IS IN PLACE

class Helper(object):

    def __init__(self, pending_queue, executed_queue):

        self._uid           = ru.generate_id('radical.entk.helper')
        self._logger        = ru.get_logger('radical.entk.helper')

        self._pending_queue = pending_queue
        self._executed_queue = executed_queue

        self._terminate     = threading.Event()

        self._helper_thread = None
        self._thread_alive = False

        self._logger.info('Created populator object: %s'%self._uid)


    def start_helper(self):

        # This method starts the extractor function in a separate thread

        self._logger.info('Starting populator thread')
        self._helper_thread = threading.Thread(target=self.helper, name='helper')
        self._helper_thread.start()
        self._thread_alive = True
        self._logger.debug('Populator thread started')


    def helper(self):

        # This function extracts currently pending tasks from the pending_queue
        # and pushes it to the executed_queue. Thus mimicking an execution plugin

        try:

            # Thread should run till terminate condtion is encountered
            while not self._terminate.is_set():

                try:

                    task = self._pending_queue.get(timeout=5)
                    self._logger.debug('Got finished task %s from pending queue'%(task.uid))

                    self._executed_queue.put(task)
                    self._logger.debug('Pushed finished task %s to executed queue'%(task.uid))

                except Queue.Empty:
                    self._logger.debug('No tasks in pending_queue.. timeout 5 secs')

                time.sleep(1)



        except Exception, ex:
            self._logger.error('Unknown error in thread: %s'%ex)
            self._thread_alive = False
            raise UnknownError(text=ex)

        except KeyboardInterrupt:

            self._logger.error('Execution interrupted by user (you probably hit Ctrl+C), '+
                            'trying to exit gracefully...')
            self._thread_alive = False


    def terminate(self):

        # Set terminattion flag
        if not self._terminate.is_set():
            self._terminate.set()

        self._helper_thread.join()

    def check_alive(self):

        return self._thread_alive