__copyright__   = "Copyright 2017-2018, http://radical.rutgers.edu"
__author__      = "Vivek Balasubramanian <vivek.balasubramaniana@rutgers.edu>"
__license__     = "MIT"

import radical.utils as ru
from radical.entk.exceptions import *
import threading
from Queue import Queue
from radical.entk import states
import time


class Populator(object):

    def __init__(self, workload):

        self._uid           = ru.generate_id('radical.entk.populator')
        self._workload      = workload
        self._logger        = ru.get_logger('radical.entk.populator')

        self._executable_queue = Queue()
        self._terminate     = threading.Event()

        self._logger.info('Created populator object: %s'%self._uid)


    def start_population(self):

        # This method starts the extractor function in a separate thread

        self._logger.info('Starting populator thread')
        self._extract_thread = threading.Thread(target=self.extract_function, name='populator')
        self._extract_thread.start()
        self._logger.debug('Populator thread started')
        return self._executable_queue


    def extract_function(self):

        # This function extracts currently executable tasks from the workload
        # and pushes it to the 'executable_queue'. This function also updates the 
        # state of the stage and task objects.

        try:

            # Thread should run till terminate condtion is encountered
            while not self._terminate.is_set():

                for pipe in self._workload:

                    #if pipe.stage_lock.acquire(False):
                    with pipe.stage_lock:

                        if not pipe.completed:

                            self._logger.debug('Pipe %s lock acquired'%(pipe.uid))

                            if pipe.stages[pipe.current_stage].state in states.INITIAL:

                                executable_stage = pipe.stages[pipe.current_stage]
                                executable_tasks = executable_stage.tasks

                                for executable_task in executable_tasks:
                                    self._logger.debug('Task: %s, Stage: %s, Pipeline: %s'%(
                                                                    executable_task.uid,
                                                                    executable_task.parent_stage,
                                                                    executable_task.parent_pipeline))
                                    self._executable_queue.put(executable_task)

                                pipe.stages[pipe.current_stage].set_task_state(states.QUEUED)
                                pipe.stages[pipe.current_stage].state = states.QUEUED

                                self._logger.info('Tasks in Stage %s of Pipeline %s: %s'%(
                                                            pipe.stages[pipe.current_stage].uid,
                                                            pipe.uid,
                                                            pipe.stages[pipe.current_stage].state))

                time.sleep(1)

        except Exception, ex:
            self._logger.error('Unknown error in thread: %s'%ex)
            raise UnknownError(text=ex)

        except KeyboardInterrupt:

            self._logger.error('Execution interrupted by user (you probably hit Ctrl+C), '+
                            'trying to exit gracefully...')
            sys.exit(1)


    def terminate(self):

        # Set terminattion flag
        if not self._terminate.is_set():
            self._terminate.set()

        self._extract_thread.join()