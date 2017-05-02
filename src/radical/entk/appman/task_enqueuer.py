__copyright__   = "Copyright 2017-2018, http://radical.rutgers.edu"
__author__      = "Vivek Balasubramanian <vivek.balasubramaniana@rutgers.edu>"
__license__     = "MIT"

import radical.utils as ru
from radical.entk.exceptions import *
import threading
import Queue
from radical.entk import states, Pipeline
import time
from random import randint

class Task_enqueuer(object):

    def __init__(self, workload, pending_queue):

        self._uid           = ru.generate_id('radical.entk.task_enqueuer')        
        self._logger        = ru.get_logger('radical.entk.task_enqueuer')
        self._workload      = self.validate_workload(workload)

        if not isinstance(pending_queue,Queue.Queue):
            raise TypeError(expected_type="Queue", actual_type=type(pending_queue))

        self._pending_queue = pending_queue
        self._terminate     = threading.Event()

        self._enqueue_thread = None
        self._thread_alive = False

        self._logger.info('Created task_enqueuer object: %s'%self._uid)

    def validate_workload(self, workload):

        if not isinstance(workload, set):

            if not isinstance(workload, list):
                workload = set([workload])
            else:
                workload = set(workload)


        for item in workload:
            if not isinstance(item, Pipeline):
                self._logger.info('Workload type incorrect')
                raise TypeError(expected_type=['Pipeline', 'set of Pipeline'], actual_type=type(item))

        return workload
        

    def start_enqueuer(self):

        # This method starts the extractor function in a separate thread

        self._logger.info('Starting enqueue thread')
        self._enqueue_thread = threading.Thread(target=self.enqueue, name='enqueue')
        self._enqueue_thread.start()
        self._thread_alive = True

        self._logger.debug('Enqueue thread started')


    def enqueue(self):

        # This function extracts currently executable tasks from the workload
        # and pushes it to the 'pending_queue'. This function also updates the 
        # state of the stage and task objects.

        try:

            # Thread should run till terminate condtion is encountered
            while not self._terminate.is_set():

                for pipe in self._workload:

                    #if pipe.stage_lock.acquire(False):
                    with pipe.stage_lock:

                        if not pipe.completed:

                            self._logger.debug('Pipe %s lock acquired'%(pipe.uid))

                            # Update corresponding pipeline's state
                            if not pipe.state == states.SCHEDULED:
                                pipe.state = states.SCHEDULED

                            if pipe.stages[pipe.current_stage].state in [states.NEW, states.SCHEDULED]:

                                executable_stage = pipe.stages[pipe.current_stage]
                                executable_tasks = executable_stage.tasks

                                try:

                                    for executable_task in executable_tasks:

                                        if executable_task.state == state.NEW:
                                    
                                            self._logger.debug('Task: %s, Stage: %s, Pipeline: %s'%(
                                                                        executable_task.uid,
                                                                        executable_task.parent_stage,
                                                                        executable_task.parent_pipeline))

                                            # Try-exception block for tasks
                                            try:

                                                # Add unscheduled task to pending_queue
                                                self._pending_queue.put(executable_task)

                                                # Update specific task's state if put to pending_queue
                                                executable_task.state = states.QUEUED

                                                # Update corresponding stage's state
                                                if not pipe.stages[pipe.current_stage].state == states.SCHEDULED:
                                                    pipe.stages[pipe.current_stage].state = states.SCHEDULED

                                            except Exception, ex:

                                                # Rolling back queue status
                                                self._logger.error('Error while updating task '+
                                                    'state, rolling back. Error: %s'%ex)

                                                # Now pending_queue does not have the specific task
                                                temp_queue = Queue.Queue()
                                                for task_id in range(self._pending_queue.qsize()-1):
                                                    task = self._pending_queue.get()
                                                    if not task.uid == executable_task.uid:
                                                        temp_queue.put(task)                                        
                                                self._pending_queue = temp_queue
                                        
                                                # Revert task status
                                                executable_task.state = states.NEW
                                                raise # should go to the next exception

                                    self._logger.info('Tasks in Stage %s of Pipeline %s: %s'%(
                                                            pipe.stages[pipe.current_stage].uid,
                                                            pipe.uid,
                                                            pipe.stages[pipe.current_stage].state))

                                                                        
                                except Exception, ex:

                                    # Rolling back queue status
                                    self._logger.error('Error while updating stage '+
                                                        'state, rolling back. Error: %s'%ex)

                                    # Revert stage state
                                    pipe.stages[pipe.current_stage].state = states.NEW                                        
                                
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
        try:
            if not self._terminate.is_set():
                self._terminate.set()
                self._thread_alive = False

            self._enqueue_thread.join()

        except Exception, ex:
            self._logger.error('Could not terminate enqueuer thread')
            pass

    def check_alive(self):
        return self._thread_alive

