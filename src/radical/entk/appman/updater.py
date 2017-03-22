__copyright__   = "Copyright 2017-2018, http://radical.rutgers.edu"
__author__      = "Vivek Balasubramanian <vivek.balasubramaniana@rutgers.edu>"
__license__     = "MIT"

import radical.utils as ru
from radical.entk.exceptions import *
import threading
import Queue
from radical.entk import states, Pipeline

class Updater(object):

    def __init__(self, workload, executed_queue):

        self._uid           = ru.generate_id('radical.entk.updater')
        self._logger        = ru.get_logger('radical.entk.updater')

        if not isinstance(executed_queue,Queue.Queue):
            raise TypeError(expected_type="Queue", actual_type=type(executed_queue))

        self._executed_queue    = executed_queue
        self._workload          = self.validate_workload(workload)

        self._terminate     = threading.Event()

        self._update_thread = None
        self._thread_alive = False

        self._logger.info('Created updater object: %s'%self._uid)


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

    def start_update(self):

        # This method starts the update function in a separate thread
        self._logger.info('Starting updater thread')
        self._update_thread = threading.Thread(target=self.update, name='updater')
        self._update_thread.start()
        self._thread_alive = True
        self._logger.debug('Updater thread started')


    def update(self):

        # This function gets tasks from the 'executed_queue' that have finished 
        # executing. It then updates the various objects in the workload to their
        # appropriate states.

        try:

            while not self._terminate.is_set():

                try:

                    task = self._executed_queue.get(timeout=5)
                    self._logger.debug('Got finished task %s from queue'%(task.uid))

                    for pipe in self._workload:

                        with pipe.stage_lock:

                            if not pipe.completed:

                                if task.parent_pipeline == pipe.uid:

                                    self._logger.debug('Found parent pipeline: %s'%pipe.uid)
                            
                                    for stage in pipe.stages:

                                        if task.parent_stage == stage.uid:
                                            self._logger.debug('Found parent stage: %s'%(stage.uid))
                                            try:
                                                task.state = states.DONE
                                                self._logger.info('Task %s in Stage %s of Pipeline %s: %s'%(
                                                            task.uid,
                                                            pipe.stages[pipe.current_stage].uid,
                                                            pipe.uid,
                                                            task.state))

                                            except Exception,ex:

                                                # Rollback queue and task status
                                                self._logger.error('Error while updating task '+
                                                    'state, rolling back')

                                                # Revert the state of the queue
                                                self._executed_queue.put(task)

                                                # Revert the state of the task
                                                task.state = states.EXECUTING


                                            if stage.check_tasks_status():
                                                self._logger.info('All tasks of stage %s finished' %(stage.uid))
                                                stage.state = states.DONE
                                                pipe.increment_stage()

                                if pipe.completed:
                                    #self._workload.remove(pipe)
                                    self._logger.info('Pipelines %s has completed'%(pipe.uid))
                                    pipe.state = states.DONE

                except Queue.Empty:
                    self._logger.debug('No tasks in executed_queue.. timeout 5 secs')


        except KeyboardInterrupt:

            self._logger.error('Execution interrupted by user (you probably hit Ctrl+C), '+
                            'trying to exit gracefully...')
            self._thread_alive = False

        except Exception, ex:
            self._logger.error('Unknown error in thread: %s'%ex)
            self._thread_alive = False
            raise UnknownError(text=ex)

        
    def terminate(self):

        # Set terminattion flag
        try:
            if not self._terminate.is_set():
                self._terminate.set()
                self._thread_alive = False

            self._update_thread.join()

        except Exception, ex:
            self._logger.error('Could not terminate populator thread')
            pass


    def check_alive(self):

        return self._thread_alive