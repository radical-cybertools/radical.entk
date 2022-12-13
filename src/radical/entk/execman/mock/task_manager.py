
__copyright__ = "Copyright 2017-2018, http://radical.rutgers.edu"
__author__    = "Vivek Balasubramanian <vivek.balasubramanian@rutgers.edu>"
__license__   = "MIT"

import queue

import threading       as mt
import multiprocessing as mp

from ...exceptions       import EnTKError
from ...                 import states, Task
from ..base.task_manager import Base_TaskManager


# pylint: disable=unused-argument
# ------------------------------------------------------------------------------
#
class TaskManager(Base_TaskManager):
    """
    A Task Manager takes the responsibility of dispatching tasks it receives
    from a 'pending' queue for execution on to the available resources using a
    runtime system. Once the tasks have completed execution, they are pushed
    on to the completed_queue for other components of EnTK to process.

    :arguments:
        :completed_queue:   (list) List of queue(s) with tasks that have
                            finished execution. Currently, only one queue.
        :rmgr:              (ResourceManager) Object to be used to access the
                            Pilot where the tasks can be submitted

    Currently, EnTK is configured to work with one pending queue and one
    completed queue. In the future, the number of queues can be varied for
    different throughput requirements at the cost of additional Memory and CPU
    consumption.
    """

    # --------------------------------------------------------------------------
    #
    def __init__(self, sid, rmgr, zmq_info):

        super(TaskManager, self).__init__(sid, rmgr, rts='mock',
                                          zmq_info=zmq_info)
        self._rts_runner = None

        self._log.info('Created task manager object: %s', self._uid)
        self._prof.prof('tmgr_create', uid=self._uid)


    # --------------------------------------------------------------------------
    #
    def _tmgr(self, uid, rmgr, zmq_info):
        """
        **Purpose**: Method to be run by the tmgr process. This method receives
                     a Task from the 'pending' and submits it to the RTS.
                     Currently, it also converts Tasks into CUDs and CUs into
                     (partially described) Tasks.  This conversion is necessary
                     since the current RTS is RADICAL Pilot.  Once Tasks are
                     recovered from a CU, they are then pushed to the
                     completed_queue. At all state transititons, they are synced
                     (blocking) with the AppManager in the master process.

        **Details**: The AppManager can re-invoke the tmgr process with this
                    function if the execution of the workflow is still
                    incomplete. There is also population of a dictionary,
                    `placeholders`, which stores the path of each of the tasks
                    on the remote machine.
        """

        try:

            self._prof.prof('tmgr process started', uid=self._uid)
            self._log.info('Task Manager process started')

            # Queue for communication between threads of this process
            task_queue = queue.Queue()

            # Start second thread to receive tasks and push to RTS
            self._rts_runner = mt.Thread(target=self._process_tasks,
                                         args=(task_queue, rmgr, zmq_info))
            self._rts_runner.start()

            self._prof.prof('tmgr infrastructure setup done', uid=uid)

            while not self._tmgr_terminate.is_set():

                try:

                    msgs = self._zmq_queue['get'].get_nowait(
                            qname='pending', timeout=1000)
                    if msgs:
                        for msg in msgs:

                            print('==== 1 get', msg)
                            if msg['type'] == 'workload':
                                task_queue.put(msg['body'])
                            elif msg['type'] == 'rts':
                                self._update_resource(msg['body'])
                            else:
                                self._log.error('TMGR receiver wrong message type')

                except Exception as e:
                    self._log.exception('Error in task execution: %s', e)
                    raise


        except KeyboardInterrupt:

            self._log.exception('Execution interrupted (probably by Ctrl+C), '
                                'cancel tmgr process gracefully...')
            raise


        except Exception as e:

            self._log.exception('%s failed with %s', self._uid, e)
            raise EnTKError(e) from e

        finally:

            self._prof.prof('tmgr_term', uid=uid)

            if self._rts_runner:
                self._rts_runner.join()

            self._prof.close()


    # --------------------------------------------------------------------------
    #
    def _process_tasks(self, task_queue, rmgr, zmq_info):
        '''
        **Purpose**: The new thread that gets spawned by the main tmgr process
                     invokes this function. This function receives tasks from
                     'task_queue' and submits them to the RADICAL Pilot RTS.
        '''

        # placeholders = dict()

        # # --------------------------------------------------------------------
        # def load_placeholder(task):
        #
        #     parent_pipeline = str(task.parent_pipeline['name'])
        #     parent_stage = str(task.parent_stage['name'])
        #
        #     if parent_pipeline not in placeholders:
        #         placeholders[parent_pipeline] = dict()
        #
        #     if parent_stage not in placeholders[parent_pipeline]:
        #         placeholders[parent_pipeline][parent_stage] = dict()
        #
        #     if None not in [parent_pipeline, parent_stage, task.name]:
        #         placeholders[parent_pipeline][parent_stage][str(
        #             task.name)] = str(task.path)
        # # --------------------------------------------------------------------

        try:

            while not self._tmgr_terminate.is_set():

                body = None

                try:
                    body = task_queue.get(block=True, timeout=10)

                except queue.Empty:
                    # Ignore, we don't always have new tasks to run
                    pass

                if not body:
                    continue

                task_queue.task_done()

                bulk_tasks = list()

                for msg in body:

                    task = Task(from_dict=msg)
                    bulk_tasks.append(task)

                    self._advance(task, 'Task', states.SUBMITTING,
                                 'tmgr-to-sync')

                # this mock RTS immmedialtely completes all tasks
                for task in bulk_tasks:

                    self._advance(task, 'Task', states.COMPLETED, 'cb-to-sync')

                    self._log.info('Pushed task %s with state %s to completed',
                                   task.uid, task.state)

        except KeyboardInterrupt:
            self._log.exception('Execution interrupted (probably by Ctrl+C), '
                                'cancel task processor gracefully...')

        except Exception as e:
            self._log.exception('%s failed with %s', self._uid, e)
            raise EnTKError(e) from e


    # --------------------------------------------------------------------------
    #
    def start_manager(self):
        """
        **Purpose**: Method to start the tmgr process. The tmgr function
                     is not to be accessed directly. The function is started
                     in a separate thread using this method.
        """

        if self._tmgr_process:
            self._log.warn('tmgr process already running!')
            return


        try:

            self._prof.prof('creating tmgr process', uid=self._uid)
            self._tmgr_terminate = mp.Event()

            self._tmgr_process = mp.Process(target=self._tmgr,
                                            name='task-manager',
                                            args=(self._uid,
                                                  self._rmgr,
                                                  self._zmq_info)
                                            )

            self._log.info('Starting task manager process')
            self._prof.prof('starting tmgr process', uid=self._uid)

            self._tmgr_process.start()

            return True

        except Exception as e:

            self._log.exception('Task manager not started, error: %s', e)
            self.terminate_manager()
            raise


# ------------------------------------------------------------------------------

