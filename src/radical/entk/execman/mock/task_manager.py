
__copyright__ = 'Copyright 2017-2018, http://radical.rutgers.edu'
__author__    = 'Vivek Balasubramanian <vivek.balasubramanian@rutgers.edu>'
__license__   = 'MIT'


import os
import json
import pika
import queue

import threading       as mt
import multiprocessing as mp

from ...exceptions       import EnTKError
from ...                 import states, Task
from ..base.task_manager import Base_TaskManager, heartbeat_response


# pylint: disable=unused-argument
# ------------------------------------------------------------------------------
#
class TaskManager(Base_TaskManager):
    '''
    A Task Manager takes the responsibility of dispatching tasks it receives
    from a pending_queue for execution on to the available resources using a
    runtime system. Once the tasks have completed execution, they are pushed
    on to the completed_queue for other components of EnTK to process.

    :arguments:
        :pending_queue:     (list) List of queue(s) with tasks ready to be
                            executed. Currently, only one queue.
        :completed_queue:   (list) List of queue(s) with tasks that have
                            finished execution. Currently, only one queue.
        :rmgr:              (ResourceManager) Object to be used to access the
                            Pilot where the tasks can be submitted
        :rmq_conn_params:   (pika.connection.ConnectionParameters) object of
                            parameters necessary to connect to RabbitMQ

    Currently, EnTK is configured to work with one pending queue and one
    completed queue. In the future, the number of queues can be varied for
    different throughput requirements at the cost of additional Memory and CPU
    consumption.
    '''

    # --------------------------------------------------------------------------
    #
    def __init__(self, sid, pending_queue, completed_queue, rmgr,
                       rmq_conn_params):

        super(TaskManager, self).__init__(sid, pending_queue, completed_queue,
                                          rmgr, rmq_conn_params,
                                          rts='mock')
        self._rts_runner = None

        self._rmq_ping_interval = int(os.getenv('RMQ_PING_INTERVAL', '10'))

        self._log.info('Created task manager object: %s', self._uid)
        self._prof.prof('tmgr_create', uid=self._uid)


    # --------------------------------------------------------------------------
    #
    def _tmgr(self, uid, rmgr, pending_queue, completed_queue,
                    rmq_conn_params):
        '''
        **Purpose**: Method to be run by the tmgr process. This method receives
                     a Task from the pending_queue and submits it to the RTS.
                     Currently, it also converts Tasks into CUDs and CUs into
                     (partially described) Tasks.  This conversion is necessary
                     since the current RTS is RADICAL Pilot.  Once Tasks are
                     recovered from a CU, they are then pushed to the
                     completed_queue. At all state transititons, they are synced
                     (blocking) with the AppManager in the master process.

                     In addition the tmgr also receives heartbeat 'request' msgs
                     from the heartbeat-req queue. It responds with a 'response'
                     message to the 'heartbeart-res' queue.

        **Details**: The AppManager can re-invoke the tmgr process with this
                    function if the execution of the workflow is still
                    incomplete. There is also population of a dictionary,
                    `placeholders`, which stores the path of each of the tasks
                    on the remote machine.
        '''

        try:

            self._prof.prof('tmgr process started', uid=self._uid)
            self._log.info('Task Manager process started')

            # Acquire a connection+channel to the rmq server
            mq_connection = pika.BlockingConnection(rmq_conn_params)
            mq_channel = mq_connection.channel()

            # Make sure the heartbeat response queue is empty
            mq_channel.queue_delete(queue=self._hb_response_q)
            mq_channel.queue_declare(queue=self._hb_response_q)

            # Queue for communication between threads of this process
            task_queue = queue.Queue()

            # Start second thread to receive tasks and push to RTS
            self._rts_runner = mt.Thread(target=self._process_tasks,
                                         args=(task_queue, rmgr,
                                               rmq_conn_params))
            self._rts_runner.start()

            self._prof.prof('tmgr infrastructure setup done', uid=uid)

            # While we are supposed to run and the thread that does the work is
            # alive go.
            while not self._tmgr_terminate.is_set():

                try:

                    # Get tasks from the pending queue
                    method_frame, _, body = \
                                    mq_channel.basic_get(queue=pending_queue[0])

                    if body:

                        body = json.loads(body)

                        if body['type'] == 'workload':
                            task_queue.put(body['body'])
                        elif body['type'] == 'rts':
                            pass
                        else:
                            self._log.error('TMGR receiver wrong message type')

                        mq_channel.basic_ack(
                                delivery_tag=method_frame.delivery_tag)

                    heartbeat_response(mq_channel,
                                       self._hb_request_q,
                                       self._hb_response_q,
                                       conn_params=rmq_conn_params,
                                       log=self._log)

                except Exception as e:
                    self._log.exception('Error in task execution')
                    raise EnTKError(e) from e

            self._log.debug('Exited TMGR main loop')

        except KeyboardInterrupt:

            self._log.exception('Execution interrupted (probably by Ctrl+C), '
                                'cancel tmgr process gracefully...')
            raise


        except Exception as e:

            self._log.exception('task %s failed')
            raise EnTKError(e) from e

        finally:

            self._prof.prof('tmgr_term', uid=uid)

            if self._rts_runner:
                self._rts_runner.join()

            self._log.debug('TMGR RTS Runner joined')

            mq_connection.close()
            self._log.debug('TMGR RMQ connection closed')
            self._prof.close()
            self._log.debug('TMGR profile closed')


    # --------------------------------------------------------------------------
    #
    def _process_tasks(self, task_queue, rmgr, rmq_conn_params):
        '''
        **Purpose**: The new thread that gets spawned by the main tmgr process
                     invokes this function. This function receives tasks from
                     'task_queue' and submits them to the RADICAL Pilot RTS.
        '''


        mq_connection = pika.BlockingConnection(rmq_conn_params)
        mq_channel = mq_connection.channel()

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
                                  mq_channel, rmq_conn_params,
                                  '%s-tmgr-to-sync' % self._sid)


                # this mock RTS immmedialtely completes all tasks
                for task in bulk_tasks:

                    task.exit_code = 0
                    task_as_dict = json.dumps(task.as_dict())

                    self._advance(task, 'Task', states.COMPLETED,
                                  mq_channel, rmq_conn_params,
                                  '%s-cb-to-sync' % self._sid)

                    mq_channel.basic_publish(exchange='',
                                          routing_key='%s-completedq-1' % self._sid,
                                          body=task_as_dict)

                    self._log.info('Pushed task %s with state %s to completed '
                                   'queue %s-completedq-1',
                                   task.uid, task.state, self._sid)

        except KeyboardInterrupt:
            self._log.exception('Execution interrupted (probably by Ctrl+C), '
                                'cancel task processor gracefully...')
            raise

        except Exception as e:
            self._log.exception('%s failed', self._uid)
            raise EnTKError(e) from e


    # --------------------------------------------------------------------------
    #
    def start_manager(self):
        '''
        **Purpose**: Method to start the tmgr process. The tmgr function
                     is not to be accessed directly. The function is started
                     in a separate thread using this method.
        '''
        # pylint: disable=attribute-defined-outside-init, access-member-before-definition
        if self._tmgr_process:
            self._log.warn('tmgr process already running!')
            return

        try:
            # Redeclare the heartbeat queues in case they got deleted because
            # of the task manager failure.
            # If the queues exist this has no effect.

            mq_connection = pika.BlockingConnection(self._rmq_conn_params)
            mq_channel = mq_connection.channel()

            mq_channel.queue_declare(queue=self._hb_response_q)
            mq_channel.queue_declare(queue=self._hb_request_q)

            self._prof.prof('creating tmgr process', uid=self._uid)
            self._tmgr_terminate = mp.Event()

            self._tmgr_process = mp.Process(target=self._tmgr,
                                            name='task-manager',
                                            args=(self._uid,
                                                  self._rmgr,
                                                  self._pending_queue,
                                                  self._completed_queue,
                                                  self._rmq_conn_params)
                                            )

            self._log.info('Starting task manager process')
            self._prof.prof('starting tmgr process', uid=self._uid)

            self._tmgr_process.start()
            self._log.debug('tmgr pid %s' % self._tmgr_process.pid)

            return True

        except Exception as e:

            self._log.exception('Task manager not started')
            self.terminate_manager()
            raise EnTKError(e) from e


# ------------------------------------------------------------------------------
# pylint: enable=attribute-defined-outside-init, access-member-before-definition
