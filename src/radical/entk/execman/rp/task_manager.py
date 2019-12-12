
__copyright__ = "Copyright 2017-2018, http://radical.rutgers.edu"
__author__    = "Vivek Balasubramanian <vivek.balasubramanian@rutgers.edu>"
__license__   = "MIT"


import os
import json
import pika
import queue

import threading       as mt
import multiprocessing as mp

import radical.pilot   as rp

from ...exceptions       import EnTKError
from ...                 import states, Task
from ..base.task_manager import Base_TaskManager
from .task_processor     import create_cud_from_task, create_task_from_cu


# ------------------------------------------------------------------------------
#
class TaskManager(Base_TaskManager):
    """
    A Task Manager takes the responsibility of dispatching tasks it receives
    from a queue for execution on to the available resources using a runtime
    system. In this case, the runtime system being used RADICAL Pilot. Once
    the tasks have completed execution, they are pushed on to another queue for
    other components of EnTK to access.


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
    """

    # --------------------------------------------------------------------------
    #
    def __init__(self, sid, pending_queue, completed_queue, rmgr,
                       rmq_conn_params):

        super(TaskManager, self).__init__(sid, pending_queue, completed_queue,
                                          rmgr, rmq_conn_params,
                                          rts='radical.pilot')
        self._umgr       = None
        self._rts_runner = None

        self._rmq_ping_interval = os.getenv('RMQ_PING_INTERVAL', 10)

        self._log.info('Created task manager object: %s', self._uid)
        self._prof.prof('tmgr_create', uid=self._uid)


    # --------------------------------------------------------------------------
    #
    def _tmgr(self, uid, umgr, rmgr, pending_queue, completed_queue,
                    rmq_conn_params):
        """
        **Purpose**: This method has 3 purposes: Respond to a heartbeat thread
                     indicating the live-ness of the RTS, receive tasks from the
                     pending_queue, start a new thread that processes these
                     tasks and submits to the RTS.

                     It is important to separate the reception of the tasks from
                     their processing due to RMQ/AMQP design. The channel (i.e.,
                     the thread that holds the channel) that is receiving msgs
                     from the RMQ server needs to be non-blocking as it can
                     interfere with the heartbeat intervals of RMQ.  Processing
                     a large number of tasks can considerable time and can block
                     the communication channel. Hence, the two are separated.

                     The new thread is responsible for pushing completed tasks
                     (returned by the RTS) to the dequeueing queue. It also
                     converts Tasks into CUDs and CUs into (partially described)
                     Tasks. This conversion is necessary since the current RTS
                     is RADICAL Pilot. Once Tasks are recovered from a CU, they
                     are then pushed to the completed_queue. At all state
                     transititons, they are synced (blocking) with the
                     AppManager in the master process.

                     In addition the tmgr also receives heartbeat 'request' msgs
                     from the heartbeat-request queue. It responds with
                     a 'response' message to the heartbeart-response queue.

        **Details**: The AppManager can re-invoke the tmgr process with this
                     function if the execution of the workflow is still
                     incomplete. There is also population of a dictionary,
                     `placeholders`, which stores the path of each of the
                     tasks on the remote machine.
        """

        try:

            # ------------------------------------------------------------------
            def heartbeat_response(mq_channel):

                try:

                    # Get request from heartbeat-req for heartbeat response
                    method_frame, props, body = \
                                  mq_channel.basic_get(queue=self._hb_request_q)

                    if not body:
                        return

                    self._log.info('Received heartbeat request')

                    nprops = pika.BasicProperties(
                                            correlation_id=props.correlation_id)
                    mq_channel.basic_publish(
                                exchange='',
                                routing_key=self._hb_response_q,
                                properties=nprops,
                                body='response')

                    self._log.info('Sent heartbeat response')

                    mq_channel.basic_ack(delivery_tag=method_frame.delivery_tag)


                except Exception as e:
                    self._log.exception('Failed to respond to heartbeat, '
                                        'error: %s', e)
                    raise
            # ------------------------------------------------------------------

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

            while not self._tmgr_terminate.is_set():

                try:

                    # Get tasks from the pending queue
                    method_frame, header_frame, body = \
                                    mq_channel.basic_get(queue=pending_queue[0])

                    if body:

                        body = json.loads(body)
                        task_queue.put(body)

                        mq_channel.basic_ack(
                                delivery_tag=method_frame.delivery_tag)

                    heartbeat_response(mq_channel)

                except Exception as e:
                    self._log.exception('Error in task execution: %s', e)
                    raise
            self._log.debug('Exited TMGR main loop')

        except KeyboardInterrupt:

            self._log.exception('Execution interrupted (probably by Ctrl+C), '
                                'cancel tmgr process gracefully...')
            raise


        except Exception as e:

            self._log.exception('%s failed with %s', self._uid, e)
            raise EnTKError(e)

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

        placeholders = dict()

        # ----------------------------------------------------------------------
        def load_placeholder(task, rts_uid):

            parent_pipeline = str(task.parent_pipeline['name'])
            parent_stage    = str(task.parent_stage['name'])

            if parent_pipeline not in placeholders:
                placeholders[parent_pipeline] = dict()

            if parent_stage not in placeholders[parent_pipeline]:
                placeholders[parent_pipeline][parent_stage] = dict()

            if None not in [parent_pipeline, parent_stage, task.name]:
                placeholders[parent_pipeline][parent_stage][task.name] = \
                                                          {'path'   : task.path,
                                                           'rts_uid': rts_uid}

        # ----------------------------------------------------------------------
        def unit_state_cb(unit, state):

            try:

                self._log.debug('Unit %s in state %s' % (unit.uid, unit.state))

                if unit.state in rp.FINAL:

                    # Acquire a connection+channel to the rmq server
                    mq_connection = pika.BlockingConnection(rmq_conn_params)
                    mq_channel = mq_connection.channel()

                    task = None
                    task = create_task_from_cu(unit, self._prof)

                    self._advance(task, 'Task', states.COMPLETED,
                                  mq_channel, '%s-cb-to-sync' % self._sid)

                    load_placeholder(task, unit.uid)

                    task_as_dict = json.dumps(task.to_dict())

                    mq_channel.basic_publish(
                            exchange='',
                            routing_key='%s-completedq-1' % self._sid,
                            body=task_as_dict)

                    self._log.info('Pushed task %s with state %s to completed '
                                   'queue %s-completedq-1',
                                   task.uid, task.state, self._sid)

                    mq_connection.close()

            except KeyboardInterrupt:
                self._log.exception('Execution interrupted (probably by Ctrl+C)'
                                    ' exit callback thread gracefully...')
                raise KeyboardInterrupt

            except Exception as e:
                self._log.exception('Error in RP callback thread: %s', e)
        # ----------------------------------------------------------------------


        umgr = rp.UnitManager(session=rmgr._session)
        umgr.add_pilots(rmgr.pilot)
        umgr.register_callback(unit_state_cb)

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
                bulk_cuds  = list()

                for msg in body:

                    task = Task()
                    task.from_dict(msg)
                    bulk_tasks.append(task)
                    bulk_cuds.append(create_cud_from_task(
                                            task, placeholders, self._prof))

                    mq_connection = pika.BlockingConnection(rmq_conn_params)
                    mq_channel = mq_connection.channel()

                    self._advance(task, 'Task', states.SUBMITTING,
                                  mq_channel, '%s-tmgr-to-sync' % self._sid)
                    mq_connection.close()

                umgr.submit_units(bulk_cuds)
            self._log.debug('Exited RTS main loop. TMGR terminating')
        except KeyboardInterrupt:
            self._log.exception('Execution interrupted (probably by Ctrl+C), '
                                'cancel task processor gracefully...')

        except Exception as e:
            self._log.exception('%s failed with %s', self._uid, e)
            raise EnTKError(e)

        finally:
            umgr.close()


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
                                                  self._umgr,
                                                  self._rmgr,
                                                  self._pending_queue,
                                                  self._completed_queue,
                                                  self._rmq_conn_params)
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

