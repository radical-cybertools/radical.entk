
__copyright__ = "Copyright 2017-2018, http://radical.rutgers.edu"
__author__    = "Vivek Balasubramanian <vivek.balasubramanian@rutgers.edu>"
__license__   = "MIT"


import json
import pika
import queue
import os
import pickle

import threading       as mt
import multiprocessing as mp

import radical.pilot   as rp

from ...exceptions     import EnTKError
from ...               import states, Task
from ..base            import Base_TaskManager
from .task_processor   import create_td_from_task, create_task_from_rp


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
        self._rts_runner = None
        self._submitted_tasks = dict()
        self._log.info('Created task manager object: %s', self._uid)
        self._prof.prof('tmgr_create', uid=self._uid)
        self._rp_tmgr = None
        self._total_res = {'cores': 0,
                           'gpus': 0}


    # --------------------------------------------------------------------------
    #
    def _tmgr(self, uid, rmgr, pending_queue, completed_queue,
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
                     converts Tasks into TDs and CUs into (partially described)
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
            def heartbeat_response(mq_channel, conn_params):

                channel = mq_channel
                try:

                    # Get request from heartbeat-req for heartbeat response
                    method_frame, props, body = \
                                  channel.basic_get(queue=self._hb_request_q)

                    if not body:
                        return

                    self._log.info('Received heartbeat request')
                    try:
                        nprops = pika.BasicProperties(
                                            correlation_id=props.correlation_id)
                        channel.basic_publish(exchange='',
                                              routing_key=self._hb_response_q,
                                              properties=nprops,
                                              body='response')
                    except (pika.exceptions.ConnectionClosed,
                            pika.exceptions.ChannelClosed):
                        connection = pika.BlockingConnection(conn_params)
                        channel = connection.channel()
                        nprops = pika.BasicProperties(
                                            correlation_id=props.correlation_id)
                        channel.basic_publish(exchange='',
                                              routing_key=self._hb_response_q,
                                              properties=nprops,
                                              body='response')

                    self._log.info('Sent heartbeat response')

                    channel.basic_ack(delivery_tag=method_frame.delivery_tag)

                except Exception as ex:
                    self._log.exception('Failed to respond to heartbeat, ' +
                                        'error: %s', ex)
                    raise EnTKError(ex) from ex
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

            # Pickle file for task id history.
            # TODO: How do you take care the first execution.
            pkl_path = self._path + '/.task_submitted.pkl'
            if os.path.exists(pkl_path):
                with open(pkl_path, 'rb') as f:
                    self._submitted_tasks = pickle.load(f)

            # Start second thread to receive tasks and push to RTS
            self._rts_runner = mt.Thread(target=self._process_tasks,
                                         args=(task_queue, rmgr,
                                               rmq_conn_params))
            self._rts_runner.start()

            self._prof.prof('tmgr infrastructure setup done', uid=uid)

            # While we are supposed to run and the thread that does the work is
            # alive go.
            while not self._tmgr_terminate.is_set() and \
                      self._rts_runner.is_alive():

                try:

                    # Get tasks from the pending queue
                    method_frame, _ , body = \
                                    mq_channel.basic_get(queue=pending_queue[0])

                    if body:

                        body = json.loads(body)

                        if body['type'] == 'workload':
                            task_queue.put(body['body'])
                        elif body['type'] == 'rts':
                            self._update_resource(body['body'])
                        else:
                            self._log.error('TMGR receiver wrong message type')

                        mq_channel.basic_ack(
                                delivery_tag=method_frame.delivery_tag)

                    heartbeat_response(mq_channel, rmq_conn_params)

                except Exception as ex:
                    self._log.exception('Error in task execution: %s', ex)
                    raise EnTKError(ex) from ex
            self._log.debug('Exited TMGR main loop')

        except KeyboardInterrupt as ex:

            self._log.exception('Execution interrupted (probably by Ctrl+C), '
                                'cancel tmgr process gracefully...')
            raise KeyboardInterrupt from ex


        except Exception as ex:

            self._log.exception('%s failed with %s', self._uid, ex)
            raise EnTKError(ex) from ex

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
    def _update_resource(self, pilot):
        '''
        Update used pilot.
        '''

        # Busy wait unit RP TMGR exists. Does some other way make sense?
        self._log.debug('Adding pilot.')
        while self._rp_tmgr is None:
            pass

        curr_pilot = self._rp_tmgr.list_pilots()
        self._log.debug('Got old pilots')
        if curr_pilot:
            self._rp_tmgr.remove_pilots(pilot_ids=curr_pilot)
        self._rp_tmgr.add_pilots(pilot)

        self._total_res = {'cores': pilot['description']['cores'],
                           'gpus' : pilot['description']['gpus']}
        self._log.debug('Added new pilot')


    # --------------------------------------------------------------------------
    #
    def _process_tasks(self, task_queue, rmgr, rmq_conn_params):
        '''
        **Purpose**: The new thread that gets spawned by the main tmgr process
                     invokes this function. This function receives tasks from
                     'task_queue' and submits them to the RADICAL Pilot RTS.
        '''

        placeholders = dict()
        placeholders["__by_name__"] = dict()
        placeholders_by_name = placeholders["__by_name__"]
        placeholder_lock = mt.Lock()

        # ----------------------------------------------------------------------
        def load_placeholder(task):
            with placeholder_lock:
                parent_pipeline = str(task.parent_pipeline['uid'])
                parent_stage = str(task.parent_stage['uid'])

                if parent_pipeline not in placeholders:
                    placeholders[parent_pipeline] = dict()

                if parent_stage not in placeholders[parent_pipeline]:
                    placeholders[parent_pipeline][parent_stage] = dict()

                if None not in [parent_pipeline, parent_stage, task.uid]:
                    placeholders[parent_pipeline][parent_stage][task.uid] = \
                                                            {'path': task.path,
                                                             'uid': task.uid}

                parent_pipeline_n = str(task.parent_pipeline['name'])
                parent_stage_n = str(task.parent_stage['name'])

                if parent_pipeline_n not in placeholders_by_name:
                    placeholders_by_name[parent_pipeline_n] = dict()

                if parent_stage_n not in placeholders_by_name[parent_pipeline_n]:
                    placeholders_by_name[parent_pipeline_n][parent_stage_n] = dict()

                if None not in [parent_pipeline_n, parent_stage_n, task.name]:
                    placeholders_by_name[parent_pipeline_n][parent_stage_n][task.name] = \
                                                            {'path': task.path,
                                                             'uid': task.uid}

        # ----------------------------------------------------------------------
        def check_resource_reqs(task):

            cpu_reqs = task.cpu_reqs['cpu_processes'] * task.cpu_reqs['cpu_threads']
            gpu_reqs = task.cpu_reqs['cpu_processes'] * task.gpu_reqs['gpu_processes']

            if cpu_reqs > self._total_res['cores'] or \
               gpu_reqs > self._total_res['gpus']:
                return False
            return True

        # ----------------------------------------------------------------------
        def task_state_cb(rp_task, state, cb_data):

            try:

                channel = cb_data['channel']
                conn_params = cb_data['params']
                self._log.debug('Task %s in state %s' % (rp_task.uid,
                                                         rp_task.state))

                if rp_task.state in rp.FINAL:

                    task = create_task_from_rp(rp_task, self._log, self._prof)

                    self._advance(task, 'Task', states.COMPLETED,
                                  channel, conn_params,
                                  '%s-cb-to-sync' % self._sid)

                    load_placeholder(task)

                    task_as_dict = json.dumps(task.to_dict())
                    try:
                        channel.basic_publish(exchange='',
                                              routing_key='%s-completedq-1' % self._sid,
                                              body=task_as_dict)
                    except (pika.exceptions.ConnectionClosed,
                            pika.exceptions.ChannelClosed):
                        connection = pika.BlockingConnection(conn_params)
                        channel = connection.channel()
                        channel.basic_publish(exchange='',
                                              routing_key='%s-completedq-1' % self._sid,
                                              body=task_as_dict)

                    self._log.info('Pushed task %s with state %s to completed '
                                   'queue %s-completedq-1',
                                   task.uid, task.state, self._sid)

            except KeyboardInterrupt as ex:
                self._log.exception('Execution interrupted (probably by Ctrl+C)'
                                    ' exit callback thread gracefully...')
                raise KeyboardInterrupt(ex) from ex

            except Exception as ex:
                self._log.exception('Error in RP callback thread: %s', ex)
                raise EnTKError(ex) from ex
        # ----------------------------------------------------------------------


        mq_connection = pika.BlockingConnection(rmq_conn_params)
        mq_channel = mq_connection.channel()

        self._rp_tmgr = rp.TaskManager(session=rmgr._session)
        self._rp_tmgr.register_callback(task_state_cb,
                                  cb_data={'channel': mq_channel,
                                           'params' : rmq_conn_params})

        try:
            pkl_path = self._path + '/.task_submitted.pkl'

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

                bulk_tds   = list()

                for msg in body:

                    task = Task()
                    task.from_dict(msg)
                    task_fits_res = check_resource_reqs(task)
                    if task_fits_res:
                        load_placeholder(task)
                        bulk_tds.append(create_td_from_task(
                                            task, placeholders,
                                            self._submitted_tasks, pkl_path,
                                            self._sid, self._log, self._prof))

                        self._advance(task, 'Task', states.SUBMITTING,
                                      mq_channel, rmq_conn_params,
                                      '%s-tmgr-to-sync' % self._sid)
                    else:
                        self._advance(task, 'Task', states.FAILED,
                                      mq_channel, rmq_conn_params,
                                      '%s-cb-to-sync' % self._sid)
                        load_placeholder(task)

                        task_as_dict = json.dumps(task.to_dict())
                        try:
                            mq_channel.basic_publish(exchange='',
                                              routing_key='%s-completedq-1' % self._sid,
                                              body=task_as_dict)
                        except (pika.exceptions.ConnectionClosed,
                                pika.exceptions.ChannelClosed):
                            mq_connection = pika.BlockingConnection(rmq_conn_params)
                            mq_channel = mq_connection.channel()
                            mq_channel.basic_publish(exchange='',
                                              routing_key='%s-completedq-1' % self._sid,
                                              body=task_as_dict)
                if bulk_tds:
                    self._rp_tmgr.submit_tasks(bulk_tds)
            mq_connection.close()
            self._log.debug('Exited RTS main loop. TMGR terminating')
        except KeyboardInterrupt as ex:
            self._log.exception('Execution interrupted (probably by Ctrl+C), '
                                'cancel task processor gracefully...')
            raise KeyboardInterrupt from ex
        except Exception as ex:
            self._log.exception('%s failed with %s', self._uid, ex)
            raise EnTKError(ex) from ex

        finally:
            self._rp_tmgr.close()


    # --------------------------------------------------------------------------
    #
    def start_manager(self):
        """
        **Purpose**: Method to start the tmgr process. The tmgr function
                     is not to be accessed directly. The function is started
                     in a separate thread using this method.
        """
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

        except Exception as ex:

            self._log.exception('Task manager not started, error: %s', ex)
            self.terminate_manager()
            raise EnTKError(ex) from ex

# ------------------------------------------------------------------------------
# pylint: enable=attribute-defined-outside-init, access-member-before-definition
