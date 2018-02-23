__copyright__ = "Copyright 2017-2018, http://radical.rutgers.edu"
__author__ = "Vivek Balasubramanian <vivek.balasubramaniana@rutgers.edu>"
__license__ = "MIT"

import radical.utils as ru
from radical.entk.exceptions import *
import threading
from multiprocessing import Process, Event
import Queue
from radical.entk import states, Task
from radical.entk.execman.resource_manager import ResourceManager
from radical.entk.utils.init_transition import transition
import time
import json
import pika
import traceback
import os
import radical.pilot as rp
from task_processor import create_cud_from_task, create_task_from_cu
import uuid


class TaskManager(object):

    """
    A Task Manager takes the responsibility of dispatching tasks it receives from a queue for execution on to 
    the available resources using a runtime system. In this case, the runtime system being used RADICAL Pilot. Once 
    the tasks have completed execution, they are pushed on to another queue for other components of EnTK to access.


    :arguments:
        :pending_queue: List of queue(s) with tasks ready to be executed. Currently, only one queue.
        :completed_queue: List of queue(s) with tasks that have finished execution. Currently, only one queue.
        :rmgr: ResourceManager object to be used to access the Pilot where the tasks can be submitted
        :mq_hostname: Name of the host where RabbitMQ is running
        :port: port at which rabbitMQ can be accessed

    Currently, EnTK is configured to work with one pending queue and one completed queue. In the future, the number of 
    queues can be varied for different throughput requirements at the cost of additional Memory and CPU consumption.
    """

    def __init__(self, sid, pending_queue, completed_queue, rmgr, mq_hostname, port):

        if isinstance(sid, str):
            self._sid = sid
        else:
            raise TypeError(expected_type=str, actual_type=type(sid))

        self._uid = ru.generate_id('radical.entk.task_manager.%(item_counter)04d', ru.ID_CUSTOM, namespace=self._sid)
        self._path = os.getcwd() + '/' + self._sid

        self._logger = ru.get_logger(self._uid, path=self._path)
        self._prof = ru.Profiler(name=self._uid + '-obj', path=self._path)

        self._prof.prof('create tmgr obj', uid=self._uid)

        if isinstance(pending_queue, list):
            self._pending_queue = pending_queue
        else:
            raise TypeError(expected_type=str, actual_type=type(pending_queue))

        if isinstance(completed_queue, list):
            self._completed_queue = completed_queue
        else:
            raise TypeError(expected_type=str, actual_type=type(completed_queue))

        if isinstance(mq_hostname, str):
            self._mq_hostname = mq_hostname
        else:
            raise TypeError(expected_type=str, actual_type=type(mq_hostname))

        if isinstance(port, int):
            self._port = port
        else:
            raise TypeError(expected_type=int, actual_type=type(port))

        if isinstance(rmgr, ResourceManager):
            self._rmgr = rmgr
        else:
            raise TypeError(expected_type=ResourceManager, actual_type=type(rmgr))

        self._tmgr_process = None
        self._tmgr_terminate = None
        self._hb_thread = None
        self._hb_alive = None
        self._umgr = None

        self._logger.info('Created task manager object: %s' % self._uid)

        self._prof.prof('tmgr obj created', uid=self._uid)

    # ------------------------------------------------------------------------------------------------------------------
    # Private Methods
    # ------------------------------------------------------------------------------------------------------------------

    def _heartbeat(self):
        """
        **Purpose**: Method to be executed in the heartbeat thread. This method sends a 'request' to the
        heartbeat-req queue. It expects a 'response' message from the 'heartbeart-res' queue within 10 seconds. This
        message should contain the same correlation id. If no message if received in 10 seconds, the tmgr is assumed
        dead. The end_manager() is called to cleanly terminate tmgr process and the heartbeat thread is also 
        terminated.

        **Details**: The AppManager can re-invoke both if the execution is still not complete.
        """

        try:

            self._prof.prof('heartbeat thread started', uid=self._uid)

            if os.environ.get('DISABLE_RMQ_HEARTBEAT', None):
                self._mq_connection = pika.BlockingConnection(pika.ConnectionParameters(host=self._mq_hostname,
                                                                                        port=self._port,
                                                                                        heartbeat=0
                                                                                        )
                                                              )
            else:
                self._mq_connection = pika.BlockingConnection(pika.ConnectionParameters(host=self._mq_hostname,
                                                                                        port=self._port
                                                                                        )
                                                              )

            channel = self._mq_connection.channel()
            channel.queue_delete(queue='%s-heartbeat-req'%self._sid)
            channel.queue_declare(queue='%s-heartbeat-req'%self._sid)
            response = True

            while (response and (not self._hb_alive.is_set())):
                response = False
                corr_id = str(uuid.uuid4())

                # Heartbeat request signal sent to task manager via rpc-queue
                channel.basic_publish(exchange='',
                                      routing_key='%s-heartbeat-req'%self._sid,
                                      properties=pika.BasicProperties(
                                          reply_to='%s-heartbeat-res'%self._sid,
                                          correlation_id=corr_id),
                                      body='request')

                self._logger.info('Sent heartbeat request')

                # Ten second interval for heartbeat request to be responded to
                time.sleep(10)

                method_frame, props, body = channel.basic_get(queue='%s-heartbeat-res'%self._sid)

                if body:
                    if corr_id == props.correlation_id:
                        self._logger.info('Received heartbeat response')
                        response = True

                        channel.basic_ack(delivery_tag=method_frame.delivery_tag)

        except KeyboardInterrupt:
            self._logger.error('Execution interrupted by user (you probably hit Ctrl+C), ' +
                               'trying to cancel tmgr process gracefully...')
            raise KeyboardInterrupt

        except Exception as ex:
            self._logger.error('Heartbeat failed with error: %s' % ex)
            raise

        finally:

            if self._hb_thread.is_alive():
                self._hb_alive.set()

            self._prof.prof('terminating heartbeat thread', uid=self._uid)

    def _tmgr(self, uid, umgr, rmgr, logger, mq_hostname, port, pending_queue, completed_queue):
        """
        **Purpose**: Method to be run by the tmgr process. This method receives a Task from the pending_queue
        and submits it to the RTS. Currently, it also converts Tasks into CUDs and CUs into (partially described) Tasks.
        This conversion is necessary since the current RTS is RADICAL Pilot. Once Tasks are recovered from a CU, they
        are then pushed to the completed_queue. At all state transititons, they are synced (blocking) with the AppManager
        in the master process.

        In addition the tmgr also receives heartbeat 'request' msgs from the heartbeat-req queue. It responds with a
        'response' message to the 'heartbeart-res' queue.

        **Details**: The AppManager can re-invoke the tmgr process with this function if the execution of the workflow is 
        still incomplete. There is also population of a dictionary, placeholder_dict, which stores the path of each of
        the tasks on the remote machine. 
        """

        try:

            local_prof = ru.Profiler(name=self._uid + '-proc', path=self._path)

            local_prof.prof('tmgr process started', uid=self._uid)
            logger.info('Task Manager process started')

            placeholder_dict = dict()

            def load_placeholder(task):

                parent_pipeline = str(task.parent_pipeline)
                parent_stage = str(task.parent_stage)

                if parent_pipeline not in placeholder_dict:
                    placeholder_dict[parent_pipeline] = dict()

                if parent_stage not in placeholder_dict[parent_pipeline]:
                    placeholder_dict[parent_pipeline][parent_stage] = dict()

                placeholder_dict[parent_pipeline][parent_stage][str(task.uid)] = str(task.path)

            def unit_state_cb(unit, state):

                try:

                    logger.debug('Unit %s in state %s' % (unit.uid, unit.state))

                    # Thread should run till terminate condtion is encountered
                    if os.environ.get('DISABLE_RMQ_HEARTBEAT', None):
                        self._mq_connection = pika.BlockingConnection(pika.ConnectionParameters(host=mq_hostname,
                                                                                                port=port,
                                                                                                heartbeat=0
                                                                                                )
                                                                      )
                    else:
                        self._mq_connection = pika.BlockingConnection(pika.ConnectionParameters(host=mq_hostname,
                                                                                                port=port
                                                                                                )
                                                                      )
                    mq_channel = self._mq_connection.channel()

                    if unit.state in [rp.DONE, rp.FAILED]:

                        try:

                            task = None
                            task = create_task_from_cu(unit, local_prof)

                            transition(obj=task,
                                       obj_type='Task',
                                       new_state=states.COMPLETED,
                                       channel=mq_channel,
                                       queue='%s-cb-to-sync'%self._sid,
                                       profiler=local_prof,
                                       logger=logger)

                            load_placeholder(task)

                        except Exception, ex:

                            # Rollback and pass exception
                            if task:
                                self._logger.error('Task %s creation for completed cu %s failed, error: %s' %
                                                   (task.uid, unit.uid, ex))
                                task.state = states.SCHEDULED
                                transition(obj=task,
                                           obj_type='Task',
                                           new_state=states.SCHEDULED,
                                           channel=mq_channel,
                                           queue='%s-cb-to-sync'%self._sid,
                                           profiler=local_prof,
                                           logger=logger)
                            else:
                                self._logger.error(
                                    'Task creation from completed cu %s failed, error: %s' % (unit.uid, ex))

                            raise

                        task_as_dict = json.dumps(task.to_dict())

                        mq_channel.basic_publish(exchange='',
                                                 routing_key='%s-completedq-1'%self._sid,
                                                 body=task_as_dict
                                                 # properties=pika.BasicProperties(
                                                 # make message persistent
                                                 #    delivery_mode = 2,
                                                 #)
                                                 )

                        logger.info('Pushed task %s with state %s to completed queue %s' % (
                            task.uid,
                            task.state,
                            completed_queue[0])
                        )

                    self._mq_connection.close()

                except KeyboardInterrupt:
                    self._logger.error('Execution interrupted by user (you probably hit Ctrl+C), ' +
                                       'trying to exit callback thread gracefully...')

                    raise KeyboardInterrupt

                except Exception, ex:

                    self._logger.error('Error in RP callback thread: %s' % ex)
                    print traceback.format_exc()
                    # raise      # Not necessary to raise the callback thread, we don't want RP to react

            if not umgr:
                umgr = rp.UnitManager(session=rmgr._session)
                umgr.add_pilots(rmgr.pilot)
                umgr.register_callback(unit_state_cb)

            # Thread should run till terminate condtion is encountered
            if os.environ.get('DISABLE_RMQ_HEARTBEAT', None):
                self._mq_connection = pika.BlockingConnection(pika.ConnectionParameters(host=mq_hostname,
                                                                                        port=port,
                                                                                        heartbeat=0
                                                                                        )
                                                              )
            else:
                self._mq_connection = pika.BlockingConnection(pika.ConnectionParameters(host=mq_hostname,
                                                                                        port=port
                                                                                        )
                                                              )
            mq_channel = self._mq_connection.channel()

            # To respond to heartbeat - get request from rpc_queue
            mq_channel.queue_delete(queue='%s-heartbeat-res'%self._sid)
            mq_channel.queue_declare(queue='%s-heartbeat-res'%self._sid)

            '''
            # Function to be invoked upon request message
            def on_request(ch, method, props, body):

                self._logger.info('Received heartbeat request')

                ch.basic_publish(   exchange='',
                                    routing_key=props.reply_to,
                                    properties=pika.BasicProperties(correlation_id = props.correlation_id),
                                    body='response')

                self._logger.info('Sent heartbeat response')

                ch.basic_ack(delivery_tag = method.delivery_tag)
            '''

            local_prof.prof('tmgr infrastructure setup done', uid=uid)

            while not self._tmgr_terminate.is_set():

                try:

                    method_frame, header_frame, body = mq_channel.basic_get(queue=pending_queue[0])

                    if body:

                        try:

                            task = None
                            task = Task(duplicate=True)
                            task.from_dict(json.loads(body))

                            transition(obj=task,
                                       obj_type='Task',
                                       new_state=states.SUBMITTING,
                                       channel=mq_channel,
                                       queue='%s-tmgr-to-sync'%self._sid,
                                       profiler=local_prof,
                                       logger=self._logger)

                        except Exception, ex:

                            # Rollback and pass exception
                            if task:
                                self._logger.error(
                                    'Task %s preparation for submission failed, error: %s' % (task.uid, ex))
                                task.state = states.SCHEDULED
                                transition(obj=task,
                                           obj_type='Task',
                                           new_state=states.SCHEDULED,
                                           channel=mq_channel,
                                           queue='%s-tmgr-to-sync'%self._sid,
                                           profiler=local_prof,
                                           logger=self._logger)
                            else:
                                self._logger.error('Task preparation for submission failed, error: %s' % ex)

                            raise

                        try:

                            umgr.submit_units(create_cud_from_task(task, placeholder_dict, local_prof))
                            self._logger.info('Task %s, %s; submitted to RTS' % (task.uid, task.state))

                            transition(obj=task,
                                       obj_type='Task',
                                       new_state=states.SUBMITTED,
                                       channel=mq_channel,
                                       queue='%s-tmgr-to-sync'%self._sid,
                                       profiler=local_prof,
                                       logger=self._logger)

                            mq_channel.basic_ack(delivery_tag=method_frame.delivery_tag)

                        except Exception, ex:

                            # Rollback and pass exception
                            self._logger.error('Task %s submission failed, error: %s' % (task.uid, ex))
                            task.state = states.SUBMITTING
                            transition(obj=task,
                                       obj_type='Task',
                                       new_state=states.SUBMITTING,
                                       channel=mq_channel,
                                       queue='%s-tmgr-to-sync'%self._sid,
                                       profiler=local_prof,
                                       logger=self._logger)
                            raise

                except Exception, ex:

                    # Rollback and pass exception
                    logger.error('Error in task execution')
                    raise

                try:

                    # Get request from heartbeat-req for heartbeat response
                    method_frame, props, body = mq_channel.basic_get(queue='%s-heartbeat-req'%self._sid)

                    if body:

                        logger.info('Received heartbeat request')

                        mq_channel.basic_publish(exchange='',
                                                 routing_key='%s-heartbeat-res'%self._sid,
                                                 properties=pika.BasicProperties(correlation_id=props.correlation_id),
                                                 body='response')

                        logger.info('Sent heartbeat response')
                        mq_channel.basic_ack(delivery_tag=method_frame.delivery_tag)

                except Exception, ex:

                    logger.error('Failed to respond to heartbeat request, error: %s' % ex)
                    raise

            local_prof.prof('terminating tmgr process', uid=uid)
            self._mq_connection.close()
            # umgr.unregister_callback(unit_state_cb)
            local_prof.close()

        except KeyboardInterrupt:

            self._logger.error('Execution interrupted by user (you probably hit Ctrl+C), ' +
                               'trying to cancel tmgr process gracefully...')
            raise KeyboardInterrupt

        except Exception, ex:

            print traceback.format_exc()
            raise Error(text=ex)

    # ------------------------------------------------------------------------------------------------------------------
    # Public Methods
    # ------------------------------------------------------------------------------------------------------------------

    def start_heartbeat(self):
        """
        **Purpose**: Method to start the heartbeat thread. The heartbeat function
        is not to be accessed directly. The function is started in a separate
        thread using this method.
        """

        if not self._hb_thread:

            try:

                self._logger.info('Starting heartbeat thread')
                self._prof.prof('creating heartbeat thread', uid=self._uid)
                self._hb_thread = threading.Thread(target=self._heartbeat, name='heartbeat')
                self._hb_alive = threading.Event()
                self._prof.prof('starting heartbeat thread', uid=self._uid)
                self._hb_thread.start()

                return True

            except Exception, ex:

                self._logger.error('Heartbeat not started, error: %s' % ex)
                self.end_heartbeat()
                raise

        else:
            self._logger.warn('Heartbeat thread already running, but attempted to restart!')

    def end_heartbeat(self):
        """
        **Purpose**: Method to terminate the heartbeat thread. This method is 
        blocking as it waits for the heartbeat thread to terminate (aka join).

        This is the last method that is executed from the TaskManager and
        hence closes the profiler.
        """

        try:

            if self._hb_thread:

                if self._hb_thread.is_alive():
                    self._hb_alive.set()
                    self._hb_thread.join()

                self._logger.info('Hearbeat thread terminated')

                self._prof.prof('heartbeat thread terminated', uid=self._uid)

                # We close in the heartbeat because it ends after the tmgr process
                self._prof.close()

                return True

        except Exception, ex:
            self._logger.error('Could not terminate heartbeat thread')
            raise

    def start_manager(self):
        """
        **Purpose**: Method to start the tmgr process. The tmgr function
        is not to be accessed directly. The function is started in a separate
        thread using this method.
        """

        if not self._tmgr_process:

            try:

                self._prof.prof('creating tmgr process', uid=self._uid)
                self._tmgr_terminate = Event()

                self._tmgr_process = Process(target=self._tmgr,
                                             name='task-manager',
                                             args=(
                                                 self._uid,
                                                 self._umgr,
                                                 self._rmgr,
                                                 self._logger,
                                                 self._mq_hostname,
                                                 self._port,
                                                 self._pending_queue,
                                                 self._completed_queue)
                                             )

                self._logger.info('Starting task manager process')
                self._prof.prof('starting tmgr process', uid=self._uid)
                self._tmgr_process.start()

                return True

            except Exception, ex:

                self._logger.error('Task manager not started, error: %s' % ex)
                self.end_manager()
                raise

        else:
            self._logger.warn('tmgr process already running, but attempted to restart!')

    def end_manager(self):
        """
        **Purpose**: Method to terminate the tmgr process. This method is 
        blocking as it waits for the tmgr process to terminate (aka join).
        """

        try:

            if self._tmgr_process:

                if not self._tmgr_terminate.is_set():
                    self._tmgr_terminate.set()

                self._tmgr_process.join()
                self._logger.info('Task manager process closed')

                self._prof.prof('tmgr process terminated', uid=self._uid)

                return True

        except Exception, ex:
            self._logger.error('Could not terminate task manager process')
            raise

    def check_alive(self):
        """
        **Purpose**: Check if the tmgr process is alive and running
        """

        return self._tmgr_process.is_alive()

    def check_heartbeat(self):
        """
        **Purpose**: Check if the heartbeat thread is alive and running
        """

        return self._hb_thread.is_alive()

    # ------------------------------------------------------------------------------------------------------------------
