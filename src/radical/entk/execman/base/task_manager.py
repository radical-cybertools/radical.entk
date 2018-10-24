__copyright__ = "Copyright 2017-2018, http://radical.rutgers.edu"
__author__ = "Vivek Balasubramanian <vivek.balasubramaniana@rutgers.edu>"
__license__ = "MIT"

import radical.utils as ru
from radical.entk.exceptions import *
import threading
from multiprocessing import Process, Event
import Queue
import time
import json
import pika
import os
import uuid
from resource_manager import Base_ResourceManager


class Base_TaskManager(object):

    """
    A Task Manager takes the responsibility of dispatching tasks it receives from a pending_queue for execution on to 
    the available resources using a runtime system. Once the tasks have completed execution, they are pushed on to 
    the completed_queue for other components of EnTK to process.

    :arguments:
        :pending_queue: List of queue(s) with tasks ready to be executed. Currently, only one queue.
        :completed_queue: List of queue(s) with tasks that have finished execution. Currently, only one queue.
        :rmgr: ResourceManager object to be used to access the Pilot where the tasks can be submitted
        :mq_hostname: Name of the host where RabbitMQ is running
        :port: port at which rabbitMQ can be accessed

    Currently, EnTK is configured to work with one pending queue and one completed queue. In the future, the number of 
    queues can be varied for different throughput requirements at the cost of additional Memory and CPU consumption.
    """

    def __init__(self,
                 sid,
                 pending_queue,
                 completed_queue,
                 rmgr,
                 mq_hostname,
                 port,
                 rts):

        if isinstance(sid, str):
            self._sid = sid
        else:
            raise TypeError(expected_type=str, actual_type=type(sid))

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

        if isinstance(rmgr, Base_ResourceManager):
            self._rmgr = rmgr
        else:
            raise TypeError(expected_type=ResourceManager, actual_type=type(rmgr))

        self._rts = rts

        # Utility parameters
        self._uid = ru.generate_id('task_manager.%(item_counter)04d', ru.ID_CUSTOM, namespace=self._sid)
        self._path = os.getcwd() + '/' + self._sid
        self._logger = ru.Logger('radical.entk.%s' %
                                 self._uid, path=self._path, targets=['2', '.'])
        self._prof = ru.Profiler(name='radical.entk.%s' % self._uid + '-obj', path=self._path)

        # Thread should run till terminate condtion is encountered
        mq_connection = pika.BlockingConnection(pika.ConnectionParameters(host=mq_hostname, port=port))

        self._hb_request_q = '%s-hb-request' % self._sid
        self._hb_response_q = '%s-hb-response' % self._sid

        mq_channel = mq_connection.channel()

        # To respond to heartbeat - get request from rpc_queue
        mq_channel.queue_delete(queue=self._hb_response_q)
        mq_channel.queue_declare(queue=self._hb_response_q)

        # To respond to heartbeat - get request from rpc_queue
        mq_channel.queue_delete(queue=self._hb_request_q)
        mq_channel.queue_declare(queue=self._hb_request_q)

        self._tmgr_process = None
        self._hb_thread = None
        self._hb_interval = int(os.getenv('ENTK_HB_INTERVAL', 30))

        mq_connection.close()

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

            mq_connection = pika.BlockingConnection(pika.ConnectionParameters(host=self._mq_hostname, port=self._port))
            mq_channel = mq_connection.channel()

            response = True
            while (response and (not self._hb_terminate.is_set())):
                response = False
                corr_id = str(uuid.uuid4())

                # Heartbeat request signal sent to task manager via rpc-queue
                mq_channel.basic_publish(exchange='',
                                         routing_key=self._hb_request_q,
                                         properties=pika.BasicProperties(
                                             reply_to=self._hb_response_q,
                                             correlation_id=corr_id),
                                         body='request')
                self._logger.info('Sent heartbeat request')

                # mq_connection.close()

                # Sleep for hb_interval and then check if tmgr responded
                mq_connection.sleep(self._hb_interval)

                # mq_connection = pika.BlockingConnection(
                #     pika.ConnectionParameters(host=self._mq_hostname, port=self._port))
                # mq_channel = mq_connection.channel()

                method_frame, props, body = mq_channel.basic_get(queue=self._hb_response_q)

                if body:
                    if corr_id == props.correlation_id:
                        self._logger.info('Received heartbeat response')
                        response = True

                        mq_channel.basic_ack(delivery_tag=method_frame.delivery_tag)

                # Appease pika cos it thinks the connection is dead
                # mq_connection.close()

        except KeyboardInterrupt:
            self._logger.error('Execution interrupted by user (you probably hit Ctrl+C), ' +
                               'trying to cancel tmgr process gracefully...')
            raise KeyboardInterrupt

        except Exception as ex:
            self._logger.exception('Heartbeat failed with error: %s' % ex)
            raise

        finally:

            try:
                mq_connection.close()
            except:
                self._logger.warning('mq_connection not created')

            self._prof.prof('terminating heartbeat thread', uid=self._uid)

    def _tmgr(self, uid, rmgr, logger, mq_hostname, port, pending_queue, completed_queue):
        """
        **Purpose**: Method to be run by the tmgr process. This method receives a Task from the pending_queue
        and submits it to the RTS. At all state transititons, they are synced (blocking) with the AppManager
        in the master process.

        In addition, the tmgr also receives heartbeat 'request' msgs from the heartbeat-req queue. It responds with a
        'response' message to the 'heartbeart-res' queue.

        **Details**: The AppManager can re-invoke the tmgr process with this function if the execution of the workflow is 
        still incomplete. There is also population of a dictionary, placeholder_dict, which stores the path of each of
        the tasks on the remote machine. 
        """

        raise NotImplementedError('_tmgr() method ' +
                                  'not implemented in TaskManager for %s' % self._rts)

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
                self._hb_terminate = threading.Event()
                self._hb_thread = threading.Thread(target=self._heartbeat, name='heartbeat')

                self._prof.prof('starting heartbeat thread', uid=self._uid)
                self._hb_thread.start()

                return True

            except Exception, ex:

                self._logger.error('Heartbeat not started, error: %s' % ex)
                self.terminate_heartbeat()
                raise

        else:
            self._logger.warn('Heartbeat thread already running, but attempted to restart!')

    def terminate_heartbeat(self):
        """
        **Purpose**: Method to terminate the heartbeat thread. This method is 
        blocking as it waits for the heartbeat thread to terminate (aka join).

        This is the last method that is executed from the TaskManager and
        hence closes the profiler.
        """

        try:

            if self._hb_thread:

                self._hb_terminate.set()

                if self.check_heartbeat():
                    self._hb_thread.join()

                self._hb_thread = None

                self._logger.info('Hearbeat thread terminated')

                self._prof.prof('heartbeat thread terminated', uid=self._uid)

                # We close in the heartbeat because it ends after the tmgr process
                self._prof.close()

        except Exception, ex:
            self._logger.error('Could not terminate heartbeat thread')
            raise

        finally:

            if not (self.check_heartbeat() or self.check_manager()):

                mq_connection = pika.BlockingConnection(pika.ConnectionParameters(host=self._mq_hostname, port=self._port))
                mq_channel = mq_connection.channel()

                # To respond to heartbeat - get request from rpc_queue
                mq_channel.queue_delete(queue=self._hb_response_q)
                mq_channel.queue_delete(queue=self._hb_request_q)

                mq_connection.close()

    def start_manager(self):
        """
        **Purpose**: Method to start the tmgr process. The tmgr function
        is not to be accessed directly. The function is started in a separate
        thread using this method.
        """

        raise NotImplementedError('start_manager() method ' +
                                  'not implemented in TaskManager for %s' % self._rts)

    def terminate_manager(self):
        """
        **Purpose**: Method to terminate the tmgr process. This method is 
        blocking as it waits for the tmgr process to terminate (aka join).
        """

        try:

            if self._tmgr_process:

                if not self._tmgr_terminate.is_set():
                    self._tmgr_terminate.set()

                if self.check_manager():
                    self._tmgr_process.join()

                self._tmgr_process = None
                self._logger.info('Task manager process closed')

                self._prof.prof('tmgr process terminated', uid=self._uid)

        except Exception, ex:
            self._logger.error('Could not terminate task manager process')
            raise



    def check_heartbeat(self):
        """
        **Purpose**: Check if the heartbeat thread is alive and running
        """
        if self._hb_thread:
            return self._hb_thread.is_alive()
        else:
            return None

    def check_manager(self):
        """
        **Purpose**: Check if the tmgr process is alive and running
        """

        if self._tmgr_process:
            return self._tmgr_process.is_alive()
        else:
            return None

    # ------------------------------------------------------------------------------------------------------------------
