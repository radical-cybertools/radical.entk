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
import traceback
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

    def __init__(self, sid, pending_queue, completed_queue,
                 rmgr, mq_hostname, port, rts):

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
        self._logger = ru.get_logger('radical.entk.%s' % self._uid, path=self._path)
        self._prof = ru.Profiler(name='radical.entk.%s' % self._uid + '-obj', path=self._path)


        self._hb_request_q = '%s-hb-request' % self._sid
        self._hb_response_q = '%s-hb-response' % self._sid

        self._tmgr_process = None
        self._hb_thread = None

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

        raise NotImplementedError(msg='_heartbeat() method ' +
                                  'not implemented in TaskManager for %s' % self._rts_type)

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

        raise NotImplementedError(msg='_tmgr() method ' +
                                  'not implemented in TaskManager for %s' % self._rts_type)

    # ------------------------------------------------------------------------------------------------------------------
    # Public Methods
    # ------------------------------------------------------------------------------------------------------------------

    def start_heartbeat(self):
        """
        **Purpose**: Method to start the heartbeat thread. The heartbeat function
        is not to be accessed directly. The function is started in a separate
        thread using this method.
        """

        raise NotImplementedError(msg='start_heartbeat() method ' +
                                  'not implemented in TaskManager for %s' % self._rts_type)

    def terminate_heartbeat(self):
        """
        **Purpose**: Method to terminate the heartbeat thread. This method is 
        blocking as it waits for the heartbeat thread to terminate (aka join).

        This is the last method that is executed from the TaskManager and
        hence closes the profiler.
        """

        raise NotImplementedError(msg='terminate_heartbeat() method ' +
                                  'not implemented in TaskManager for %s' % self._rts_type)

    def start_manager(self):
        """
        **Purpose**: Method to start the tmgr process. The tmgr function
        is not to be accessed directly. The function is started in a separate
        thread using this method.
        """

        raise NotImplementedError(msg='start_manager() method ' +
                                  'not implemented in TaskManager for %s' % self._rts_type)

    def terminate_manager(self):
        """
        **Purpose**: Method to terminate the tmgr process. This method is 
        blocking as it waits for the tmgr process to terminate (aka join).
        """

        raise NotImplementedError(msg='terminate_manager() method ' +
                                  'not implemented in TaskManager for %s' % self._rts_type)

    def check_manager(self):
        """
        **Purpose**: Check if the tmgr process is alive and running
        """

        raise NotImplementedError(msg='check_tmgr() method ' +
                                  'not implemented in TaskManager for %s' % self._rts_type)

    def check_heartbeat(self):
        """
        **Purpose**: Check if the heartbeat thread is alive and running
        """

        raise NotImplementedError(msg='check_heartbeat() method ' +
                                  'not implemented in TaskManager for %s' % self._rts_type)

    # ------------------------------------------------------------------------------------------------------------------
