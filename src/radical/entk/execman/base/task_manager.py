
__copyright__ = "Copyright 2017-2018, http://radical.rutgers.edu"
__author__    = "Vivek Balasubramanian <vivek.balasubramanian@rutgers.edu>"
__license__   = "MIT"


import os
import json
import pika
import uuid

import threading     as mt
import radical.utils as ru

from ...exceptions import EnTKError, TypeError

from .resource_manager import Base_ResourceManager


# ------------------------------------------------------------------------------
#
class Base_TaskManager(object):
    """
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
    """

    # --------------------------------------------------------------------------
    #
    def __init__(self, sid, pending_queue, completed_queue, rmgr,
                       rmq_conn_params, rts):

        if not isinstance(sid, str):
            raise TypeError(expected_type=str,
                            actual_type=type(sid))

        if not isinstance(pending_queue, list):
            raise TypeError(expected_type=str,
                            actual_type=type(pending_queue))

        if not isinstance(completed_queue, list):
            raise TypeError(expected_type=str,
                            actual_type=type(completed_queue))

        if not isinstance(rmgr, Base_ResourceManager):
            raise TypeError(expected_type=Base_ResourceManager,
                            actual_type=type(rmgr))

        if not isinstance(rmq_conn_params,
                            pika.connection.ConnectionParameters):
            raise TypeError(expected_type=pika.connection.ConnectionParameters,
                            actual_type=type(rmq_conn_params))

        self._sid             = sid
        self._pending_queue   = pending_queue
        self._completed_queue = completed_queue
        self._rmgr            = rmgr
        self._rts             = rts
        self._rmq_conn_params = rmq_conn_params

        # Utility parameters
        self._uid  = ru.generate_id('task_manager.%(counter)04d', ru.ID_CUSTOM)
        self._path = os.getcwd() + '/' + self._sid

        name = 'radical.entk.%s' % self._uid
        self._log  = ru.Logger  (name, path=self._path)
        self._prof = ru.Profiler(name, path=self._path)
        self._dh = ru.DebugHelper(name=name)

        # Thread should run till terminate condtion is encountered
        mq_connection = pika.BlockingConnection(rmq_conn_params)

        self._hb_request_q  = '%s-hb-request'  % self._sid
        self._hb_response_q = '%s-hb-response' % self._sid

        mq_channel = mq_connection.channel()

        # To respond to heartbeat - get request from rpc_queue
        mq_channel.queue_delete(queue=self._hb_response_q)
        mq_channel.queue_declare(queue=self._hb_response_q)

        # To respond to heartbeat - get request from rpc_queue
        mq_channel.queue_delete(queue=self._hb_request_q)
        mq_channel.queue_declare(queue=self._hb_request_q)

        self._tmgr_process = None
        self._tmgr_terminate = None
        self._hb_thread    = None
        self._hb_terminate = None
        self._hb_interval  = int(os.getenv('ENTK_HB_INTERVAL', '30'))

        mq_connection.close()


    # --------------------------------------------------------------------------
    #
    def _tmgr(self, uid, rmgr, pending_queue, completed_queue,
                    rmq_conn_params):
        """
        **Purpose**: Method to be run by the tmgr process. This method receives
                     a Task from the pending_queue and submits it to the RTS.
                     At all state transititons, they are synced (blocking) with
                     the AppManager in the master process.

                     In addition, the tmgr also receives heartbeat 'request'
                     msgs from the heartbeat-req queue. It responds with
                     a 'response' message to the 'heartbeart-res' queue.

        **Details**: The AppManager can re-invoke the tmgr process with this
                     function if the execution of the workflow is still
                     incomplete. There is also population of a dictionary,
                     placeholder_dict, which stores the path of each of the
                     tasks on the remote machine.
        """

        raise NotImplementedError('_tmgr() method not implemented in '
                                  'TaskManager for %s' % self._rts)


    # --------------------------------------------------------------------------
    #
    def _sync_with_master(self, obj, obj_type, channel, queue):

        corr_id = str(uuid.uuid4())
        body    = json.dumps({'object': obj.to_dict(),
                              'type'  : obj_type})

        if   obj_type == 'Task' : msg = obj.parent_stage['uid']
        elif obj_type == 'Stage': msg = obj.parent_pipeline['uid']
        else                    : msg = ''

        self._prof.prof('pub_sync', state=obj.state, uid=obj.uid, msg=msg)
        self._log.debug('%s (%s) to sync with amgr', obj.uid, obj.state)

        channel.basic_publish(exchange='', routing_key=queue, body=body,
                        properties=pika.BasicProperties(correlation_id=corr_id))

        # all queue name parts up to the last three are used as sid, the last
        # three parts are channel specifiers which need to be inversed to obtain
        # the target channel.
        sid         = '-'.join(queue.split('-')[:-3])
        qname       = queue.split('-')[-3:]
        reply_queue = '-'.join(list(reversed(qname)))
        reply_queue = sid + '-' + reply_queue

        # The `while` loop is diabled with PR #466, and the explanation is:
        #  The task manager and app manager continue to have ack semantics with
        #  this PR. They exchange messages to sync through two channels,
        #  sync-to-tmgr and sync-to-cb, these have not been touched. The ack
        #  that located here is to  send an acknowledgment from the app manager
        #  to the task manager although it doesn't take any further action. 
        #  This is redundant and doesn't break or reduce reliability by
        #  commenting out.

        # while True:

        #     # FIXME: is this a busy loop?

        #     method_frame, props, body = channel.basic_get(queue=reply_queue)

        #     if not body:
        #         continue

        #     if corr_id != props.correlation_id:
        #         continue

        #     channel.basic_ack(delivery_tag=method_frame.delivery_tag)

        #     self._prof.prof('sync', state=obj.state, uid=obj.uid, msg=msg)
        #     self._log.debug('%s (%s) synced with amgr', obj.uid, obj.state)

        #     break

    # --------------------------------------------------------------------------
    #
    def _advance(self, obj, obj_type, new_state, channel, queue):

        try:
            old_state = obj.state
            obj.state = new_state

            if   obj_type == 'Task' : msg = obj.parent_stage['uid']
            elif obj_type == 'Stage': msg = obj.parent_pipeline['uid']
            else                    : msg = None

            self._prof.prof('advance', uid=obj.uid, state=obj.state, msg=msg)
            self._log.info('Transition %s to %s', obj.uid, new_state)

            self._sync_with_master(obj, obj_type, channel, queue)


        except Exception as ex:
            self._log.exception('Transition %s to state %s failed, error: %s',
                                obj.uid, new_state, ex)
            obj.state = old_state
            self._sync_with_master(obj, obj_type, channel, queue)
            raise


    # --------------------------------------------------------------------------
    #
    def _heartbeat(self):
        """
        **Purpose**: Method to be executed in the heartbeat thread. This method
                     sends a 'request' to the heartbeat-req queue. It expects
                     a 'response' message from the 'heartbeart-res' queue within
                     10 seconds. This message should contain the same
                     correlation id. If no message if received in 10 seconds,
                     the tmgr is assumed dead. The end_manager() is called to
                     cleanly terminate tmgr process and the heartbeat thread is
                     also terminated.

        **Details**: The AppManager can re-invoke both if the execution is still
                     not complete.
        """

        try:
            self._prof.prof('hbeat_start', uid=self._uid)

            mq_connection = pika.BlockingConnection(self._rmq_conn_params)
            mq_channel = mq_connection.channel()

            while not self._hb_terminate.is_set():

                corr_id  = str(uuid.uuid4())

                # Heartbeat request signal sent to task manager via rpc-queue
                props = pika.BasicProperties(reply_to=self._hb_response_q,
                                             correlation_id=corr_id)
                mq_channel.basic_publish(exchange='',
                                         routing_key=self._hb_request_q,
                                         properties=props,
                                         body='request')
                self._log.info('Sent heartbeat request')

                # Sleep for hb_interval and then check if tmgr responded
                mq_connection.sleep(self._hb_interval)

                method_frame, props, body = mq_channel.basic_get(
                                                      queue=self._hb_response_q)
                if not body:
                    # no usable response
                    return
                    # raise EnTKError('heartbeat timeout')

                if corr_id != props.correlation_id:
                    # incorrect response
                    return
                    # raise EnTKError('heartbeat timeout')

                self._log.info('Received heartbeat response')
                mq_channel.basic_ack(delivery_tag=method_frame.delivery_tag)

                # Appease pika cos it thinks the connection is dead
                # mq_connection.close()

        except EnTKError as e:
            # make sure that timeouts did not race with termination
            if 'heartbeat timeout' not in str(e):
                raise

            if not self._hb_terminate.is_set():
                raise

            # we did indeed race with termination - exit gracefully
            return

        except KeyboardInterrupt as e:
            self._log.exception('Execution interrupted by user (probably '
                                   ' hit Ctrl+C), cancel tmgr gracefully...')
            raise KeyboardInterrupt from e

        except Exception as e:
            self._log.exception('Heartbeat failed with error: %s', e)
            raise

        finally:
            try:
                mq_connection.close()
            except:
                self._log.warning('mq_connection not closed')

            self._prof.prof('hbeat_stop', uid=self._uid)


    # --------------------------------------------------------------------------
    #
    def start_heartbeat(self):
        """
        **Purpose**: Method to start the heartbeat thread. The heartbeat
                     function is not to be accessed directly. The function
                     is started in a separate thread using this method.
        """

        if self._hb_thread:
            self._log.warn('Heartbeat thread already running!')
            return

        try:
            self._log.info('Starting heartbeat thread')
            self._prof.prof('hbeat_create', uid=self._uid)

            self._hb_terminate = mt.Event()
            self._hb_thread    = mt.Thread(target=self._heartbeat,
                                           name='heartbeat')

            self._prof.prof('starting heartbeat thread', uid=self._uid)
            self._hb_thread.start()

            return True

        except Exception as e:

            self._log.exception('Heartbeat not started, error: %s', e)
            self.terminate_heartbeat()
            raise


    # --------------------------------------------------------------------------
    #
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

                self._log.info('Hearbeat thread terminated')
                self._prof.prof('hbeat_term', uid=self._uid)


        except Exception:
            self._log.exception('Could not terminate heartbeat thread')
            raise


        finally:

            if not self.check_heartbeat() or self.check_manager():

                conn = pika.BlockingConnection(self._rmq_conn_params)
                mq_channel = conn.channel()

                # To respond to heartbeat - get request from rpc_queue
                mq_channel.queue_delete(queue=self._hb_response_q)
                mq_channel.queue_delete(queue=self._hb_request_q)

                conn.close()


    # --------------------------------------------------------------------------
    #
    def start_manager(self):
        """
        **Purpose**: Method to start the tmgr process. The tmgr function
        is not to be accessed directly. The function is started in a separate
        thread using this method.
        """

        raise NotImplementedError('start_manager() method not implemented in '
                                  'TaskManager for %s' % self._rts)


    # --------------------------------------------------------------------------
    #
    def terminate_manager(self):
        """
        **Purpose**: Method to terminate the tmgr process. This method is
        blocking as it waits for the tmgr process to terminate (aka join).
        """

        try:
            if self._tmgr_process:
                self._log.debug('Trying to terminate task manager.')
                if self._tmgr_terminate is not None:
                    if not self._tmgr_terminate.is_set():
                        self._tmgr_terminate.set()
                    self._log.debug('TMGR terminate is set %s' % self._tmgr_terminate.is_set())
                if self.check_manager():
                    self._log.debug('TMGR process is alive')
                    self._tmgr_process.join(30)
                self._log.debug('TMGR process joined')
                self._tmgr_process = None

                self._log.info('Task manager process closed')
                self._prof.prof('tmgr_term', uid=self._uid)

        except Exception:
            self._log.exception('Could not terminate task manager process')
            raise


    # --------------------------------------------------------------------------
    #
    def check_heartbeat(self):
        """
        **Purpose**: Check if the heartbeat thread is alive and running
        """

        if self._hb_thread:
            return self._hb_thread.is_alive()


    # --------------------------------------------------------------------------
    #
    def check_manager(self):
        """
        **Purpose**: Check if the tmgr process is alive and running
        """

        if self._tmgr_process:
            return self._tmgr_process.is_alive()


# ------------------------------------------------------------------------------

