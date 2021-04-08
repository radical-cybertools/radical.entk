# pylint: disable=protected-access, unused-argument
# pylint: disable=no-value-for-parameter
import os
import pika
import json
import time

from unittest import TestCase, mock
import threading as mt

import radical.utils as ru
from radical.entk              import Task, Stage
from radical.entk.execman.base import Base_TaskManager     as BaseTmgr


# ------------------------------------------------------------------------------
#
class TestTask(TestCase):

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(BaseTmgr, '__init__',   return_value=None)
    @mock.patch('radical.utils.Logger')
    @mock.patch('radical.utils.Profiler')
    def test_sync_with_master(self, mocked_init, mocked_Logger, mocked_Profiler):

        # --------------------------------------------------------------------------
        #
        def component_execution(inputs, method, channel, conn_params, queue):

            for obj_type, obj, in inputs:
                method(obj, obj_type, channel, conn_params, queue)
            return True


        task = Task()
        task.parent_stage = {'uid':'stage.0000', 'name': 'stage.0000'}
        hostname = os.environ.get('RMQ_HOSTNAME', 'localhost')
        port = int(os.environ.get('RMQ_PORT', '5672'))
        username = os.environ.get('RMQ_USERNAME','guest')
        password = os.environ.get('RMQ_PASSWORD','guest')
        packets = [('Task', task)]
        stage = Stage()
        stage.parent_pipeline = {'uid':'pipe.0000', 'name': 'pipe.0000'}
        packets.append(('Stage', stage))
        credentials = pika.PlainCredentials(username, password)
        rmq_conn_params = pika.ConnectionParameters(host=hostname, port=port,
                credentials=credentials)
        mq_connection = pika.BlockingConnection(rmq_conn_params)
        mq_channel = mq_connection.channel()
        mq_channel.queue_declare(queue='master')
        tmgr = BaseTmgr(None, None, None, None, None, None)
        tmgr._log = mocked_Logger
        tmgr._prof = mocked_Profiler

        master_thread = mt.Thread(target=component_execution,
                                  name='tmgr_sync', 
                                  args=(packets, tmgr._sync_with_master,
                                  mq_channel, rmq_conn_params, 'master'))
        master_thread.start()
        time.sleep(0.1)
        try:
            while packets:
                packet = packets.pop(0)
                _, _, body = mq_channel.basic_get(queue='master')
                msg = json.loads(body)
                self.assertEqual(msg['object'], packet[1].to_dict())
                self.assertEqual(msg['type'], packet[0])
        except Exception as ex:
            print(body)
            print(json.loads(body))
            master_thread.join()
            mq_channel.queue_delete(queue='master')
            mq_channel.close()
            mq_connection.close()
            raise ex
        else:
            master_thread.join()
            mq_channel.queue_delete(queue='master')
            mq_channel.close()
            mq_connection.close()


    # --------------------------------------------------------------------------
    #
    @mock.patch.object(BaseTmgr, '__init__',   return_value=None)
    @mock.patch('radical.utils.Profiler')
    def test_heartbeat(self, mocked_init, mocked_Profiler):


        hostname = os.environ.get('RMQ_HOSTNAME', 'localhost')
        port = int(os.environ.get('RMQ_PORT', '5672'))
        username = os.environ.get('RMQ_USERNAME','guest')
        password = os.environ.get('RMQ_PASSWORD','guest')
        credentials = pika.PlainCredentials(username, password)
        rmq_conn_params = pika.ConnectionParameters(host=hostname, port=port,
                credentials=credentials)
        mq_connection = pika.BlockingConnection(rmq_conn_params)
        mq_channel = mq_connection.channel()
        mq_channel.queue_declare(queue='heartbeat_rq')
        mq_channel.queue_declare(queue='heartbeat_res')
        tmgr = BaseTmgr(None, None, None, None, None, None)
        tmgr._uid = 'tmgr.0000'
        tmgr._rmq_conn_params = rmq_conn_params
        tmgr._hb_request_q = 'heartbeat_rq'
        tmgr._hb_response_q = 'heartbeat_res'
        tmgr._hb_interval = 2
        tmgr._hb_terminate = mt.Event()
        tmgr._heartbeat_error = mt.Event()
        tmgr._log = ru.Logger(name='radical.entk.taskmanager', ns='radical.entk')
        tmgr._prof = mocked_Profiler

        master_thread = mt.Thread(target=tmgr._heartbeat,
                                name='tmgr_heartbeat')
        master_thread.start()
        time.sleep(0.1)
        i = 0
        try:
            while i < 20:
                method_frame, props, body = mq_channel.basic_get(queue='heartbeat_rq')
                i += 1
                if body and body.decode('utf-8') == 'request':
                    nprops = pika.BasicProperties(correlation_id=props.correlation_id)
                    mq_channel.basic_publish(exchange='', routing_key='heartbeat_res',
                                                properties=nprops, body='response')
                    mq_channel.basic_ack(delivery_tag=method_frame.delivery_tag)
                    self.assertTrue(master_thread.is_alive())
                else:
                    self.assertTrue(master_thread.is_alive())
                time.sleep(0.5)
        except Exception as ex:
            raise ex
        finally:
            tmgr._hb_terminate.set()
            time.sleep(3)
            master_thread.join()
            mq_channel.queue_delete(queue='heartbeat_rq')
            mq_channel.queue_delete(queue='heartbeat_res')
            mq_channel.close()
            mq_connection.close()


    # --------------------------------------------------------------------------
    #
    @mock.patch.object(BaseTmgr, '__init__',   return_value=None)
    @mock.patch('radical.utils.Profiler')
    def test_heartbeat_error(self, mocked_init, mocked_Profiler):


        hostname = os.environ.get('RMQ_HOSTNAME', 'localhost')
        port = int(os.environ.get('RMQ_PORT', '5672'))
        username = os.environ.get('RMQ_USERNAME','guest')
        password = os.environ.get('RMQ_PASSWORD','guest')
        credentials = pika.PlainCredentials(username, password)
        rmq_conn_params = pika.ConnectionParameters(host=hostname, port=port,
                credentials=credentials)
        mq_connection = pika.BlockingConnection(rmq_conn_params)
        mq_channel = mq_connection.channel()
        mq_channel.queue_declare(queue='heartbeat_rq')
        mq_channel.queue_declare(queue='heartbeat_res')
        tmgr = BaseTmgr(None, None, None, None, None, None)
        tmgr._uid = 'tmgr.0000'
        tmgr._rmq_conn_params = rmq_conn_params
        tmgr._hb_request_q = 'heartbeat_rq'
        tmgr._hb_response_q = 'heartbeat_res'
        tmgr._hb_interval = 2
        tmgr._hb_terminate = mt.Event()
        tmgr._log = ru.Logger(name='radical.entk.taskmanager', ns='radical.entk')
        tmgr._prof = mocked_Profiler

        master_thread = mt.Thread(target=tmgr._heartbeat,
                                name='tmgr_heartbeat')
        master_thread.start()

        body = None
        while not body:
            method_frame, _, body = mq_channel.basic_get(queue='heartbeat_rq')

        time.sleep(3)
        self.assertFalse(master_thread.is_alive())
        master_thread.join()

        master_thread = mt.Thread(target=tmgr._heartbeat,
                                name='tmgr_heartbeat')
        master_thread.start()
        body = None
        while not body:
            method_frame, _, body = mq_channel.basic_get(queue='heartbeat_rq')

        nprops = pika.BasicProperties(correlation_id='wrong_id')
        mq_channel.basic_publish(exchange='', routing_key='heartbeat_res',
                                 properties=nprops, body='response')
        mq_channel.basic_ack(delivery_tag=method_frame.delivery_tag)

        time.sleep(3)
        self.assertFalse(master_thread.is_alive())
        master_thread.join()
        mq_channel.queue_delete(queue='heartbeat_rq')
        mq_channel.queue_delete(queue='heartbeat_res')
        mq_channel.close()
        mq_connection.close()
