# pylint: disable=protected-access, unused-argument
# pylint: disable=no-value-for-parameter
import os
import pika
import time

from unittest import TestCase, mock

import threading as mt
import radical.utils as ru

from radical.entk.execman.base import Base_TaskManager as BaseTmgr


# ------------------------------------------------------------------------------
#
class TestTask(TestCase):

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
        tmgr = BaseTmgr(None, None, None, None, None, None)
        tmgr._uid = 'tmgr.0000'
        tmgr._log = ru.Logger('radical.entk.manager.base', level='DEBUG')
        tmgr._prof = mocked_Profiler
        tmgr._hb_interval = 0.1
        tmgr._hb_terminate = mt.Event()
        tmgr._hb_request_q = 'tmgr-hb-request'
        tmgr._hb_response_q = 'tmgr-hb-response'
        tmgr._rmq_conn_params = rmq_conn_params
        mq_connection = pika.BlockingConnection(rmq_conn_params)
        mq_channel = mq_connection.channel()
        mq_channel.queue_declare(queue='tmgr-hb-request')
        mq_channel.queue_declare(queue='tmgr-hb-response')
        tmgr._log.info('Starting test')
        master_thread = mt.Thread(target=tmgr._heartbeat,
                                  name='tmgr_heartbeat')

        master_thread.start()
        time.sleep(0.1)
        body = None
        try:
            for _ in range(5):
                while body is None:
                    _, props, body = mq_channel.basic_get(queue='tmgr-hb-request')
                self.assertEqual(body, b'request')
                nprops = pika.BasicProperties(correlation_id=props.correlation_id)
                mq_channel.basic_publish(exchange='',
                                         routing_key='tmgr-hb-response',
                                         properties=nprops,
                                         body='response')
                self.assertTrue(master_thread.is_alive())
                body = None

            time.sleep(0.5)
            self.assertFalse(master_thread.is_alive())
            master_thread.join()
            mq_channel.queue_delete(queue='tmgr-hb-request')
            mq_channel.queue_delete(queue='tmgr-hb-response')
            mq_channel.queue_declare(queue='tmgr-hb-request')
            mq_channel.queue_declare(queue='tmgr-hb-response')

            master_thread = mt.Thread(target=tmgr._heartbeat,
                                      name='tmgr_heartbeat')
            master_thread.start()
            body = None
            while body is None:
                _, props, body = mq_channel.basic_get(queue='tmgr-hb-request')
            mq_channel.basic_publish(exchange='',
                                     routing_key='tmgr-hb-response',
                                     body='response')
            time.sleep(0.2)
            self.assertFalse(master_thread.is_alive())

        except Exception as ex:
            tmgr._hb_terminate.set()
            master_thread.join()
            mq_channel.queue_delete(queue='tmgr-hb-request')
            mq_channel.queue_delete(queue='tmgr-hb-response')
            mq_channel.close()
            mq_connection.close()
            raise ex
        else:
            tmgr._hb_terminate.set()
            master_thread.join()
            mq_channel.queue_delete(queue='tmgr-hb-request')
            mq_channel.queue_delete(queue='tmgr-hb-response')
            mq_channel.close()
            mq_connection.close()
