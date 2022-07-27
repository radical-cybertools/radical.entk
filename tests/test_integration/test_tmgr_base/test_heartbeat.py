#!/usr/bin/env python3

# pylint: disable=protected-access, unused-argument, no-value-for-parameter

import os
import pika
import time

import threading     as mt
import radical.utils as ru

from unittest import TestCase, mock

from radical.entk.execman.base import Base_TaskManager as BaseTmgr, \
                                      heartbeat_response


# ------------------------------------------------------------------------------
#
class HeartbeatTC(TestCase):

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(BaseTmgr, '__init__', return_value=None)
    @mock.patch('radical.utils.Profiler')
    def test_heartbeat(self, mocked_profiler, mocked_init):

        hostname        = os.environ.get('RMQ_HOSTNAME', 'localhost')
        port            = os.environ.get('RMQ_PORT',     '5672')
        username        = os.environ.get('RMQ_USERNAME', 'guest')
        password        = os.environ.get('RMQ_PASSWORD', 'guest')

        rmq_conn_params = pika.ConnectionParameters(
            host=hostname,
            port=int(port),
            credentials=pika.PlainCredentials(username, password))

        tmgr       = BaseTmgr(None, None, None, None, None, None)
        tmgr._uid  = 'tmgr.0000'
        tmgr._log  = ru.Logger('radical.entk.manager.base', level='DEBUG')
        tmgr._prof = mocked_profiler

        tmgr._hb_interval     = 0.1
        tmgr._hb_terminate    = mt.Event()
        tmgr._hb_request_q    = 'tmgr-hb-request'
        tmgr._hb_response_q   = 'tmgr-hb-response'
        tmgr._rmq_conn_params = rmq_conn_params

        mq_connection = pika.BlockingConnection(rmq_conn_params)
        mq_channel    = mq_connection.channel()

        tmgr._log.info('Starting test')

        master_thread = mt.Thread(target=tmgr._heartbeat,
                                  name='tmgr_heartbeat')

        mq_channel.queue_declare(queue=tmgr._hb_request_q)
        mq_channel.queue_declare(queue=tmgr._hb_response_q)
        try:

            master_thread.start()

            body = None
            for _ in range(5):
                while body is None:
                    _, header_frame, body = mq_channel.basic_get(
                        queue=tmgr._hb_request_q)
                self.assertEqual(body, b'request')
                mq_channel.basic_publish(
                    exchange='',
                    routing_key=tmgr._hb_response_q,
                    properties=pika.BasicProperties(
                        correlation_id=header_frame.correlation_id),
                    body='response')
                self.assertTrue(master_thread.is_alive())
                body = None

            time.sleep(0.2)  # short time for `_heartbeat` to publish "request"
            heartbeat_response(mq_channel,
                               tmgr._hb_request_q,
                               tmgr._hb_response_q,
                               conn_params=rmq_conn_params,
                               log=tmgr._log)

            time.sleep(0.5)  # to skip "response" and to stop `_heartbeat`
            self.assertFalse(master_thread.is_alive())

        finally:
            master_thread.join()
            mq_channel.queue_delete(queue=tmgr._hb_request_q)
            mq_channel.queue_delete(queue=tmgr._hb_response_q)

        master_thread = mt.Thread(target=tmgr._heartbeat,
                                  name='tmgr_heartbeat')

        mq_channel.queue_declare(queue=tmgr._hb_request_q)
        mq_channel.queue_declare(queue=tmgr._hb_response_q)
        try:

            master_thread.start()

            body = None
            while body is None:
                _, _, body = mq_channel.basic_get(queue=tmgr._hb_request_q)
            mq_channel.basic_publish(exchange='',
                                     routing_key=tmgr._hb_response_q,
                                     body='response')

            time.sleep(0.2)  # wrong correlation -> stop `_heartbeat`
            self.assertFalse(master_thread.is_alive())

        finally:
            master_thread.join()
            mq_channel.queue_delete(queue=tmgr._hb_request_q)
            mq_channel.queue_delete(queue=tmgr._hb_response_q)

        tmgr._hb_terminate.set()
        mq_channel.close()
        mq_connection.close()


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    tc = HeartbeatTC()
    tc.test_heartbeat()


# ------------------------------------------------------------------------------

