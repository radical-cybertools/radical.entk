# pylint: disable=protected-access, unused-argument
# pylint: disable=no-value-for-parameter
import os
import pika
import json
import time

from unittest import TestCase, mock
import threading as mt

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
                if channel.is_open:
                    channel.close()
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
                                  mq_channel, 'master'))
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



