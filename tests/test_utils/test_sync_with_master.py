
import os
import pika

import threading as mt

from radical.entk.execman.base import Base_TaskManager     as BaseTmgr
from radical.entk.execman.base import Base_ResourceManager as BaseRmgr
from radical.entk              import Task, Stage, Pipeline


# ------------------------------------------------------------------------------
#
def syncer(obj, obj_type, queue1):

    hostname = os.environ.get('RMQ_HOSTNAME', 'localhost')
    port = int(os.environ.get('RMQ_PORT', 5672))
    username = os.environ.get('RMQ_USERNAME', 'guest')
    password = os.environ.get('RMQ_PASSWORD', 'guest')

    credentials = pika.PlainCredentials(username, password)
    rmq_conn_params = pika.ConnectionParameters(host=hostname, port=port,
            credentials=credentials)
    mq_connection = pika.BlockingConnection(rmq_conn_params)
    mq_channel = mq_connection.channel()

    sid = 'test.0015'
    rmgr = BaseRmgr({}, sid, None, {})
    tmgr = BaseTmgr(sid=sid,
                    pending_queue=['pending-1'],
                    completed_queue=['completed-1'],
                    rmgr=rmgr,
                    rmq_conn_params=rmq_conn_params,
                    rts=None)

    tmgr._sync_with_master(obj, obj_type, mq_channel, queue1)

    mq_connection.close()


# ------------------------------------------------------------------------------
#
def master(obj, obj_type):

    hostname =     os.environ.get('RMQ_HOSTNAME', 'localhost')
    port     = int(os.environ.get('RMQ_PORT', 5672))
    username = os.environ.get('RMQ_USERNAME', 'guest')
    password = os.environ.get('RMQ_PASSWORD', 'guest')

    credentials = pika.PlainCredentials(username, password)
    mq_connection = pika.BlockingConnection(pika.ConnectionParameters(
                                                      host=hostname, port=port, 
                                                      credentials=credentials))
    mq_channel = mq_connection.channel()

    queue1 = 'test-1-2-3'       # Expected queue name structure 'X-A-B-C'
    queue2 = 'test-3-2-1'       # Expected queue name structure 'X-C-B-A'

    mq_channel.queue_declare(queue=queue1)
    mq_channel.queue_declare(queue=queue2)

    thread1 = mt.Thread(target=syncer, args=(obj, obj_type, queue1))
    thread1.start()

    while True:

        method_frame, props, body = mq_channel.basic_get(queue=queue1)

        if not body:
            continue

        nprops = pika.BasicProperties(correlation_id=props.correlation_id)
        mq_channel.basic_publish(exchange='',
                                 routing_key=queue2,
                                 properties=nprops,
                                 body='ack')
        mq_channel.basic_ack(delivery_tag=method_frame.delivery_tag)
        break

    mq_channel.queue_delete(queue=queue1)
    mq_channel.queue_delete(queue=queue2)
    mq_connection.close()
    thread1.join()


# ------------------------------------------------------------------------------
#
def test_utils_sync_with_master():

    master(Task(),     'Task')
    master(Stage(),    'Stage')
    master(Pipeline(), 'Pipeline')


# ------------------------------------------------------------------------------

