
import pika
import os
import json

import threading as mt

from radical.entk import states
from radical.entk import Task, Stage, Pipeline

from radical.entk.execman.base import Base_TaskManager     as BaseTmgr
from radical.entk.execman.base import Base_ResourceManager as BaseRmgr


# ------------------------------------------------------------------------------
#
def func(obj, obj_type, new_state, queue1):

    hostname = os.environ.get('RMQ_HOSTNAME', 'localhost')
    port = int(os.environ.get('RMQ_PORT', 5672))

    sid  = 'test.0013'
    rmgr = BaseRmgr({}, sid, None, {})
    tmgr = BaseTmgr(sid=sid,
                    pending_queue=['pending-1'],
                    completed_queue=['completed-1'],
                    rmgr=rmgr,
                    mq_hostname=hostname,
                    port=port,
                    rts=None)

    mq_connection = pika.BlockingConnection(pika.ConnectionParameters(
                                                      host=hostname, port=port))
    mq_channel = mq_connection.channel()

    tmgr._advance(obj, obj_type, new_state, mq_channel, queue1)

    mq_connection.close()


# ------------------------------------------------------------------------------
#
def master(obj, obj_type, new_state):

    hostname = os.environ.get('RMQ_HOSTNAME', 'localhost')
    port = int(os.environ.get('RMQ_PORT', 5672))

    mq_connection = pika.BlockingConnection(pika.ConnectionParameters(
                                                      host=hostname, port=port))
    mq_channel = mq_connection.channel()

    queue1 = 'test-1-2-3'       # Expected queue name structure 'X-A-B-C'
    queue2 = 'test-3-2-1'       # Expected queue name structure 'X-C-B-A'
    mq_channel.queue_declare(queue=queue1)
    mq_channel.queue_declare(queue=queue2)

    thread1 = mt.Thread(target=func, args=(obj, obj_type, new_state, queue1))
    thread1.start()

    while True:
        method_frame, props, body = mq_channel.basic_get(queue=queue1)
        if body:

            msg = json.loads(body)
            assert msg['object']['state'] == new_state

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

    master(Task(),     'Task',     states.DONE)
    master(Stage(),    'Stage',    states.DONE)
    master(Pipeline(), 'Pipeline', states.DONE)


# ------------------------------------------------------------------------------

