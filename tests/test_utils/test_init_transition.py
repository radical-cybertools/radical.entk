from radical.entk.utils.init_transition import transition
import pika
from radical.entk import Task, Stage, Pipeline
import radical.utils as ru
import os
from threading import Thread
import json
from radical.entk import states

MLAB = 'mongodb://entk:entk123@ds143511.mlab.com:43511/entk_0_7_4_release'

def func(obj, obj_type, new_state, queue1, logger, profiler):

    hostname = os.environ.get('RMQ_HOSTNAME', 'localhost')
    port = int(os.environ.get('RMQ_PORT', 5672))

    mq_connection = pika.BlockingConnection(pika.ConnectionParameters(host=hostname, port=port))
    mq_channel = mq_connection.channel()

    transition( obj, 
                obj_type, 
                new_state, 
                mq_channel, 
                queue1, 
                profiler, 
                logger)

    mq_connection.close()


def master(obj, obj_type, new_state):

    hostname = os.environ.get('RMQ_HOSTNAME', 'localhost')
    port = int(os.environ.get('RMQ_PORT', 5672))

    mq_connection = pika.BlockingConnection(pika.ConnectionParameters(host=hostname, port=port))
    mq_channel = mq_connection.channel()

    queue1 = 'test-1-2-3'       # Expected queue name structure 'X-A-B-C'
    queue2 = 'test-3-2-1'       # Expected queue name structure 'X-C-B-A'
    mq_channel.queue_declare(queue=queue1)    
    mq_channel.queue_declare(queue=queue2)

    logger = ru.Logger('radical.entk.test')
    profiler = ru.Profiler('radical.entk.test')

    thread1 = Thread(target=func, args=(obj, obj_type, new_state, queue1, logger, profiler))
    thread1.start()

    while True:
        method_frame, props, body = mq_channel.basic_get(queue=queue1)
        if body:

            msg = json.loads(body)
            assert msg['object']['state'] == new_state
            mq_channel.basic_publish(exchange='',
                                     routing_key=queue2,
                                     properties=pika.BasicProperties(correlation_id=props.correlation_id),
                                     body='ack')
            mq_channel.basic_ack(delivery_tag=method_frame.delivery_tag)
            break

    mq_channel.queue_delete(queue=queue1)
    mq_channel.queue_delete(queue=queue2)
    mq_connection.close()
    thread1.join()

def test_utils_sync_with_master():

    obj = Task()
    obj_type = 'Task'    
    master(obj, obj_type, states.DONE)

    obj = Stage()
    obj_type = 'Stage'    
    master(obj, obj_type, states.DONE)

    obj = Pipeline()
    obj_type = 'Pipeline'    
    master(obj, obj_type, states.DONE)
    