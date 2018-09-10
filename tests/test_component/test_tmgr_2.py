from radical.entk.execman.base import Base_TaskManager as BaseTmgr
from radical.entk.execman.base import Base_ResourceManager as BaseRmgr
from radical.entk.execman.rp import TaskManager as RPTmgr
from radical.entk.execman.rp import ResourceManager as RPRmgr
from radical.entk.execman.mock import TaskManager as MockTmgr
from radical.entk.execman.mock import ResourceManager as MockRmgr
from radical.entk import Task, states
from radical.entk.exceptions import *
import pytest
from hypothesis import given
import hypothesis.strategies as st
import os
from radical.entk.exceptions import *
from time import sleep
import threading
import pika
from multiprocessing import Process, Event
import json
import radical.utils as ru

hostname = os.environ.get('RMQ_HOSTNAME', 'localhost')
port = int(os.environ.get('RMQ_PORT', 5672))
# MLAB = 'mongodb://entk:entk123@ds143511.mlab.com:43511/entk_0_7_4_release'
MLAB = os.environ.get('RADICAL_PILOT_DBURL')


def func_for_mock_tmgr_test(mq_hostname, port, pending_queue, completed_queue):

    mq_connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=mq_hostname, port=port))
    mq_channel = mq_connection.channel()

    tasks = list()
    for _ in range(16):
        t = Task()
        t.state = states.SCHEDULING
        t.executable = '/bin/echo'
        tasks.append(t.to_dict())

    tasks_as_json = json.dumps(tasks)
    mq_channel.basic_publish(exchange='',
                             routing_key=pending_queue,
                             body=tasks_as_json)

    cnt = 0
    while cnt < 15:

        method_frame, props, body = mq_channel.basic_get(queue=completed_queue)
        if body:
            task = Task()
            task.from_dict(json.loads(body))
            if task.state == states.DONE:
                cnt += 1
            mq_channel.basic_ack(delivery_tag=method_frame.delivery_tag)

    mq_connection.close()

def test_tmgr_rp_tmgr():

    os.environ['RADICAL_PILOT_DBURL'] = MLAB
    os.environ['ENTK_HB_INTERVAL'] = '30'

    res_dict = {
        'resource': 'local.localhost',
                    'walltime': 40,
                    'cpus': 20,
    }
    config={ "sandbox_cleanup": False,"db_cleanup": False}
    rmgr_id = ru.generate_id('test.%(item_counter)04d', ru.ID_CUSTOM)
    rmgr = RPRmgr(resource_desc=res_dict, sid=rmgr_id, rts_config=config)
    rmgr._validate_resource_desc()
    rmgr._populate()
    rmgr._submit_resource_request()

    tmgr = RPTmgr(sid=rmgr_id,
                     pending_queue=['pendingq-1'],
                     completed_queue=['completedq-1'],
                     rmgr=rmgr,
                     mq_hostname=hostname,
                     port=port)

    tmgr.start_manager()

    proc = Process(target=func_for_mock_tmgr_test, args=(hostname,
                                                          port,
                                                          tmgr._pending_queue[0],
                                                          tmgr._completed_queue[0]))
    proc.start()

    proc.join()
    tmgr.terminate_manager()
    rmgr._terminate_resource_request()
