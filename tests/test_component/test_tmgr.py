from radical.entk.execman.base import Base_TaskManager as BaseTmgr
from radical.entk.execman.base import Base_ResourceManager as BaseRmgr
from radical.entk.execman.rp import TaskManager as RPTmgr
from radical.entk.execman.rp import ResourceManager as RPRmgr
from radical.entk.execman.mock import TaskManager as MockTmgr
from radical.entk.execman.mock import ResourceManager as MockRmgr
from radical.entk import Task, states
from radical.entk.exceptions import *
import pytest
from hypothesis import given, settings
import hypothesis.strategies as st
import os
from radical.entk.exceptions import *
from time import sleep
import threading
import pika
from multiprocessing import Process, Event
import json

hostname = os.environ.get('RMQ_HOSTNAME', 'localhost')
port = int(os.environ.get('RMQ_PORT', 5672))
# MLAB = 'mongodb://entk:entk123@ds143511.mlab.com:43511/entk_0_7_4_release'
MLAB = os.environ.get('RADICAL_PILOT_DBURL')

# Hypothesis settings
settings.register_profile("travis", max_examples=100, deadline=None)
settings.load_profile("travis")

@given(s=st.text(),
       l=st.lists(st.characters()),
       i=st.integers())
def test_tmgr_base_initialization(s, l, i):

    try:
        import glob
        import shutil
        import os
        home = os.environ.get('HOME', '/home')
        test_fold = glob.glob('%s/.radical/utils/test.*' % home)
        for f in test_fold:
            shutil.rmtree(f)
    except:
        pass

    sid = 'test.0000'
    rmgr = BaseRmgr({}, sid, None, {})

    tmgr = BaseTmgr(sid=sid,
                    pending_queue=['pending-1'],
                    completed_queue=['completed-1'],
                    rmgr=rmgr,
                    mq_hostname=hostname,
                    port=port,
                    rts=None)

    assert tmgr._uid == 'task_manager.0000'
    assert tmgr._pending_queue == ['pending-1']
    assert tmgr._completed_queue == ['completed-1']
    assert tmgr._mq_hostname == hostname
    assert tmgr._port == port
    assert tmgr._rts == None

    assert tmgr._logger
    assert tmgr._prof
    assert tmgr._hb_request_q == '%s-hb-request' % sid
    assert tmgr._hb_response_q == '%s-hb-response' % sid
    assert not tmgr._tmgr_process
    assert not tmgr._hb_thread


@given(s=st.characters(),
       l=st.lists(st.characters()),
       i=st.integers().filter(lambda x: isinstance(x,int)),
       b=st.booleans(),
       se=st.sets(st.text()),
       di=st.dictionaries(st.text(), st.text()))
def test_tmgr_base_assignment_exceptions(s, l, i, b, se, di):

    sid = 'test.0000'
    rmgr = BaseRmgr({}, sid, None, {})

    data_type = [s, l, i, b, se, di]

    for d in data_type:

        if not isinstance(d, str):

            with pytest.raises(TypeError):

                tmgr = BaseTmgr(sid=s,
                                pending_queue=s,
                                completed_queue=s,
                                rmgr=rmgr,
                                mq_hostname=s,
                                port=d,
                                rts=None)

        if not isinstance(d, int):

            with pytest.raises(TypeError):

                tmgr = BaseTmgr(sid=s,
                                pending_queue=s,
                                completed_queue=s,
                                rmgr=rmgr,
                                mq_hostname=s,
                                port=d,
                                rts=None)


def func_for_heartbeat_test(mq_hostname, port, hb_request_q, hb_response_q):

    mq_connection = pika.BlockingConnection(pika.ConnectionParameters(host=mq_hostname, port=port))
    mq_channel = mq_connection.channel()

    while True:

        method_frame, props, body = mq_channel.basic_get(queue=hb_request_q)
        if body:
            mq_channel.basic_publish(exchange='',
                                     routing_key=hb_response_q,
                                     properties=pika.BasicProperties(
                                         correlation_id=props.correlation_id),
                                     body='response')
            mq_channel.basic_ack(delivery_tag=method_frame.delivery_tag)
            break

    mq_connection.close()


def test_tmgr_base_heartbeat():
    sid = 'test.0000'
    rmgr = BaseRmgr({}, sid, None, {})

    os.environ['ENTK_HB_INTERVAL'] = '30'

    tmgr = BaseTmgr(sid=sid,
                    pending_queue=['pending-1'],
                    completed_queue=['completed-1'],
                    rmgr=rmgr,
                    mq_hostname=hostname,
                    port=port,
                    rts=None)

    tmgr._hb_terminate = threading.Event()
    tmgr._hb_thread = threading.Thread(target=tmgr._heartbeat, name='heartbeat')
    tmgr._hb_thread.start()

    proc = Process(target=func_for_heartbeat_test, args=(hostname,
                                                         port,
                                                         tmgr._hb_request_q,
                                                         tmgr._hb_response_q))
    proc.start()

    proc.join()
    tmgr._hb_thread.join()


def test_tmgr_base_start_heartbeat():

    sid = 'test.0000'
    rmgr = BaseRmgr({}, sid, None, {})

    os.environ['ENTK_HB_INTERVAL'] = '30'

    tmgr = BaseTmgr(sid=sid,
                    pending_queue=['pending-1'],
                    completed_queue=['completed-1'],
                    rmgr=rmgr,
                    mq_hostname=hostname,
                    port=port,
                    rts=None)

    assert tmgr.start_heartbeat()
    assert not tmgr._hb_terminate.is_set()
    assert tmgr._hb_thread.is_alive()
    tmgr._hb_terminate.set()
    sleep(15)
    tmgr._hb_thread.join()
    assert not tmgr._hb_thread.is_alive()


def test_tmgr_base_terminate_heartbeat():

    sid = 'test.0000'
    rmgr = BaseRmgr({}, sid, None, {})
    os.environ['ENTK_HB_INTERVAL'] = '30'

    tmgr = BaseTmgr(sid=sid,
                    pending_queue=['pending-1'],
                    completed_queue=['completed-1'],
                    rmgr=rmgr,
                    mq_hostname=hostname,
                    port=port,
                    rts=None)

    tmgr._hb_terminate = threading.Event()

    assert tmgr.start_heartbeat()
    assert not tmgr._hb_terminate.is_set()
    assert tmgr._hb_thread.is_alive()
    tmgr.terminate_heartbeat()
    sleep(30)
    assert not tmgr._hb_thread
    assert tmgr._hb_terminate.is_set()


def test_tmgr_base_terminate_manager():

    sid = 'test.0000'
    rmgr = BaseRmgr({}, sid, None, {})

    tmgr = BaseTmgr(sid=sid,
                    pending_queue=['pending-1'],
                    completed_queue=['completed-1'],
                    rmgr=rmgr,
                    mq_hostname=hostname,
                    port=port,
                    rts=None)

    tmgr._tmgr_process = Process(target=tmgr._tmgr, name='heartbeat')
    tmgr._tmgr_terminate = Event()
    tmgr.terminate_manager()

    assert not tmgr._tmgr_process
    assert tmgr._tmgr_terminate.is_set()


def test_tmgr_base_check_heartbeat():

    sid = 'test.0000'
    rmgr = BaseRmgr({}, sid, None, {})

    os.environ['ENTK_HB_INTERVAL'] = '30'

    tmgr = BaseTmgr(sid=sid,
                    pending_queue=['pending-1'],
                    completed_queue=['completed-1'],
                    rmgr=rmgr,
                    mq_hostname=hostname,
                    port=port,
                    rts=None)

    tmgr._hb_thread = threading.Thread(target=tmgr._heartbeat, name='heartbeat')
    tmgr._hb_terminate = threading.Event()
    tmgr._hb_thread.start()
    assert tmgr.check_heartbeat()
    tmgr.terminate_heartbeat()


def test_tmgr_base_check_manager():

    sid = 'test.0000'
    rmgr = BaseRmgr({}, sid, None, {})

    tmgr = BaseTmgr(sid=sid,
                    pending_queue=['pending-1'],
                    completed_queue=['completed-1'],
                    rmgr=rmgr,
                    mq_hostname=hostname,
                    port=port,
                    rts=None)

    tmgr._tmgr_process = Process(target=tmgr._tmgr, name='heartbeat')
    tmgr._tmgr_terminate = Event()
    tmgr._tmgr_process.start()
    assert tmgr.check_manager()
    tmgr.terminate_manager()


def test_tmgr_base_methods():

    sid = 'test.0000'
    rmgr = BaseRmgr({}, sid, None, {})

    tmgr = BaseTmgr(sid=sid,
                    pending_queue=['pending-1'],
                    completed_queue=['completed-1'],
                    rmgr=rmgr,
                    mq_hostname=hostname,
                    port=port,
                    rts=None)

    with pytest.raises(NotImplementedError):
        tmgr._tmgr(uid=None,
                   rmgr=None,
                   logger=None,
                   mq_hostname=None,
                   port=None,
                   pending_queue=None,
                   completed_queue=None)

    with pytest.raises(NotImplementedError):
        tmgr.start_manager()


@given(s=st.text(),
       l=st.lists(st.characters()),
       i=st.integers())
def test_tmgr_mock_initialization(s, l, i):

    try:
        import glob
        import shutil
        import os
        home = os.environ.get('HOME', '/home')
        test_fold = glob.glob('%s/.radical/utils/test.*' % home)
        for f in test_fold:
            shutil.rmtree(f)
    except:
        pass

    sid = 'test.0000'
    rmgr = MockRmgr(resource_desc={}, sid=sid)

    tmgr = MockTmgr(sid=sid,
                     pending_queue=['pending'],
                     completed_queue=['completed'],
                     rmgr=rmgr,
                     mq_hostname=hostname,
                     port=port)

    assert tmgr._uid == 'task_manager.0000'
    assert tmgr._pending_queue == ['pending']
    assert tmgr._completed_queue == ['completed']
    assert tmgr._mq_hostname == hostname
    assert tmgr._port == port
    assert tmgr._rts == 'mock'

    assert tmgr._logger
    assert tmgr._prof
    assert tmgr._hb_request_q == '%s-hb-request' % sid
    assert tmgr._hb_response_q == '%s-hb-response' % sid
    assert tmgr._tmgr_process == None
    assert tmgr._hb_thread == None
    assert tmgr._rmq_ping_interval == 10


def func_for_mock_tmgr_test(mq_hostname, port, pending_queue, completed_queue):

    mq_connection = pika.BlockingConnection(pika.ConnectionParameters(host=mq_hostname, port=port))
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


def test_tmgr_mock_tmgr():

    res_dict = {
        'resource': 'local.localhost',
                    'walltime': 40,
                    'cpus': 20,
                    'project': 'Random'
    }

    os.environ['RADICAL_PILOT_DBURL'] = 'mlab-url'
    os.environ['ENTK_HB_INTERVAL'] = '30'

    rmgr = MockRmgr(resource_desc=res_dict, sid='test.0000')

    tmgr = MockTmgr(sid='test.0000',
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


@given(s=st.text(),
       l=st.lists(st.characters()),
       i=st.integers())
def test_tmgr_rp_initialization(s, l, i):

    sid = 'test.0000'
    os.environ['RADICAL_PILOT_DBURL'] = MLAB

    config={ "sandbox_cleanup": False,"db_cleanup": False}
    rmgr = RPRmgr({}, sid, config)

    tmgr = RPTmgr(sid=sid,
                  pending_queue=['pending'],
                  completed_queue=['completed'],
                  rmgr=rmgr,
                  mq_hostname=hostname,
                  port=port)

    assert 'task_manager' in tmgr._uid
    assert tmgr._pending_queue == ['pending']
    assert tmgr._completed_queue == ['completed']
    assert tmgr._mq_hostname == hostname
    assert tmgr._port == port
    assert tmgr._rts == 'radical.pilot'
    assert tmgr._umgr == None

    assert tmgr._tmgr_process == None
    assert tmgr._hb_thread == None


# def test_tmgr_rp_tmgr():

#     os.environ['RADICAL_PILOT_DBURL'] = MLAB
#     os.environ['ENTK_HB_INTERVAL'] = '30'

#     res_dict = {
#         'resource': 'local.localhost',
#                     'walltime': 40,
#                     'cpus': 20,
#     }
#     config={ "sandbox_cleanup": False,"db_cleanup": False}
#     rmgr = RPRmgr(resource_desc=res_dict, sid='test.0000', config=config)
#     rmgr._validate_resource_desc()
#     rmgr._populate()
#     rmgr._submit_resource_request()

#     tmgr = RPTmgr(sid='test.0000',
#                      pending_queue=['pendingq-1'],
#                      completed_queue=['completedq-1'],
#                      rmgr=rmgr,
#                      mq_hostname=hostname,
#                      port=port)

#     tmgr.start_manager()

#     proc = Process(target=func_for_mock_tmgr_test, args=(hostname,
#                                                           port,
#                                                           tmgr._pending_queue[0],
#                                                           tmgr._completed_queue[0]))
#     proc.start()

#     proc.join()
#     tmgr.terminate_manager()
#     rmgr._terminate_resource_request()
