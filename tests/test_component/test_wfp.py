
import os
import json
import pika

from hypothesis import given, settings, strategies as st
import threading as mt

from radical.entk.appman.wfprocessor import WFprocessor
from radical.entk                    import AppManager as Amgr
from radical.entk                    import Pipeline, Stage, Task, states


hostname =     os.environ.get('RMQ_HOSTNAME', 'localhost')
port     = int(os.environ.get('RMQ_PORT', 5672))
username = os.environ.get('RMQ_USERNAME', 'guest')
password = os.environ.get('RMQ_PASSWORD', 'guest')

# Hypothesis settings
settings.register_profile("travis", max_examples=100, deadline=None)
settings.load_profile("travis")


# ------------------------------------------------------------------------------
#
@given(s=st.characters(),
       b=st.booleans(),
       l=st.lists(st.characters()))
def test_wfp_initialization(s, b, l):

    p  = Pipeline()
    stage = Stage()
    t  = Task()

    t.executable = '/bin/date'
    stage.add_tasks(t)
    p.add_stages(stage)
    credentials = pika.PlainCredentials(username, password)
    rmq_conn_params = pika.ConnectionParameters(host=hostname, port=port,
            credentials=credentials)

    wfp = WFprocessor(sid='rp.session.local.0000',
                      workflow=set([p]),
                      pending_queue=['pending'],
                      completed_queue=['completed'],
                      rmq_conn_params=rmq_conn_params,
                      resubmit_failed=True)

    assert len(wfp._uid.split('.')) == 2
    assert 'wfprocessor'            == wfp._uid.split('.')[0]
    assert wfp._pending_queue       == ['pending']
    assert wfp._completed_queue     == ['completed']
    assert wfp._rmq_conn_params     == rmq_conn_params
    assert wfp._wfp_process         is None
    assert wfp._workflow            == set([p])

    if not isinstance(s, str):
        wfp = WFprocessor(sid=s,
                          workflow=set([p]),
                          pending_queue=l,
                          completed_queue=l,
                          rmq_conn_params=rmq_conn_params,
                          resubmit_failed=b)


# ------------------------------------------------------------------------------
#
def func_for_enqueue_test(p):

    while True:

        if  p.state           == states.SCHEDULING and \
            p.stages[0].state == states.SCHEDULED:

            for t in p.stages[0].tasks:

                if t.state == states.SCHEDULED:
                    return


# ------------------------------------------------------------------------------
#
def test_wfp_enqueue():

    p = Pipeline()
    s = Stage()
    t = Task()

    t.executable = '/bin/date'
    s.add_tasks(t)
    p.add_stages(s)

    amgr = Amgr(hostname=hostname, port=port, username=username,
            password=password)
    amgr._setup_mqs()

    wfp = WFprocessor(sid=amgr._sid,
                      workflow=[p],
                      pending_queue=amgr._pending_queue,
                      completed_queue=amgr._completed_queue,
                      rmq_conn_params=amgr._rmq_conn_params,
                      resubmit_failed=False)

    assert p.uid           is not None
    assert p.stages[0].uid is not None

    for t in p.stages[0].tasks:
        assert t.uid is not None

    assert p.state           == states.INITIAL
    assert p.stages[0].state == states.INITIAL

    for t in p.stages[0].tasks:
        assert t.state == states.INITIAL

    wfp.start_processor()

    th = mt.Thread(target=func_for_enqueue_test, name='temp-proc', args=(p,))
    th.start()
    th.join()

    wfp.terminate_processor()

    assert p.state           == states.SCHEDULING
    assert p.stages[0].state == states.SCHEDULED

    for t in p.stages[0].tasks:
        assert t.state == states.SCHEDULED


# ------------------------------------------------------------------------------
#
def func_for_dequeue_test(p):

    while True:

        if  p.state           == states.DONE and \
            p.stages[0].state == states.DONE:

            for t in p.stages[0].tasks:

                if t.state == states.DONE:
                    return


# ------------------------------------------------------------------------------
#
def test_wfp_dequeue():

    p = Pipeline()
    s = Stage()
    t = Task()

    t.executable = '/bin/date'
    s.add_tasks(t)
    p.add_stages(s)

    amgr = Amgr(hostname=hostname, port=port, username=username,
            password=password)
    amgr._setup_mqs()

    wfp = WFprocessor(sid=amgr._sid,
                      workflow=[p],
                      pending_queue=amgr._pending_queue,
                      completed_queue=amgr._completed_queue,
                      rmq_conn_params=amgr._rmq_conn_params,
                      resubmit_failed=False)

    assert p.state           == states.INITIAL
    assert p.stages[0].state == states.INITIAL

    for t in p.stages[0].tasks:
        assert t.state == states.INITIAL

    for t in p.stages[0].tasks:
        t.state = states.COMPLETED

    task_as_dict  = json.dumps(t.to_dict())
    credentials = pika.PlainCredentials(amgr._username, amgr._password)
    mq_connection = pika.BlockingConnection(pika.ConnectionParameters(
                                          host=amgr._hostname, port=amgr._port,
                                          credentials=credentials))
    mq_channel    = mq_connection.channel()


    mq_channel.basic_publish(exchange    = '',
                             routing_key = '%s' % amgr._completed_queue[0],
                             body        = task_as_dict)

    wfp.start_processor()

    th = mt.Thread(target=func_for_dequeue_test, name='temp-proc', args=(p,))
    th.start()
    th.join()

    wfp.terminate_processor()

    assert p.state           == states.DONE
    assert p.stages[0].state == states.DONE

    for t in p.stages[0].tasks:
        assert t.state == states.DONE


# ------------------------------------------------------------------------------
#
def test_wfp_start_processor():

    p = Pipeline()
    s = Stage()
    t = Task()

    t.executable = '/bin/date'
    s.add_tasks(t)
    p.add_stages(s)

    amgr = Amgr(hostname=hostname, port=port, username=username,
            password=password)
    amgr._setup_mqs()

    wfp = WFprocessor(sid=amgr._sid,
                      workflow=[p],
                      pending_queue=amgr._pending_queue,
                      completed_queue=amgr._completed_queue,
                      rmq_conn_params=amgr._rmq_conn_params,
                      resubmit_failed=False)

    wfp.start_processor()

    assert wfp._enqueue_thread
    assert wfp._dequeue_thread

    assert not wfp._enqueue_thread_terminate.is_set()
    assert not wfp._dequeue_thread_terminate.is_set()

    wfp.terminate_processor()


# ------------------------------------------------------------------------------
#
def test_wfp_terminate_processor():

    p = Pipeline()
    s = Stage()
    t = Task()

    t.executable = '/bin/date'
    s.add_tasks(t)
    p.add_stages(s)

    amgr = Amgr(hostname=hostname, port=port, username=username,
            password=password)
    amgr._setup_mqs()

    wfp = WFprocessor(sid=amgr._sid,
                      workflow=[p],
                      pending_queue=amgr._pending_queue,
                      completed_queue=amgr._completed_queue,
                      rmq_conn_params=amgr._rmq_conn_params,
                      resubmit_failed=False)

    wfp.start_processor()
    wfp.terminate_processor()

    assert not wfp._enqueue_thread
    assert not wfp._dequeue_thread

    assert wfp._enqueue_thread_terminate.is_set()
    assert wfp._dequeue_thread_terminate.is_set()


# ------------------------------------------------------------------------------
#
def test_wfp_workflow_incomplete():

    p = Pipeline()
    s = Stage()
    t = Task()

    t.executable = '/bin/date'
    s.add_tasks(t)
    p.add_stages(s)

    amgr = Amgr(hostname=hostname, port=port, username=username,
            password=password)
    amgr._setup_mqs()

    wfp = WFprocessor(sid=amgr._sid,
                      workflow=[p],
                      pending_queue=amgr._pending_queue,
                      completed_queue=amgr._completed_queue,
                      rmq_conn_params=amgr._rmq_conn_params,
                      resubmit_failed=False)

    for t in p.stages[0].tasks:
        t.state = states.COMPLETED

    task_as_dict  = json.dumps(t.to_dict())
    credentials = pika.PlainCredentials(amgr._username, amgr._password)
    mq_connection = pika.BlockingConnection(pika.ConnectionParameters(
                                          host=amgr._hostname, port=amgr._port))
    mq_channel    = mq_connection.channel()

    mq_channel.basic_publish(exchange    = '',
                             routing_key = '%s' % amgr._completed_queue[0],
                             body        = task_as_dict)

    wfp.start_processor()

    th = mt.Thread(target=func_for_dequeue_test, name='temp-proc', args=(p,))
    th.start()
    th.join()

    wfp.terminate_processor()

    assert not wfp.workflow_incomplete()


# ------------------------------------------------------------------------------
#
def test_wfp_check_processor():

    p = Pipeline()
    s = Stage()
    t = Task()

    t.executable = '/bin/date'
    s.add_tasks(t)
    p.add_stages(s)

    amgr = Amgr(hostname=hostname, port=port, username=username,
            password=password)
    amgr._setup_mqs()

    wfp = WFprocessor(sid=amgr._sid,
                      workflow=[p],
                      pending_queue=amgr._pending_queue,
                      completed_queue=amgr._completed_queue,
                      rmq_conn_params=amgr._rmq_conn_params,
                      resubmit_failed=False)

    wfp.start_processor()
    assert wfp.check_processor()

    wfp.terminate_processor()
    assert not wfp.check_processor()


# ------------------------------------------------------------------------------

