
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

# Hypothesis settings
settings.register_profile("travis", max_examples=100, deadline=None)
settings.load_profile("travis")


# ------------------------------------------------------------------------------
#
@given(s=st.characters(),
       i=st.integers().filter(lambda x: isinstance(x,int)),
       b=st.booleans(),
       l=st.lists(st.characters()))
def test_wfp_initialization(s, i, b, l):

    p  = Pipeline()
    st = Stage()
    t  = Task()

    t.executable = '/bin/date'
    st.add_tasks(t)
    p.add_stages(st)

    wfp = WFprocessor(sid='rp.session.local.0000',
                      workflow=set([p]),
                      pending_queue=['pending'],
                      completed_queue=['completed'],
                      mq_hostname=hostname,
                      port=port,
                      resubmit_failed=True)

    assert len(wfp._uid.split('.')) == 2
    assert 'wfprocessor'            == wfp._uid.split('.')[0]
    assert wfp._pending_queue       == ['pending']
    assert wfp._completed_queue     == ['completed']
    assert wfp._hostname            == hostname
    assert wfp._port                == port
    assert wfp._wfp_process         is None
    assert wfp._workflow            == set([p])

    if not isinstance(s, unicode):
        wfp = WFprocessor(sid=s,
                          workflow=set([p]),
                          pending_queue=l,
                          completed_queue=l,
                          mq_hostname=s,
                          port=i,
                          resubmit_failed=b)


# ------------------------------------------------------------------------------
#
def test_wfp_initialize_workflow():

    p = Pipeline()
    s = Stage()
    t = Task()

    t.executable = '/bin/date'
    s.add_tasks(t)
    p.add_stages(s)

    wfp = WFprocessor(sid='test',
                      workflow=[p],
                      pending_queue=list(),
                      completed_queue=list(),
                      mq_hostname=hostname,
                      port=port,
                      resubmit_failed=False)

    wfp.initialize_workflow()
    assert p.uid           is not None
    assert p.stages[0].uid is not None

    for t in p.stages[0].tasks:
        assert t.uid is not None


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

    amgr = Amgr(hostname=hostname, port=port)
    amgr._setup_mqs()

    wfp = WFprocessor(sid=amgr._sid,
                      workflow=[p],
                      pending_queue=amgr._pending_queue,
                      completed_queue=amgr._completed_queue,
                      mq_hostname=amgr._hostname,
                      port=amgr._port,
                      resubmit_failed=False)

    wfp.initialize_workflow()

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

    amgr = Amgr(hostname=hostname, port=port)
    amgr._setup_mqs()

    wfp = WFprocessor(sid=amgr._sid,
                      workflow=[p],
                      pending_queue=amgr._pending_queue,
                      completed_queue=amgr._completed_queue,
                      mq_hostname=amgr._hostname,
                      port=amgr._port,
                      resubmit_failed=False)

    wfp.initialize_workflow()

    assert p.state           == states.INITIAL
    assert p.stages[0].state == states.INITIAL

    for t in p.stages[0].tasks:
        assert t.state == states.INITIAL

    p.state           == states.SCHEDULED
    p.stages[0].state == states.SCHEDULING

    for t in p.stages[0].tasks:
        t.state = states.COMPLETED

    task_as_dict  = json.dumps(t.to_dict())
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

    amgr = Amgr(hostname=hostname, port=port)
    amgr._setup_mqs()

    wfp = WFprocessor(sid=amgr._sid,
                      workflow=[p],
                      pending_queue=amgr._pending_queue,
                      completed_queue=amgr._completed_queue,
                      mq_hostname=amgr._hostname,
                      port=amgr._port,
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

    amgr = Amgr(hostname=hostname, port=port)
    amgr._setup_mqs()

    wfp = WFprocessor(sid=amgr._sid,
                      workflow=[p],
                      pending_queue=amgr._pending_queue,
                      completed_queue=amgr._completed_queue,
                      mq_hostname=amgr._hostname,
                      port=amgr._port,
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

    amgr = Amgr(hostname=hostname, port=port)
    amgr._setup_mqs()

    wfp = WFprocessor(sid=amgr._sid,
                      workflow=[p],
                      pending_queue=amgr._pending_queue,
                      completed_queue=amgr._completed_queue,
                      mq_hostname=amgr._hostname,
                      port=amgr._port,
                      resubmit_failed=False)

    wfp.initialize_workflow()

    p.state           == states.SCHEDULED
    p.stages[0].state == states.SCHEDULING

    for t in p.stages[0].tasks:
        t.state = states.COMPLETED

    task_as_dict  = json.dumps(t.to_dict())
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

    amgr = Amgr(hostname=hostname, port=port)
    amgr._setup_mqs()

    wfp = WFprocessor(sid=amgr._sid,
                      workflow=[p],
                      pending_queue=amgr._pending_queue,
                      completed_queue=amgr._completed_queue,
                      mq_hostname=amgr._hostname,
                      port=amgr._port,
                      resubmit_failed=False)

    wfp.start_processor()
    assert wfp.check_processor()

    wfp.terminate_processor()
    assert not wfp.check_processor()


# ------------------------------------------------------------------------------

