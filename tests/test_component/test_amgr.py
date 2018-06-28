from radical.entk import AppManager as Amgr
from hypothesis import given
import hypothesis.strategies as st
from radical.entk import Pipeline, Stage, Task, states
from radical.entk.exceptions import *
from radical.entk.utils.sync_initiator import sync_with_master
import radical.utils as ru
import pytest
import pika
from threading import Event, Thread
from multiprocessing import Process
import os

hostname = os.environ.get('RMQ_HOSTNAME', 'localhost')
port = os.environ.get('RMQ_PORT', 5672)


def test_amgr_initialization():

    amgr = Amgr()

    assert len(amgr._uid.split('.')) == ['appmanager', '0000']
    assert type(amgr._logger) == type(ru.get_logger('radical.tests'))
    assert type(amgr._prof) == type(ru.Profiler('radical.tests'))
    assert type(amgr._report) == type(ru.Reporter('radical.tests'))
    assert isinstance(amgr.name, str)

    # RabbitMQ inits
    assert amgr._mq_hostname == 'localhost'
    assert amgr._port == 5672

    # RabbitMQ Queues
    assert amgr._num_pending_qs == 1
    assert amgr._num_completed_qs == 1
    assert isinstance(amgr._pending_queue, list)
    assert isinstance(amgr._completed_queue, list)

    # Global parameters to have default values
    assert amgr._mqs_setup == False
    assert amgr._resource_desc == None
    assert amgr._task_manager == None
    assert amgr._workflow == None
    assert amgr._resubmit_failed == False
    assert amgr._reattempts == 3
    assert amgr._cur_attempt == 1
    assert amgr._autoterminate == True


def test_amgr_read_config():
    pass


def test_amgr_resource_description_assignment():
    pass


def test_amgr_assign_workflow():
    pass


def test_amgr_run():
    pass


def test_amgr_resource_terminate():
    pass


def test_amgr_validate_workflow():
    pass


def test_amgr_setup_mqs():
    pass


def test_amgr_cleanup_mqs():

    amgr = AppManager(hostname=hostname, port=port)
    sid = amgr._sid

    amgr._setup_mqs()
    amgr._cleanup_mqs()

    mq_connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=hostname, port=port))

    qs = ['%s-tmgr-to-sync' % sid,
          '%s-cb-to-sync' % sid,
          '%s-enq-to-sync' % sid,
          '%s-deq-to-sync' % sid,
          '%s-sync-to-tmgr' % sid,
          '%s-sync-to-cb' % sid,
          '%s-sync-to-enq' % sid,
          '%s-sync-to-deq' % sid,
          '%s-pendingq-1' % sid,
          '%s-completedq-1' % sid]

    for q in qs:
        with pytest.raises(pika.exceptions.ChannelClosed):
            mq_channel = mq_connection.channel()
            mq_channel.queue_purge(q)


def func_for_synchronizer_test(sid, p, mq_channel, logger, profiler):

    mq_connection = pika.BlockingConnection(
        pika.ConnectionParameters(
            host=hostname,
            port=port)
    )
    mq_channel = mq_connection.channel()

    for t in p.stages[0].tasks:

        t.state = states.SCHEDULING
        sync_with_master(obj=t,
                         obj_type='Task',
                         channel=mq_channel,
                         queue='%s-tmgr-to-sync' % sid,
                         logger=logger,
                         local_prof=profiler)


def test_amgr_synchronizer():

    logger = ru.get_logger('radical.entk.temp_logger')
    profiler = ru.Profiler(name='radical.entk.temp')
    amgr = AppManager(hostname=hostname, port=port)

    mq_connection = pika.BlockingConnection(
        pika.ConnectionParameters(
            host=hostname,
            port=port)
    )

    mq_channel = mq_connection.channel()

    amgr._setup_mqs()

    p = Pipeline()
    s = Stage()

    # Create and add 100 tasks to the stage
    for cnt in range(100):

        t = Task()
        t.executable = ['some-executable-%s' % cnt]

        s.add_tasks(t)

    p.add_stages(s)
    p._initialize(amgr._sid)

    amgr.assign_workflow(set([p]))

    for t in p.stages[0].tasks:
        assert t.state == states.INITIAL

    # Start the synchronizer method in a thread
    amgr._terminate_sync = Event()
    sync_thread = Thread(target=amgr._synchronizer, name='synchronizer-thread')
    sync_thread.start()

    # Start the synchronizer method in a thread
    proc = Process(target=func_for_synchronizer_test, name='temp-proc',
                   args=(amgr._sid, p, mq_channel, logger, profiler))

    proc.start()
    proc.join()

    for t in p.stages[0].tasks:
        assert t.state == states.SCHEDULING

    amgr._terminate_sync.set()
    sync_thread.join()


def test_sid_in_mqs():

    appman = AppManager(hostname=hostname, port=port)
    appman._setup_mqs()
    sid = appman._sid

    qs = [
        '%s-tmgr-to-sync' % sid,
        '%s-cb-to-sync' % sid,
        '%s-enq-to-sync' % sid,
        '%s-deq-to-sync' % sid,
        '%s-sync-to-tmgr' % sid,
        '%s-sync-to-cb' % sid,
        '%s-sync-to-enq' % sid,
        '%s-sync-to-deq' % sid
    ]

    mq_connection = pika.BlockingConnection(
        pika.ConnectionParameters(
            host=hostname,
            port=port)
    )
    mq_channel = mq_connection.channel()

    def callback():
        print True

    for q in qs:

        try:
            mq_channel.basic_consume(callback, queue=q, no_ack=True)
        except Exception as ex:
            raise Error(ex)

