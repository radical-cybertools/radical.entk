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
port = int(os.environ.get('RMQ_PORT', 5672))
MLAB = 'mongodb://entk:entk123@ds143511.mlab.com:43511/entk_0_7_4_release'

def test_amgr_initialization():

    amgr = Amgr(hostname=hostname, port=port)

    assert amgr._uid.split('.') == ['appmanager', '0000']
    assert type(amgr._logger) == type(ru.get_logger('radical.tests'))
    assert type(amgr._prof) == type(ru.Profiler('radical.tests'))
    assert type(amgr._report) == type(ru.Reporter('radical.tests'))
    assert isinstance(amgr.name, str)

    # RabbitMQ inits
    assert amgr._mq_hostname == hostname
    assert amgr._port == port

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
    assert isinstance(amgr.shared_data, list)


def test_amgr_read_config():

    amgr = Amgr()

    assert amgr._mq_hostname == 'localhost'
    assert amgr._port == 5672
    assert amgr._reattempts == 3
    assert amgr._resubmit_failed == False
    assert amgr._autoterminate == True
    assert amgr._write_workflow == False
    assert amgr._rts == 'radical.pilot'
    assert amgr._num_pending_qs == 1
    assert amgr._num_completed_qs == 1
    assert amgr._rmq_cleanup == True
    assert amgr._rts_config == { "sandbox_cleanup": False, "db_cleanup": False}

    d = {"hostname": "radical.two",
         "port": 25672,
         "reattempts": 5,
         "resubmit_failed": True,
         "autoterminate": False,
         "write_workflow": True,
         "rts": "mock",
         "rts_config": { "sandbox_cleanup": True, "db_cleanup": True},
         "pending_qs": 2,
         "completed_qs": 3,
         "rmq_cleanup": False}

    ru.write_json(d, './config.json')
    amgr._read_config(config_path='./',
                      hostname=None,
                      port=None,
                      reattempts=None,
                      resubmit_failed=None,
                      autoterminate=None,
                      write_workflow=None,
                      rts=None,
                      rmq_cleanup=None,
                      rts_config=None)

    assert amgr._mq_hostname == d['hostname']
    assert amgr._port == d['port']
    assert amgr._reattempts == d['reattempts']
    assert amgr._resubmit_failed == d['resubmit_failed']
    assert amgr._autoterminate == d['autoterminate']
    assert amgr._write_workflow == d['write_workflow']
    assert amgr._rts == d['rts']
    assert amgr._rts_config == d['rts_config']
    assert amgr._num_pending_qs == d['pending_qs']
    assert amgr._num_completed_qs == d['completed_qs']
    assert amgr._rmq_cleanup == d['rmq_cleanup']

    os.remove('./config.json')


def test_amgr_resource_description_assignment():

    res_dict = {

        'resource': 'xsede.supermic',
        'walltime': 30,
        'cpus': 1000,
        'project': 'TG-MCB090174'

    }

    amgr = Amgr(rts='radical.pilot')
    amgr.resource_desc = res_dict
    from radical.entk.execman.rp import ResourceManager
    assert isinstance(amgr._resource_manager, ResourceManager)

    amgr = Amgr(rts='mock')
    amgr.resource_desc = res_dict
    from radical.entk.execman.mock import ResourceManager
    assert isinstance(amgr._resource_manager, ResourceManager)


def test_amgr_assign_workflow():

    amgr = Amgr()

    with pytest.raises(TypeError):
        amgr.workflow = [1, 2, 3]

    with pytest.raises(TypeError):
        amgr.workflow = set([1, 2, 3])

    p1 = Pipeline()
    p2 = Pipeline()
    p3 = Pipeline()

    amgr._workflow = [p1, p2, p3]
    amgr._workflow = set([p1, p2, p3])


@given(s=st.characters(),
       i=st.integers().filter(lambda x: type(x) == int),
       b=st.booleans(),
       se=st.sets(st.text()))
def test_amgr_assign_shared_data(s,i,b,se):
    amgr = Amgr(rts='radical.pilot', hostname=hostname, port=port)

    res_dict = {

        'resource': 'xsede.supermic',
        'walltime': 30,
        'cpus': 20,
        'project': 'TG-MCB090174'

    }

    amgr.resource_desc = res_dict

    data = [s, i, b, se]

    for d in data:
        with pytest.raises(TypeError):
            amgr.shared_data = d

    amgr.shared_data = ['file1.txt','file2.txt']
    assert amgr._resource_manager.shared_data == ['file1.txt','file2.txt'] 


def test_amgr_run():

    amgr = Amgr()

    with pytest.raises(MissingError):
        amgr.run()

    p1 = Pipeline()
    p2 = Pipeline()
    p3 = Pipeline()

    amgr._workflow = [p1, p2, p3]

    with pytest.raises(MissingError):
        amgr.run()

    # Remaining lines of run() should be tested in the integration
    # tests


def test_amgr_resource_terminate():

    res_dict = {

        'resource': 'xsede.supermic',
        'walltime': 30,
        'cpus': 20,
        'project': 'TG-MCB090174'

    }

    from radical.entk.execman.rp import TaskManager

    amgr = Amgr(rts='radical.pilot', hostname=hostname, port=port)
    amgr.resource_desc = res_dict
    amgr._setup_mqs()
    amgr._rmq_cleanup = True
    amgr._task_manager = TaskManager(sid='test',
                                     pending_queue=list(),
                                     completed_queue=list(),
                                     mq_hostname='localhost',
                                     rmgr=amgr._resource_manager,
                                     port=amgr._port
                                     )

    amgr.resource_terminate()


def test_amgr_setup_mqs():

    amgr = Amgr(hostname=hostname, port=port)
    assert amgr._setup_mqs() == True

    assert len(amgr._pending_queue) == 1
    assert len(amgr._completed_queue) == 1

    mq_connection = pika.BlockingConnection(pika.ConnectionParameters(host=amgr._mq_hostname, port=amgr._port))
    mq_channel = mq_connection.channel()

    qs = [
        '%s-tmgr-to-sync' % amgr._sid,
        '%s-cb-to-sync' % amgr._sid,
        '%s-enq-to-sync' % amgr._sid,
        '%s-deq-to-sync' % amgr._sid,
        '%s-sync-to-tmgr' % amgr._sid,
        '%s-sync-to-cb' % amgr._sid,
        '%s-sync-to-enq' % amgr._sid,
        '%s-sync-to-deq' % amgr._sid
    ]

    for q in qs:
        mq_channel.queue_delete(queue=q)

    with open('.%s.txt' % amgr._sid, 'r') as fp:
        lines = fp.readlines()

    for i in range(len(lines)):
        lines[i] = lines[i].strip()

    assert set(qs) < set(lines)


def test_amgr_cleanup_mqs():

    amgr = Amgr(hostname=hostname, port=port)
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


def func_for_synchronizer_test(sid, p, logger, profiler):

    mq_connection = pika.BlockingConnection(pika.ConnectionParameters(host=hostname, port=port))
    mq_channel = mq_connection.channel()

    for t in p.stages[0].tasks:

        t.state = states.SCHEDULING
        sync_with_master(obj=t,
                         obj_type='Task',
                         channel=mq_channel,
                         queue='%s-tmgr-to-sync' % sid,
                         logger=logger,
                         local_prof=profiler)

    p.stages[0].state = states.SCHEDULING
    sync_with_master(obj=p.stages[0],
                     obj_type='Stage',
                     channel=mq_channel,
                     queue='%s-enq-to-sync' % sid,
                     logger=logger,
                     local_prof=profiler)

    p.state = states.SCHEDULING
    sync_with_master(obj=p,
                     obj_type='Pipeline',
                     channel=mq_channel,
                     queue='%s-deq-to-sync' % sid,
                     logger=logger,
                     local_prof=profiler)


def test_amgr_synchronizer():

    logger = ru.get_logger('radical.entk.temp_logger')
    profiler = ru.Profiler(name='radical.entk.temp')
    amgr = Amgr(hostname=hostname, port=port)

    mq_connection = pika.BlockingConnection(pika.ConnectionParameters(host=hostname, port=port))
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
    p._assign_uid(amgr._sid)
    p._validate()

    amgr.workflow = [p]

    for t in p.stages[0].tasks:
        assert t.state == states.INITIAL

    assert p.stages[0].state == states.INITIAL
    assert p.state == states.INITIAL

    # Start the synchronizer method in a thread
    amgr._terminate_sync = Event()
    sync_thread = Thread(target=amgr._synchronizer, name='synchronizer-thread')
    sync_thread.start()

    # Start the synchronizer method in a thread
    proc = Process(target=func_for_synchronizer_test, name='temp-proc',
                   args=(amgr._sid, p, logger, profiler))

    proc.start()
    proc.join()

    for t in p.stages[0].tasks:
        assert t.state == states.SCHEDULING

    assert p.stages[0].state == states.SCHEDULING
    assert p.state == states.SCHEDULING

    amgr._terminate_sync.set()
    sync_thread.join()


def test_sid_in_mqs():

    appman = Amgr(hostname=hostname, port=port)
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


def test_state_order():

    """
    **Purpose**: Test if the Pipeline, Stage and Task are assigned their states in the correct order
    """

    def create_single_task():

        t1 = Task()
        t1.name = 'simulation'
        t1.executable = ['/bin/date']
        t1.copy_input_data = []
        t1.copy_output_data = []

        return t1

    p1 = Pipeline()
    p1.name = 'p1'

    s = Stage()
    s.name = 's1'
    s.tasks = create_single_task()
    s.add_tasks(create_single_task())

    p1.add_stages(s)

    res_dict = {

            'resource': 'local.localhost',
            'walltime': 5,
            'cpus': 1,
            'project': ''

    }

    os.environ['RADICAL_PILOT_DBURL'] = MLAB
    os.environ['RP_ENABLE_OLD_DEFINES'] = 'True'
    
    appman = Amgr(hostname=hostname, port=port)
    appman.resource_desc = res_dict

    appman.workflow = [p1]
    appman.run()

    p_state_hist = p1.state_history
    assert p_state_hist == ['DESCRIBED', 'SCHEDULING', 'DONE']

    s_state_hist = p1.stages[0].state_history
    assert s_state_hist == ['DESCRIBED', 'SCHEDULING', 'SCHEDULED', 'DONE']

    tasks = p1.stages[0].tasks

    for t in tasks:

        t_state_hist = t.state_history
        assert t_state_hist == ['DESCRIBED', 'SCHEDULING', 'SCHEDULED', 'SUBMITTING', 'SUBMITTED',
                            'EXECUTED', 'DEQUEUEING', 'DEQUEUED', 'DONE']
