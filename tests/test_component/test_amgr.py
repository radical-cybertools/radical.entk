#!usr/bin/env python

import os
import pika
import pytest

from   hypothesis      import settings

import threading       as mt
import multiprocessing as mp

import radical.utils   as ru

from radical.entk.exceptions   import MissingError, EnTKError
from radical.entk              import Pipeline, Stage, Task, states

from radical.entk              import AppManager           as Amgr
from radical.entk.execman.base import Base_TaskManager     as BaseTmgr
from radical.entk.execman.base import Base_ResourceManager as BaseRmgr

# pylint: disable=protected-access

hostname =     os.environ.get('RMQ_HOSTNAME', 'localhost')
port     = int(os.environ.get('RMQ_PORT', 5672))

# Hypothesis settings
settings.register_profile("travis", max_examples=100, deadline=None)
settings.load_profile("travis")


# ------------------------------------------------------------------------------
#
def test_amgr_initialization():

    amgr_name = ru.generate_id('test.amgr.%(item_counter)04d', ru.ID_CUSTOM)
    amgr      = Amgr(hostname=hostname, port=port, name=amgr_name)

    assert amgr._name.split('.') == amgr_name.split('.')
    assert amgr._sid.split('.')  == amgr_name.split('.')
    assert amgr._uid.split('.')  == ['appmanager', '0000']

    assert isinstance(amgr._logger, ru.Logger)
    assert isinstance(amgr._prof,   ru.Profiler)
    assert isinstance(amgr._report, ru.Reporter)
    assert isinstance(amgr.name,    str)

    # RabbitMQ inits
    assert amgr._hostname == hostname
    assert amgr._port     == port

    # RabbitMQ Queues
    assert amgr._num_pending_qs   == 1
    assert amgr._num_completed_qs == 1

    assert isinstance(amgr._pending_queue,   list)
    assert isinstance(amgr._completed_queue, list)

    # Global parameters to have default values
    assert amgr._mqs_setup
    assert amgr._autoterminate

    assert amgr._resource_desc is None
    assert amgr._task_manager  is None
    assert amgr._workflow      is None

    assert not amgr._resubmit_failed

    assert amgr._reattempts  == 3
    assert amgr._cur_attempt == 1
    assert isinstance(amgr.shared_data, list)

    amgr = Amgr(hostname=hostname, port=port)

    assert amgr._uid.split('.') == ['appmanager', '0000']
    assert isinstance(amgr._logger, ru.Logger)
    assert isinstance(amgr._prof,   ru.Profiler)
    assert isinstance(amgr._report, ru.Reporter)
    assert isinstance(amgr.name, str)

    # RabbitMQ inits
    assert amgr._hostname == hostname
    assert amgr._port     == port

    # RabbitMQ Queues
    assert amgr._num_pending_qs   == 1
    assert amgr._num_completed_qs == 1

    assert isinstance(amgr._pending_queue, list)
    assert isinstance(amgr._completed_queue, list)

    # Global parameters to have default values
    assert amgr._mqs_setup
    assert amgr._autoterminate

    assert not amgr._resubmit_failed

    assert amgr._resource_desc is None
    assert amgr._task_manager  is None
    assert amgr._workflow      is None

    assert amgr._reattempts  == 3
    assert amgr._cur_attempt == 1

    assert isinstance(amgr.shared_data, list)


# ------------------------------------------------------------------------------
#
def test_amgr_read_config():

    amgr = Amgr()

    assert amgr._hostname   == 'localhost'
    assert amgr._port       == 5672
    assert amgr._reattempts == 3

    assert amgr._rmq_cleanup
    assert amgr._autoterminate

    assert not amgr._write_workflow
    assert not amgr._resubmit_failed

    assert amgr._rts              == 'radical.pilot'
    assert amgr._num_pending_qs   == 1
    assert amgr._num_completed_qs == 1
    assert amgr._rts_config       == {"sandbox_cleanup": False,
                                      "db_cleanup"     : False}

    d = {"hostname"       : "radical.two",
         "port"           : 25672,
         "reattempts"     : 5,
         "resubmit_failed": True,
         "autoterminate"  : False,
         "write_workflow" : True,
         "rts"            : "mock",
         "rts_config"     : {"sandbox_cleanup": True,
                             "db_cleanup"     : True},
         "pending_qs"     : 2,
         "completed_qs"   : 3,
         "rmq_cleanup"    : False}

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

    assert amgr._hostname         == d['hostname']
    assert amgr._port             == d['port']
    assert amgr._reattempts       == d['reattempts']
    assert amgr._resubmit_failed  == d['resubmit_failed']
    assert amgr._autoterminate    == d['autoterminate']
    assert amgr._write_workflow   == d['write_workflow']
    assert amgr._rts              == d['rts']
    assert amgr._rts_config       == d['rts_config']
    assert amgr._num_pending_qs   == d['pending_qs']
    assert amgr._num_completed_qs == d['completed_qs']
    assert amgr._rmq_cleanup      == d['rmq_cleanup']

    os.remove('./config.json')


# ------------------------------------------------------------------------------
#
def test_amgr_resource_description_assignment():

    res_dict = {'resource': 'xsede.supermic',
                'walltime': 30,
                'cpus'    : 1000,
                'project' : 'TG-MCB090174'}

    amgr = Amgr(rts='radical.pilot')
    amgr.resource_desc = res_dict

    from radical.entk.execman.rp import ResourceManager as RM_RP
    assert isinstance(amgr._rmgr, RM_RP)

    amgr = Amgr(rts='mock')
    amgr.resource_desc = res_dict

    from radical.entk.execman.mock import ResourceManager as RM_MOCK
    assert isinstance(amgr._rmgr, RM_MOCK)


# ------------------------------------------------------------------------------
#
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


# ------------------------------------------------------------------------------
#
def test_amgr_assign_shared_data():

    amgr = Amgr(rts='radical.pilot', hostname=hostname, port=port)

    res_dict = {'resource': 'xsede.supermic',
                'walltime': 30,
                'cpus'    : 20,
                'project' : 'TG-MCB090174'}

    amgr.resource_desc = res_dict
    amgr.shared_data   = ['file1.txt','file2.txt']

    assert amgr._rmgr.shared_data == ['file1.txt','file2.txt']


# ------------------------------------------------------------------------------
#
def test_amgr_run():

    amgr = Amgr(hostname=hostname, port=port)

    with pytest.raises(MissingError):
        amgr.run()

    p1 = Pipeline()
    p2 = Pipeline()
    p3 = Pipeline()

    with pytest.raises(MissingError):
        amgr.workflow = [p1, p2, p3]

    # Remaining lines of run() should be tested in the integration
    # tests


# ------------------------------------------------------------------------------
#
def test_amgr_run_mock():

    p = Pipeline()
    s = Stage()
    t = Task()

    t.name       = 'simulation'
    t.executable = '/bin/date'
    s.tasks      = t
    p.add_stages(s)

    res_dict = {'resource': 'local.localhost',
                'walltime': 5,
                'cpus'    : 1,
                'project' : ''}

    appman = Amgr(hostname=hostname, port=port, rts="mock")
    appman.resource_desc = res_dict

    appman.workflow = [p]
    appman.run()


# ------------------------------------------------------------------------------
#
def test_amgr_resource_terminate():

    res_dict = {'resource': 'xsede.supermic',
                'project' : 'TG-MCB090174',
                'walltime': 30,
                'cpus'    : 20}

    from radical.entk.execman.rp import TaskManager

    amgr = Amgr(rts='radical.pilot', hostname=hostname, port=port)
    amgr.resource_desc = res_dict

    amgr._setup_mqs()
    amgr._rmq_cleanup  = True
    amgr._task_manager = TaskManager(sid='test',
                                     pending_queue=list(),
                                     completed_queue=list(),
                                     mq_hostname=amgr._hostname,
                                     rmgr=amgr._rmgr,
                                     port=amgr._port)
    amgr.resource_terminate()
    # FIXME: what is tested (asserted) here?  What happens if terminate fails?


# ------------------------------------------------------------------------------
#
def test_amgr_terminate():

    res_dict = {'resource': 'xsede.supermic',
                'walltime': 30,
                'cpus'    : 20,
                'project' : 'TG-MCB090174'}

    from radical.entk.execman.rp import TaskManager

    amgr = Amgr(rts='radical.pilot', hostname=hostname, port=port)
    amgr.resource_desc = res_dict

    amgr._setup_mqs()
    amgr._rmq_cleanup  = True
    amgr._task_manager = TaskManager(sid='test',
                                     pending_queue=list(),
                                     completed_queue=list(),
                                     mq_hostname=amgr._hostname,
                                     rmgr=amgr._rmgr,
                                     port=amgr._port
                                     )
    amgr.terminate()
    # FIXME: what is tested (asserted) here?  What happens if terminate fails?


# ------------------------------------------------------------------------------
#
def test_amgr_setup_mqs():

    amgr = Amgr(hostname=hostname, port=port)
    amgr._setup_mqs()

    assert len(amgr._pending_queue)   == 1
    assert len(amgr._completed_queue) == 1

    mq_connection = pika.BlockingConnection(pika.ConnectionParameters(
                                       host=amgr._hostname, port=amgr._port))
    mq_channel    = mq_connection.channel()

    qs = ['%s-tmgr-to-sync' % amgr._sid,
          '%s-cb-to-sync'   % amgr._sid,
          '%s-sync-to-tmgr' % amgr._sid,
          '%s-sync-to-cb'   % amgr._sid,
          '%s-pendingq-1'   % amgr._sid,
          '%s-completedq-1' % amgr._sid]

    for q in qs:
        mq_channel.queue_delete(queue=q)

    with open('.%s.txt' % amgr._sid, 'r') as fp:
        lines = fp.readlines()

    for ind, val in enumerate(lines):
        lines[ind] = val.strip()

    assert set(qs) == set(lines)


# ------------------------------------------------------------------------------
#
def test_amgr_cleanup_mqs():

    amgr = Amgr(hostname=hostname, port=port)
    sid  = amgr._sid

    amgr._setup_mqs()
    amgr._cleanup_mqs()

    mq_connection = pika.BlockingConnection(pika.ConnectionParameters(
                                                      host=hostname, port=port))

    qs = ['%s-tmgr-to-sync' % sid,
          '%s-cb-to-sync'   % sid,
          '%s-sync-to-tmgr' % sid,
          '%s-sync-to-cb'   % sid,
          '%s-pendingq-1'   % sid,
          '%s-completedq-1' % sid]

    for q in qs:
        with pytest.raises(pika.exceptions.ChannelClosed):
            mq_channel = mq_connection.channel()
            mq_channel.queue_purge(q)


# ------------------------------------------------------------------------------
#
def func_for_synchronizer_test(sid, p, tmgr):

    # FIXME: what is tested / asserted here?

    mq_connection = pika.BlockingConnection(pika.ConnectionParameters(
                                                      host=hostname, port=port))
    mq_channel    = mq_connection.channel()

    for t in p.stages[0].tasks:

        t.state = states.COMPLETED
        tmgr._sync_with_master(obj=t,
                               obj_type='Task',
                               channel=mq_channel,
                               queue='%s-tmgr-to-sync' % sid)
    mq_connection.close()


# ------------------------------------------------------------------------------
#
def test_amgr_synchronizer():

    amgr = Amgr(hostname=hostname, port=port)
    amgr._setup_mqs()

    p = Pipeline()
    s = Stage()

    # Create and add 10 tasks to the stage
    for cnt in range(10):

        t = Task()
        t.executable = 'some-executable-%s' % cnt

        s.add_tasks(t)

    p.add_stages(s)
    p._assign_uid(amgr._sid)
    p._validate()

    amgr.workflow = [p]

    sid  = 'test.0016'
    rmgr = BaseRmgr({}, sid, None, {})
    tmgr = BaseTmgr(sid=sid,
                    pending_queue=['pending-1'],
                    completed_queue=['completed-1'],
                    rmgr=rmgr,
                    mq_hostname=hostname,
                    port=port,
                    rts=None)

    amgr._rmgr         = rmgr
    rmgr._task_manager = tmgr

    for t in p.stages[0].tasks:
        assert t.state == states.INITIAL

    assert p.stages[0].state == states.INITIAL
    assert p.state           == states.INITIAL

    # Start the synchronizer method in a thread
    amgr._terminate_sync = mt.Event()
    sync_thread = mt.Thread(target=amgr._synchronizer,
                            name='synchronizer-thread')
    sync_thread.start()

    # Start the synchronizer method in a thread
    proc = mp.Process(target=func_for_synchronizer_test, name='temp-proc',
                      args=(amgr._sid, p, tmgr))

    proc.start()
    proc.join()

    amgr._terminate_sync.set()
    sync_thread.join()

    for t in p.stages[0].tasks:
        assert t.state == states.COMPLETED


# ------------------------------------------------------------------------------
#
def test_sid_in_mqs():

    # FIXME: what is tested / asserted here?

    appman = Amgr(hostname=hostname, port=port)
    sid    = appman._sid
    appman._setup_mqs()

    qs = ['%s-tmgr-to-sync' % sid,
          '%s-cb-to-sync'   % sid,
          '%s-sync-to-tmgr' % sid,
          '%s-sync-to-cb'   % sid]

    mq_connection = pika.BlockingConnection(pika.ConnectionParameters(
                                                      host=hostname, port=port))
    mq_channel    = mq_connection.channel()

    def callback():
        pass

    for q in qs:
        try:
            mq_channel.basic_consume(callback, queue=q, no_ack=True)

        except Exception as ex:
            raise EnTKError(ex)


# ------------------------------------------------------------------------------
#
def test_state_order():
    """
    **Purpose**: Test if the Pipeline, Stage and Task are assigned their states
                 in the correct order
    """

    def create_single_task():

        t1 = Task()
        t1.name             = 'simulation'
        t1.executable       = '/bin/date'
        t1.copy_input_data  = []
        t1.copy_output_data = []
        return t1

    p1 = Pipeline()
    p1.name = 'p1'

    s = Stage()
    s.name  = 's1'
    s.tasks = create_single_task()
    s.add_tasks(create_single_task())

    p1.add_stages(s)

    res_dict = {'resource': 'local.localhost',
                'walltime': 5,
                'cpus'    : 1,
                'project' : ''}

    os.environ['RP_ENABLE_OLD_DEFINES'] = 'True'

    appman = Amgr(hostname=hostname, port=port)
    appman.resource_desc = res_dict

    appman.workflow = [p1]
    appman.run()

    p_state_hist = p1.state_history
    assert p_state_hist == ['DESCRIBED', 'SCHEDULING', 'DONE']

    s_state_hist = p1.stages[0].state_history
    assert s_state_hist == ['DESCRIBED', 'SCHEDULING', 'SCHEDULED', 'DONE']

    for t in p1.stages[0].tasks:
        assert t.state_history == ['DESCRIBED',  'SCHEDULING', 'SCHEDULED',
                                   'SUBMITTING', 'EXECUTED',   'DONE']


# ------------------------------------------------------------------------------
# pylint: enable=protected-access

