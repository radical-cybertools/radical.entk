from radical.entk.execman.base import Base_TaskManager as BaseTmgr
from radical.entk.execman.base import Base_ResourceManager as BaseRmgr
from radical.entk.execman.rp import TaskManager as RPTmgr
from radical.entk.execman.rp import ResourceManager as RPRmgr
from radical.entk.execman.dummy import TaskManager as DummyTmgr
from radical.entk.execman.dummy import ResourceManager as DummyRmgr
from radical.entk.exceptions import *
import pytest
from hypothesis import given
import hypothesis.strategies as st
import os
from radical.entk.exceptions import *
from time import sleep


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
    rmgr = BaseRmgr({}, sid, None)

    tmgr = BaseTmgr(sid=sid,
                    pending_queue=['pending-1'],
                    completed_queue=['completed-1'],
                    rmgr=rmgr,
                    mq_hostname='localhost',
                    port=123,
                    rts=None)

    assert tmgr._uid == 'task_manager.0000'
    assert tmgr._pending_queue == ['pending-1']
    assert tmgr._completed_queue == ['completed-1']
    assert tmgr._mq_hostname == 'localhost'
    assert tmgr._port == 123
    assert tmgr._rts == None

    assert tmgr._logger
    assert tmgr._prof
    assert tmgr._hb_request_q == '%s-hb-request' % sid
    assert tmgr._hb_response_q == '%s-hb-response' % sid
    assert not tmgr._tmgr_process
    assert not tmgr._hb_thread


@given(s=st.characters(),
       l=st.lists(st.characters()),
       i=st.integers().filter(lambda x: type(x) == int),
       b=st.booleans(),
       se=st.sets(st.text()),
       di=st.dictionaries(st.text(), st.text()))
def test_tmgr_base_assignment_exceptions(s, l, i, b, se, di):

    sid = 'test.0000'
    rmgr = BaseRmgr({}, sid, None)

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


def test_tmgr_base_heartbeat():
    pass

def test_tmgr_base_start_heartbeat():
    pass

def test_tmgr_base_terminate_heartbeat():
    pass

def test_tmgr_base_terminate_manager():
    pass

def test_tmgr_base_check_heartbeat():
    pass

def test_tmgr_base_check_manager():
    pass

def test_tmgr_base_methods():

    sid = 'test.0000'
    rmgr = BaseRmgr({}, sid, None)

    tmgr = BaseTmgr(sid=sid,
                    pending_queue=['pending-1'],
                    completed_queue=['completed-1'],
                    rmgr=rmgr,
                    mq_hostname='localhost',
                    port=123,
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
def test_tmgr_dummy_initialization(s, l, i):

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
    rmgr = DummyRmgr(resource_desc={}, sid=sid)

    tmgr = DummyTmgr(sid=sid,
                     pending_queue=['pending'],
                     completed_queue=['completed'],
                     rmgr=rmgr,
                     mq_hostname='localhost',
                     port=123)

    assert tmgr._uid == 'task_manager.0000'
    assert tmgr._pending_queue == ['pending']
    assert tmgr._completed_queue == ['completed']
    assert tmgr._mq_hostname == 'localhost'
    assert tmgr._port == 123
    assert tmgr._rts == 'dummy'

    assert tmgr._logger
    assert tmgr._prof
    assert tmgr._hb_request_q == '%s-hb-request' % sid
    assert tmgr._hb_response_q == '%s-hb-response' % sid
    assert tmgr._tmgr_process == None
    assert tmgr._hb_thread == None
    assert tmgr._rmq_ping_interval == 10


def test_tmgr_dummy_tmgr():

    res_dict = {
        'resource': 'local.localhost',
                    'walltime': 40,
                    'cores': 20,
                    'project': 'Random'
    }

    os.environ['RADICAL_PILOT_DBURL'] = 'mlab-url'

    rm = DummyRmgr(resource_desc=res_dict, sid='test.0000')

    t = DummyTmgr(sid='test.0000',
                  pending_queue=['pendingq'],
                  completed_queue=['completedq'],
                  rmgr=rm,
                  mq_hostname='localhost',
                  port=5672)

    t.start_heartbeat()
    assert t.check_heartbeat() == True
    t.terminate_heartbeat()
    assert t.check_heartbeat() == False


def test_tmgr_dummy_start_manager():
    pass


@given(s=st.text(),
       l=st.lists(st.characters()),
       i=st.integers())
def test_tmgr_rp_initialization(s, l, i):

    sid = 'test.0000'
    db_url = 'mongodb://138.201.86.166:27017/ee_exp_4c'
    os.environ['RADICAL_PILOT_DBURL'] = db_url

    rmgr = RPRmgr({}, sid)

    tmgr = RPTmgr(sid=sid,
                  pending_queue=['pending'],
                  completed_queue=['completed'],
                  rmgr=rmgr,
                  mq_hostname='localhost',
                  port=123)

    assert 'task_manager' in tmgr._uid
    assert tmgr._pending_queue == ['pending']
    assert tmgr._completed_queue == ['completed']
    assert tmgr._mq_hostname == 'localhost'
    assert tmgr._port == 123
    assert tmgr._rts == 'radical.pilot'
    assert tmgr._umgr == None

    assert tmgr._tmgr_process == None
    assert tmgr._hb_thread == None


def test_tmgr_rp_tmgr():

    res_dict = {
        'resource': 'local.localhost',
                    'walltime': 40,
                    'cpus': 20,
                    'project': 'Random'
    }

    os.environ['RADICAL_PILOT_DBURL'] = 'mlab-url'

    rm = RPRmgr(res_dict, sid='test.0000')

    t = RPTmgr(sid='test.0000',
               pending_queue=['pendingq'],
               completed_queue=['completedq'],
               rmgr=rm,
               mq_hostname='localhost',
               port=5672)
    t.start_manager()
    assert t.check_manager() == True
    t.terminate_manager()
    sleep(10)
    assert t.check_manager() == False


def test_tmgr_rp_start_manager():
    pass