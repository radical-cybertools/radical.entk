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


@given(s=st.text(),
       l=st.lists(st.characters()),
       i=st.integers())
def test_base_initialization(s, l, i):

    sid = 'test.0000'
    rmgr = BaseRmgr({}, sid, None)

    tmgr = BaseTmgr(sid=sid,
                    pending_queue=['pending'],
                    completed_queue=['completed'],
                    rmgr=rmgr,
                    mq_hostname='localhost',
                    port=123,
                    rts=None)

    assert 'task_manager' in tmgr._uid
    assert sid == tmgr._sid
    assert tmgr._pending_queue == ['pending']
    assert tmgr._completed_queue == ['completed']
    assert tmgr._mq_hostname == 'localhost'
    assert tmgr._port == 123
    assert tmgr._rts == None

    assert tmgr._tmgr_process == None
    assert tmgr._hb_thread == None



@given(s=st.text(),
       l=st.lists(st.characters()),
       i=st.integers())
def test_rp_initialization(s, l, i):

    sid = 'test.0000'
    db_url = 'mongodb://138.201.86.166:27017/ee_exp_4c'
    os.environ['RADICAL_PILOT_DBURL'] = db_url

    rmgr = RPRmgr({}, sid)

    tmgr = RPTmgr(  sid=sid,
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


@given(s=st.text(),
       l=st.lists(st.characters()),
       i=st.integers())
def test_dummy_initialization(s, l, i):

    sid = 'test.0000'
    rmgr = DummyRmgr(resource_desc={}, sid=sid)

    tmgr = DummyTmgr(sid=sid,
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
    assert tmgr._rts == 'dummy'

    assert tmgr._tmgr_process == None
    assert tmgr._hb_thread == None



@given(s=st.characters(),
       l=st.lists(st.characters()),
       i=st.integers().filter(lambda x: type(x) == int),
       b=st.booleans(),
       se=st.sets(st.text()),
       di=st.dictionaries(st.text(), st.text()))
def test_assignment_exceptions(s, l, i, b, se, di):

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
