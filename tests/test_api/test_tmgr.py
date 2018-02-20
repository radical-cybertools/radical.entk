from radical.entk.execman.task_manager import TaskManager as Tmgr
from radical.entk import ResourceManager
from radical.entk.exceptions import *
import pytest
from hypothesis import given
import hypothesis.strategies as st
import os

@given(s = st.characters(), i=st.integers())
def test_initialization(s,i):

    if not isinstance(s, str):
        return

    res_dict = {
                    'resource': 'local.localhost',
                    'walltime': 40,
                    'cores': 20,
                    'project': 'Random'
                }
    
    os.environ['RADICAL_PILOT_DBURL'] = 'mlab-url'

    rm = ResourceManager(res_dict)
    rm._validate_resource_desc(sid='rp.session.local.0000')
    rm._populate()

    tmgr = Tmgr(sid = 'rp.session.local.0000', 
                pending_queue = 'pending', 
                completed_queue = 'completed', 
                rmgr = rm, 
                mq_hostname = 'temp1', 
                port = 123
            )

    assert 'radical.entk.task_manager' in tmgr._uid
    assert tmgr._pending_queue == 'pending'
    assert tmgr._completed_queue == 'completed'
    assert tmgr._rmgr.walltime == res_dict['walltime']
    assert tmgr._rmgr.cores == res_dict['cores']
    assert tmgr._rmgr.project == res_dict['project']
    assert tmgr._rmgr.resource == res_dict['resource']
    assert tmgr._mq_hostname == 'temp1'
    assert tmgr._port == 123
    assert tmgr._tmgr_process == None
    assert tmgr._tmgr_terminate == None
    assert tmgr._hb_thread == None
    assert tmgr._hb_alive == None
    assert tmgr._umgr == None

    tmgr = Tmgr(sid = s, 
                pending_queue = s, 
                completed_queue = s, 
                rmgr = rm, 
                mq_hostname = s, 
                port = i
            )


@given(s=st.characters(),
       l=st.lists(st.characters()),
       i=st.integers().filter(lambda x: type(x) == int),
       b=st.booleans(),
       se=st.sets(st.text()),
       di=st.dictionaries(st.text(),st.text()))
def test_assignment_exceptions(s,l,i,b,se,di):


    res_dict = {
                    'resource': 'local.localhost',
                    'walltime': 40,
                    'cores': 20,
                    'project': 'Random'
                }
    
    os.environ['RADICAL_PILOT_DBURL'] = 'mlab-url'

    rm = ResourceManager(res_dict)

    data_type = [s,l,i,b,se,di]

    for d in data_type:

        if not isinstance(d,str):

            with pytest.raises(TypeError):

                tmgr = Tmgr(sid = d, 
                        pending_queue = d, 
                        completed_queue = d, 
                        rmgr = rm, 
                        mq_hostname = d, 
                        port = 567
                    )

        if not isinstance(d,int):

            with pytest.raises(TypeError):

                tmgr = Tmgr(sid = s, 
                        pending_queue = s, 
                        completed_queue = s, 
                        rmgr = rm, 
                        mq_hostname = s, 
                        port = d
                    )