from radical.entk import Pipeline, Stage, Task, AppManager as Amgr
from radical.entk import states
from radical.entk.exceptions import *
import pytest
from hypothesis import given
import hypothesis.strategies as st
import threading
import radical.utils as ru

def test_initialization():

    amgr = Amgr()

    assert len(amgr._uid.split('.'))==2
    assert 'appmanager' == amgr._uid.split('.')[0]
    assert type(amgr._logger) == type(ru.get_logger('radical.tests'))
    assert type(amgr._prof) == type(ru.Profiler('radical.tests'))
    assert isinstance(amgr.name, str)

    # RabbitMQ inits
    assert amgr._mq_hostname == 'localhost'
    assert amgr._port == 5672

    # RabbitMQ Queues
    assert amgr._num_pending_qs == 1
    assert amgr._num_completed_qs == 1
    assert isinstance(amgr._pending_queue, list)
    assert isinstance(amgr._completed_queue, list)

    # Threads and procs counts
    assert amgr._num_push_threads == 1
    assert amgr._num_pull_threads == 1
    assert amgr._num_sync_threads == 1

    # Global parameters to have default values
    assert amgr._mqs_setup == False
    assert amgr._resource_manager == None
    assert amgr._task_manager == None
    assert amgr._workflow  == None
    assert amgr._resubmit_failed == False
    assert amgr._reattempts == 3
    assert amgr._cur_attempt == 1
    assert amgr._resource_autoterminate == True

    # Check if RP Profiler is set
    assert amgr._rp_profile == False


@given(t=st.text(),
       l=st.lists(st.text()),
       i=st.integers().filter(lambda x: type(x) == int),
       b=st.booleans(),
       se=st.sets(st.text()),
       d=st.dictionaries(st.text(),st.text()))
def test_assignment_exceptions(t,l,i,b,se,d):
    

    amgr = Amgr()

    data_type = [t,l,i,b,se,d]

    for data in data_type:

        print 'Using: %s, %s'%(data, type(data))

        with pytest.raises(TypeError):
            amgr.resource_manager = data

        if not isinstance(data,str):
            with pytest.raises(TypeError):
                amgr.name = data