from radical.entk import  AppManager as Amgr
from radical.entk import states
from radical.entk.exceptions import *
import pytest
from hypothesis import given
import hypothesis.strategies as st
import threading
import radical.utils as ru

def test_initialization():

    amgr = Amgr()

    assert len(amgr._uid.split('.'))==['appmanager','0000']
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
    assert amgr._workflow  == None
    assert amgr._resubmit_failed == False
    assert amgr._reattempts == 3
    assert amgr._cur_attempt == 1
    assert amgr._autoterminate == True

