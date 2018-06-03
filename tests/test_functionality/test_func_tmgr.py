# from radical.entk.execman.base.resource_manager import Base_ResourceManager as BaseRmgr
from radical.entk.execman.rp.resource_manager import ResourceManager as RPRmgr
from radical.entk.execman.dummy.resource_manager import ResourceManager as DummyRmgr
# from radical.entk.execman.base.task_manager import Base_TaskManager as BaseTmgr
from radical.entk.execman.rp.task_manager import TaskManager as RPTmgr
from radical.entk.execman.dummy.task_manager import TaskManager as DummyTmgr
import pytest
from radical.entk.exceptions import *
import os

def test_rp_tmgr():

    """
    **Purpose**: Test the functions to start and terminate the RP tmgr process
    """

    res_dict = {
                    'resource': 'local.localhost',
                    'walltime': 40,
                    'cores': 20,
                    'project': 'Random'
                }
    
    os.environ['RADICAL_PILOT_DBURL'] = 'mlab-url'

    rm = RPRmgr(res_dict, sid='test.0000')

    t = RPTmgr( sid='test.0000',
                pending_queue = ['pendingq'], 
                completed_queue = ['completedq'], 
                rmgr = rm, 
                mq_hostname = 'localhost', 
                port = 5672)
    t.start_manager()
    assert t.check_manager() == True
    t.terminate_manager()
    assert t.check_manager() == False


def test_rp_hb():

    """
    **Purpose**: Test the functions to start and terminate the RP heartbeat thread
    """

    res_dict = {
                    'resource': 'local.localhost',
                    'walltime': 40,
                    'cores': 20,
                    'project': 'Random'
                }
    
    os.environ['RADICAL_PILOT_DBURL'] = 'mlab-url'

    rm = RPRmgr(res_dict, sid='test.0000')

    t = RPTmgr( sid='test.0000',
                pending_queue = ['pendingq'], 
                completed_queue = ['completedq'], 
                rmgr = rm, 
                mq_hostname = 'localhost', 
                port = 5672)

    t.start_heartbeat()
    assert t.check_heartbeat() == True
    t.terminate_heartbeat()
    assert t.check_heartbeat() == False

def test_dummy_tmgr():
    
    """
    **Purpose**: Test the functions to start and terminate the dummy tmgr process
    """

    res_dict = {
                    'resource': 'local.localhost',
                    'walltime': 40,
                    'cores': 20,
                    'project': 'Random'
                }
    
    os.environ['RADICAL_PILOT_DBURL'] = 'mlab-url'

    rm = DummyRmgr(resource_desc=res_dict, sid='test.0000')

    t = DummyTmgr( sid='test.0000',
                pending_queue = ['pendingq'], 
                completed_queue = ['completedq'], 
                rmgr = rm, 
                mq_hostname = 'localhost', 
                port = 5672)
    
    t.start_manager()
    assert t.check_manager() == True
    t.terminate_manager()
    assert t.check_manager() == False

def test_dummy_heartbear():
    
    """
    **Purpose**: Test the functions to start and terminate the dummy heartbeat thread
    """

    res_dict = {
                    'resource': 'local.localhost',
                    'walltime': 40,
                    'cores': 20,
                    'project': 'Random'
                }
    
    os.environ['RADICAL_PILOT_DBURL'] = 'mlab-url'

    rm = DummyRmgr(resource_desc=res_dict, sid='test.0000')

    t = DummyTmgr( sid='test.0000',
                pending_queue = ['pendingq'], 
                completed_queue = ['completedq'], 
                rmgr = rm, 
                mq_hostname = 'localhost', 
                port = 5672)

    t.start_heartbeat()
    assert t.check_heartbeat() == True
    t.terminate_heartbeat()
    assert t.check_heartbeat() == False