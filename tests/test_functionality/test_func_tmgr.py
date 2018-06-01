from radical.entk.execman.base.resource_manager import Base_ResourceManager as BaseRmgr
from radical.entk.execman.rp.resource_manager import ResourceManager as RPRmgr
from radical.entk.execman.dummy.resource_manager import ResourceManager as DummyRmgr
from radical.entk.execman.base.task_manager import Base_TaskManager as BaseTmgr
from radical.entk.execman.rp.task_manager import TaskManager as RPTmgr
from radical.entk.execman.dummy.task_manager import TaskManager as DummyTmgr
import pytest
from radical.entk.exceptions import *
import os

def test_tmgr_process():

    """
    **Purpose**: Test the functions to start and terminate the tmgr process
    """

    res_dict = {
                    'resource': 'local.localhost',
                    'walltime': 40,
                    'cores': 20,
                    'project': 'Random'
                }
    
    os.environ['RADICAL_PILOT_DBURL'] = 'mlab-url'

    rm = ResourceManager(res_dict)

    t = TaskManager(    sid ='xyz',
                        pending_queue = ['pendingq'], 
                        completed_queue = ['completedq'], 
                        rmgr = rm, 
                        mq_hostname = 'localhost', 
                        port = 5672)
    t.start_manager()
    assert t.check_alive() == True
    t.end_manager()
    assert t.check_alive() == False


def test_heartbeat():

    """
    **Purpose**: Test the functions to start and terminate the heartbeat thread
    """

    res_dict = {
                    'resource': 'local.localhost',
                    'walltime': 40,
                    'cores': 20,
                    'project': 'Random'
                }
    
    os.environ['RADICAL_PILOT_DBURL'] = 'mlab-url'

    rm = ResourceManager(res_dict)

    t = TaskManager(    sid ='xyz',
                        pending_queue = ['pendingq'], 
                        completed_queue = ['completedq'], 
                        rmgr = rm, 
                        mq_hostname = 'localhost', 
                        port = 5672)

    t.start_heartbeat()
    assert t.check_heartbeat() == True
    t.end_heartbeat()
    assert t.check_heartbeat() == False