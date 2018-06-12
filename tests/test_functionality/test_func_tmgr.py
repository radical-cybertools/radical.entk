from radical.entk.execman.task_manager import TaskManager
from radical.entk import ResourceManager
import pytest
from radical.entk.exceptions import *
import os
from time import sleep

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
    assert t.check_manager() == True
    t.terminate_manager()
    sleep(10)
    assert t.check_manager() == False


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
    t.terminate_heartbeat()
    assert t.check_heartbeat() == False