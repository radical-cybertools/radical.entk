from radical.entk import Pipeline, Stage, Task, AppManager
from radical.entk import states
from radical.entk.exceptions import *
import pytest
import os
from time import sleep

hostname = os.environ.get('RMQ_HOSTNAME', 'localhost')
port = int(os.environ.get('RMQ_PORT', 5672))
username = os.environ.get('RMQ_USERNAME', 'guest')
password = os.environ.get('RMQ_PASSWORD', 'guest')

def test_issue_255():

    def create_pipeline():

        p = Pipeline()

        s = Stage()

        t1 = Task()
        t1.name = 'simulation'
        t1.executable = 'sleep'
        t1.arguments = ['10']

        s.add_tasks(t1)

        p.add_stages(s)

        return p

    res_dict = {

        'resource': 'local.localhost',
        'walltime': 5,
        'cpus': 1,
        'project': ''

    }


    appman = AppManager(hostname=hostname, port=port, username=username,
            password=password, autoterminate=False)
    appman.resource_desc = res_dict

    p1 = create_pipeline()
    appman.workflow = [p1]
    appman.run()

    # p1 = create_pipeline()
    # appman.workflow = [p1]
    # appman.run()

    # appman.resource_terminate()

    tmgr = appman._task_manager

    tmgr.terminate_manager()
    # tmgr.terminate_heartbeat()

    tmgr.start_manager()
    # tmgr.start_heartbeat()

