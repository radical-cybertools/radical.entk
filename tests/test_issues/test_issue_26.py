from radical.entk import Pipeline, Stage, Task, AppManager
from radical.entk import states
from radical.entk.exceptions import *
import pytest
import os

hostname = os.environ.get('RMQ_HOSTNAME','localhost')
port = int(os.environ.get('RMQ_PORT',5672))
# MLAB = 'mongodb://entk:entk123@ds143511.mlab.com:43511/entk_0_7_4_release'
MLAB = os.environ.get('RADICAL_PILOT_DBURL')

def test_issue_26():

    def create_pipeline():

        p = Pipeline()

        s = Stage()

        t1 = Task()
        t1.name = 'simulation'
        t1.executable = ['/bin/echo']
        t1.arguments = ['hello']
        t1.copy_input_data = []
        t1.copy_output_data = []

        s.add_tasks(t1)

        p.add_stages(s)

        return p


    res_dict = {

            'resource': 'local.localhost',
            'walltime': 10,
            'cpus': 1,
            'project': ''

    }

    os.environ['RADICAL_PILOT_DBURL'] = MLAB

    appman = AppManager(hostname=hostname, port=port, autoterminate=False)
    appman.resource_desc = res_dict


    p1 = create_pipeline()
    appman.workflow = [p1]
    appman.run()
    print p1.uid, p1.stages[0].uid

    p2 = create_pipeline()
    appman.workflow = [p2]
    appman.run()
    print p2.uid, p2.stages[0].uid

    appman.resource_terminate()

    lhs = int(p1.stages[0].uid.split('.')[-1]) + 1
    rhs = int(p2.stages[0].uid.split('.')[-1])
    assert lhs == rhs

    for t in p1.stages[0].tasks:
        for tt in p2.stages[0].tasks:
            lhs = int(t.uid.split('.')[-1]) + 1
            rhs = int(tt.uid.split('.')[-1])
            assert lhs == rhs
