from radical.entk import Pipeline, Stage, Task
from radical.entk import states
from radical.entk.exceptions import *
import pytest

def test_issue():

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

        return set(p)
    

    res_dict = {

            'resource': 'local.localhost',
            'walltime': 5,
            'cores': 1,
            'project': ''

    }

    os.environ['RADICAL_PILOT_DBURL'] = 'mongodb://user:user@ds129013.mlab.com:29013/travis_tests'
    os.environ['RP_ENABLE_OLD_DEFINES'] = 'True'

    rman = ResourceManager(res_dict)

    appman = AppManager(autoterminate=False)
    appman.resource_manager = rman
    p1 = create_pipeline()
    appman.assign_workflow(p1)
    appman.run()

    p2 = create_pipeline()
    appman.assign_workflow(p2)
    appman.run()

    appman.resource_terminate()

    assert int(p1.stages[0].uid.split('.')[-1]) + 1 == int(p2.stages[0].uid.split('.')[-1])

    for t in p1.stages[0].tasks:
        for tt in p2.stages[0].tasks:
           assert int(t.uid.split('.')[-1]) + 1 == int(tt.uid.split('.')[-1]) 