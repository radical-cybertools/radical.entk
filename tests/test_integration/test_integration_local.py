from radical.entk import Pipeline, Stage, Task, AppManager
import pytest
from radical.entk.exceptions import *
import os

hostname = os.environ.get('RMQ_HOSTNAME','localhost')
port = int(os.environ.get('RMQ_PORT',5672))
MLAB = os.environ.get('RADICAL_PILOT_DBURL')

def test_integration_local():

    """
    **Purpose**: Run an EnTK application on localhost
    """

    def create_single_task():

        t1 = Task()
        t1.name = 'simulation'
        t1.executable = '/bin/echo'
        t1.arguments = ['hello']
        t1.copy_input_data = []
        t1.copy_output_data = []

        return t1

    p1 = Pipeline()
    p1.name = 'p1'

    s = Stage()
    s.name = 's1'
    s.tasks = create_single_task()
    s.add_tasks(create_single_task())

    p1.add_stages(s)

    res_dict = {

            'resource': 'local.localhost',
            'walltime': 5,
            'cpus': 1,
            'project': ''

    }

    os.environ['RADICAL_PILOT_DBURL'] = MLAB

    appman = AppManager(hostname=hostname, port=port)
    appman.resource_desc = res_dict
    appman.workflow = [p1]
    appman.run()
