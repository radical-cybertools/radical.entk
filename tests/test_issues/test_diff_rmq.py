from radical.entk import Pipeline, Stage, Task, AppManager
import pytest
import os

hostname = os.environ.get('RMQ_HOSTNAME', 'localhost')
port = int(os.environ.get('RMQ_PORT', 5672))
username = os.environ.get('RMQ_USERNAME')
password = os.environ.get('RMQ_PASSWORD')


@pytest.mark.skip(reason="no need to test this for the moment")
def test_diff_rmq():

    def create_pipeline():

        p = Pipeline()

        s = Stage()

        t1 = Task()
        t1.name = 'simulation'
        t1.executable = '/bin/echo'
        t1.arguments = ['hello']
        t1.copy_input_data = []
        t1.copy_output_data = []

        s.add_tasks(t1)

        p.add_stages(s)

        return p


    res_dict = {

            'resource': 'local.localhost',
            'walltime': 5,
            'cpus': 1,

    }

    appman = AppManager(hostname=hostname, port=port, username=username, password=password)
    appman.resource_desc = res_dict

    p1 = create_pipeline()
    print(p1.uid, p1.stages[0].uid)
    appman.workflow = [p1]
    appman.run()
