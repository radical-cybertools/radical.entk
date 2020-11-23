#!/usr/bin/env python

from radical.entk import Pipeline, Stage, Task, AppManager
import os

host = os.environ.get('RMQ_HOSTNAME', 'localhost')
port = os.environ.get('RMQ_PORT',     5672)


def generate_pipeline():

    p = Pipeline()
    s1 = Stage()

    t1 = Task()
    t1.executable = '/bin/sleep'
    t1.arguments = ['3']

    s1.add_tasks(t1)

    p.add_stages(s1)
    s2 = Stage()
    t2 = Task()
    t2.executable = '/bin/sleep'
    t2.arguments = ['3']

    s2.add_tasks(t2)
    p.add_stages(s2)
    s3 = Stage()

    t3 = Task()
    t3.executable = '/bin/sleep'
    t3.arguments = ['3']

    s3.add_tasks(t3)
    p.add_stages(s3)

    return p


if __name__ == '__main__':

    appman   = AppManager(hostname=host, port=port, autoterminate=False)
    res_dict = {
        'resource': 'local.localhost',
        'walltime': 10,
        'cpus'    :  8
    }
    appman.resource_desc = res_dict


    pipelines = list()
    for cnt in range(2):
        pipelines.append(generate_pipeline())

    appman.workflow = set(pipelines)
    appman.run()

    print('1 ===================================================')


    pipelines = list()
    for cnt in range(2):
        pipelines.append(generate_pipeline())

    appman.workflow = set(pipelines)
    appman.run()

    print('2 ===================================================')

    appman.terminate()

    print('t ===================================================')
