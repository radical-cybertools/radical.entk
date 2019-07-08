#!/usr/bin/env python

from radical.entk import Pipeline, Stage, Task, AppManager

import os

hostname = os.environ.get('RMQ_HOSTNAME','localhost')
port     = int(os.environ.get('RMQ_PORT',5672))


# ------------------------------------------------------------------------------
#
def create_pipeline():

    p = Pipeline()
    s = Stage()
    t = Task()

    t.name             = 'simulation'
    t.executable       = ['/bin/echo']
    t.arguments        = ['hello']
    t.copy_input_data  = []
    t.copy_output_data = []

    s.add_tasks(t)
    p.add_stages(s)

    return p


# ------------------------------------------------------------------------------
#
def test_issue_26():

    appman = AppManager(hostname=hostname, port=port, autoterminate=False)
    appman.resource_desc = {'resource': 'local.localhost',
                            'walltime': 10,
                            'cpus'    : 1,
                            'project' : ''}

    p1 = create_pipeline()
    appman.workflow = [p1]
    appman.run()

    p2 = create_pipeline()
    appman.workflow = [p2]
    appman.run()

    appman.resource_terminate()

    lhs = int(p1.stages[0].uid.split('.')[-1]) + 1
    rhs = int(p2.stages[0].uid.split('.')[-1])
    assert lhs == rhs

    for t in p1.stages[0].tasks:
        for tt in p2.stages[0].tasks:
            lhs = int(t.uid.split('.')[-1]) + 1
            rhs = int(tt.uid.split('.')[-1])
            assert lhs == rhs


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    test_issue_26()


# ------------------------------------------------------------------------------

