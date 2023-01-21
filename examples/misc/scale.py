#!/usr/bin/env python

import os
import sys
import time

from radical.entk import Pipeline, Stage, Task, AppManager

host = os.environ.get('RMQ_HOSTNAME', 'localhost')
port = os.environ.get('RMQ_PORT',     5672)

n_pipes  = int(sys.argv[1])
n_stages = int(sys.argv[2])
n_tasks  = int(sys.argv[3])


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    start = time.time()


    pipelines = set()
    for _ in range(n_pipes):

        p = Pipeline()

        for _ in range(n_stages):

            s = Stage()

            for _ in range(n_tasks):

                t = Task()
                t.executable = '/bin/true'

                s.add_tasks(t)

            p.add_stages(s)

        pipelines.add(p)


    appman = AppManager(hostname=host, port=port, autoterminate=True, rts='mock')
    appman.resource_desc = {}
    appman.workflow      = pipelines

    appman.run()
    appman.terminate()

    print('%10.1f' % (time.time() - start))


# ------------------------------------------------------------------------------

