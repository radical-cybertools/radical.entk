#!/usr/bin/env python3


import os
import sys
import time

import radical.entk as re


# ------------------------------------------------------------------------------
#
hostname =     os.environ.get('RMQ_HOSTNAME', 'localhost')
port     = int(os.environ.get('RMQ_PORT',      5672))

pipes    = list()
cnt      = 0


# ------------------------------------------------------------------------------
#
def generate_pipeline(master=False):

    global pipes

    if master:
        def func_post_1():
            for p in pipes[1:]:
                p.suspend()

        def func_post_2():
            for p in pipes[1:]:
                p.resume()
            # return list of resumed pipeline IDs
            return [p.uid for p in pipes[1:]]

    else:
        def func_post_1(): pass
        def func_post_2(): pass

    # --------------------------------------------------------------------------

    # create a pipeline, stage and tasks

    t1 = re.Task()
    t1.executable = '/bin/sleep'
    if master: t1.arguments = [' 1']
    else     : t1.arguments = ['10']

    s1 = re.Stage()
    s1.add_tasks(t1)
    s1.post_exec = func_post_1

    t2 = re.Task()
    t2.executable = '/bin/sleep'
    t2.arguments  = ['1']

    s2 = re.Stage()
    s2.add_tasks(t2)
    s2.post_exec = func_post_2

    p = re. Pipeline()
    p.add_stages(s1)
    p.add_stages(s2)

    return p


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    # Create a dictionary describe four mandatory keys:
    # resource, walltime, cores and project
    # resource is 'local.localhost' to execute locally
    res_dict = {
        'resource': 'local.localhost',
        'walltime': 15,
        'cpus'    : 2,
    }

    # Create Application Manager
    appman = re.AppManager(hostname=hostname, port=port)
    appman.resource_desc = res_dict

    pipes.append(generate_pipeline(True))
    pipes.append(generate_pipeline(False))
    pipes.append(generate_pipeline(False))
    pipes.append(generate_pipeline(False))

    # Assign the workflow as a set of Pipelines to the Application Manager
    appman.workflow = pipes

    def tmp():
        while True:
            print([p.state for p in pipes])
            time.sleep(1)

    import threading as mt
    t = mt.Thread(target=tmp)
    t.daemon = True
    t.start()

    # Run the Application Manager
    appman.run()
    appman.terminate()


# ------------------------------------------------------------------------------

