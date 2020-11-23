#!/usr/bin/env python

from radical.entk import Pipeline, Stage, Task, AppManager
import os

# ------------------------------------------------------------------------------
# Set default verbosity
if os.environ.get('RADICAL_ENTK_VERBOSE') is None:
    os.environ['RADICAL_ENTK_REPORT'] = 'True'


# Description of how the RabbitMQ process is accessible
# No need to change/set any variables if you installed RabbitMQ has a system
# process. If you are running RabbitMQ under a docker container or another
# VM, set "RMQ_HOSTNAME" and "RMQ_PORT" in the session where you are running
# this script.
hostname = os.environ.get('RMQ_HOSTNAME', 'localhost')
port = os.environ.get('RMQ_PORT', 5672)
username = os.environ.get('RMQ_USERNAME')
password = os.environ.get('RMQ_PASSWORD')


def generate_pipeline(name, stages):

    # Create a Pipeline object
    p = Pipeline()
    p.name = name


    for s_cnt in range(stages):

        # Create a Stage object
        s = Stage()
        s.name = 'Stage %s' % s_cnt

        for t_cnt in range(5):

            # Create a Task object
            t = Task()
            t.name = 'my-task'        # Assign a name to the task (optional)
            t.executable = '/bin/echo'   # Assign executable to the task
            # Assign arguments for the task executable
            t.arguments = ['I am task %s in %s in %s' % (t_cnt, s_cnt, name)]

            # Add the Task to the Stage
            s.add_tasks(t)

        # Add Stage to the Pipeline
        p.add_stages(s)

    return p



if __name__ == '__main__':

    p1 = generate_pipeline(name='Pipeline 1', stages=1)
    p2 = generate_pipeline(name='Pipeline 2', stages=2)

    # Create Application Manager
    appman = AppManager(hostname=hostname, port=port, username=username,
            password=password)

    # Assign the workflow as a set or list of Pipelines to the Application Manager
    # Note: The list order is not guaranteed to be preserved
    appman.workflow = set([p1, p2])

    # Create a dictionary describe four mandatory keys:
    # resource, walltime, and cpus
    # resource is 'local.localhost' to execute locally
    res_dict = {

        'resource': 'local.localhost',
        'walltime': 10,
        'cpus': 1
    }

    # Assign resource request description to the Application Manager
    appman.resource_desc = res_dict

    # Run the Application Manager
    appman.run()
