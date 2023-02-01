#!/usr/bin/env python

from radical.entk import Pipeline, Stage, Task, AppManager
import os

# ------------------------------------------------------------------------------
# Set default verbosity
if os.environ.get('RADICAL_ENTK_VERBOSE') is None:
    os.environ['RADICAL_ENTK_REPORT'] = 'True'


if __name__ == '__main__':

    # Create a Pipeline object
    p = Pipeline()

    # Create a Stage object
    s1 = Stage()
    s1.name = 'Stage.1'

    for cnt in range(10):

        # Create a Task object
        t = Task()
        t.name = 'my.task'        # Assign a name to the task (optional)
        t.executable = '/bin/echo'   # Assign executable to the task
        t.arguments = ['I am task %s in %s' % (cnt, s1.name)]  # Assign arguments for the task executable

        # Add the Task to the Stage
        s1.add_tasks(t)

    # Add Stage to the Pipeline
    p.add_stages(s1)


    # Create another Stage object
    s2 = Stage()
    s2.name = 'Stage.2'

    for cnt in range(5):

        # Create a Task object
        t = Task()
        t.name = 'my.task'        # Assign a name to the task (optional, do not use ',' or '_')
        t.executable = '/bin/echo'   # Assign executable to the task
        t.arguments = ['I am task %s in %s' % (cnt, s2.name)]  # Assign arguments for the task executable

        # Add the Task to the Stage
        s2.add_tasks(t)

    # Add Stage to the Pipeline
    p.add_stages(s2)


    # Create Application Manager
    appman = AppManager()

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

    # Assign the workflow as a set or list of Pipelines to the Application Manager
    # Note: The list order is not guaranteed to be preserved
    appman.workflow = set([p])

    # Run the Application Manager
    appman.run()
