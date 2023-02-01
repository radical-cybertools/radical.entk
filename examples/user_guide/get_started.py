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
    s = Stage()

    # Create a Task object
    t = Task()
    t.name = 'my.first.task'        # Assign a name to the task (optional, do not use ',' or '_')
    t.executable = '/bin/echo'   # Assign executable to the task
    t.arguments = ['Hello World']  # Assign arguments for the task executable

    # Add Task to the Stage
    s.add_tasks(t)

    # Add Stage to the Pipeline
    p.add_stages(s)

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
