#!/usr/bin/env python

from radical.entk import Pipeline, Stage, Task, AppManager, ResourceManager
from radical.entk import Profiler
import os

# ------------------------------------------------------------------------------
# Set default verbosity
if os.environ.get('RADICAL_ENTK_VERBOSE') == None:
    os.environ['RADICAL_ENTK_REPORT'] = 'True'


os.environ['RADICAL_ENTK_PROFILE'] = 'True'

if __name__ == '__main__':

    # Create a Pipeline object
    p = Pipeline()

    # Create a Stage object
    s1 = Stage()

    # List to hold uids of Tasks of Stage 1
    s1_task_uids = list()

    for cnt in range(10):

        # Create a Task object
        t = Task()
        t.executable = '/bin/echo'   # Assign executable to the task
        t.arguments = ['I am task %s in %s'%(cnt, s1.name)]  # Assign arguments for the task executable

        # Add the Task to the Stage
        s1.add_tasks(t)

        # Add Task uid to list
        s1_task_uids.append(t.uid)

    # Add Stage to the Pipeline
    p.add_stages(s1)


    # Create another Stage object
    s2 = Stage()

    # List to hold uids of Tasks of Stage 2
    s2_task_uids = list()

    for cnt in range(5):

        # Create a Task object
        t = Task()
        t.executable = '/bin/echo'   # Assign executable to the task
        t.arguments = ['I am task %s in %s'%(cnt, s2.name)]  # Assign arguments for the task executable

        # Add the Task to the Stage
        s2.add_tasks(t)

        # Add Task uid to list
        s2_task_uids.append(t.uid)

    # Add Stage to the Pipeline
    p.add_stages(s2)


    # Create a dictionary describe four mandatory keys:
    # resource, walltime, cpus and project
    # resource is 'local.localhost' to execute locally
    res_dict = {

            'resource': 'local.localhost',
            'walltime': 10,
            'cpus': 1,
            'project': '',
    }

    # Create Resource Manager object with the above resource description
    rman = ResourceManager(res_dict)

    # Create Application Manager
    appman = AppManager()

    # Assign resource manager to the Application Manager
    appman.resource_manager = rman

    # Assign the workflow as a set of Pipelines to the Application Manager
    appman.assign_workflow(set([p]))

    # Run the Application Manager
    appman.run()


    p = Profiler(src='./%s'%appman.sid)

    print('Tasks in Stage 1: ', s1_task_uids)
    print('Execution time: ', p.duration(objects = s1_task_uids, states=['SCHEDULING', 'EXECUTED']))

    print('Tasks in Stage 2: ', s2_task_uids)
    print('Execution time: ', p.duration(objects = s2_task_uids, states=['SCHEDULING', 'EXECUTED']))
