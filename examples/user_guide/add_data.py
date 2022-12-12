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

    # Create a Task object which creates a file named 'output.txt' of size 1 MB
    t1 = Task()
    t1.executable = '/bin/bash'
    t1.arguments = ['-l', '-c', 'base64 /dev/urandom | head -c 1000000 > output.txt']

    # Add the Task to the Stage
    s1.add_tasks(t1)

    # Add Stage to the Pipeline
    p.add_stages(s1)


    # Create another Stage object
    s2 = Stage()
    s2.name = 'Stage.2'

    # Create a Task object
    t2 = Task()
    t2.executable = '/bin/bash'
    t2.arguments = ['-l', '-c', 'grep -o . output.txt | sort | uniq -c > ccount.txt']
    # Copy data from the task in the first stage to the current task's location
    t2.copy_input_data = ['$Pipeline_%s_Stage_%s_Task_%s/output.txt' % (p.name, s1.name, t1.name)]
    # Download the output of the current task to the current location
    t2.download_output_data = ['ccount.txt']

    # Add the Task to the Stage
    s2.add_tasks(t2)

    # Add Stage to the Pipeline
    p.add_stages(s2)

    # Create Application Manager
    appman = AppManager()

    # Assign the workflow as a set or list of Pipelines to the Application Manager
    appman.workflow = set([p])

    # Create a dictionary describe four mandatory keys:
    # resource, walltime, cpus and project
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
