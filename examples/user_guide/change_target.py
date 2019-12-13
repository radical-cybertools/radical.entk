#!/usr/bin/env python

from radical.entk import Pipeline, Stage, Task, AppManager
import os

# ------------------------------------------------------------------------------
# Set default verbosity
if os.environ.get('RADICAL_ENTK_VERBOSE') == None:
    os.environ['RADICAL_ENTK_REPORT'] = 'True'


# Description of how the RabbitMQ process is accessible
# No need to change/set any variables if you installed RabbitMQ has a system
# process. If you are running RabbitMQ under a docker container or another
# VM, set "RMQ_HOSTNAME" and "RMQ_PORT" in the session where you are running
# this script.
hostname = os.environ.get('RMQ_HOSTNAME', 'localhost')
port = os.environ.get('RMQ_PORT', 5672)

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
    s2.name = 'Stage 2'

    # Create a Task object
    t2 = Task()
    t2.executable = '/bin/bash'
    t2.arguments = ['-l', '-c', 'grep -o . output.txt | sort | uniq -c > ccount.txt']
    # Copy data from the task in the first stage to the current task's location
    t2.copy_input_data = ['$Pipline_%s_Stage_%s_Task_%s/output.txt' % (p.uid, s1.uid, t1.uid)]
    # Download the output of the current task to the current location
    t2.download_output_data = ['ccount.txt']

    # Add the Task to the Stage
    s2.add_tasks(t2)

    # Add Stage to the Pipeline
    p.add_stages(s2)

   # Create Application Manager
    appman = AppManager(hostname=hostname, port=port)

    # Assign the workflow as a set or list of Pipelines to the Application Manager
    appman.workflow = set([p])

    # Create a dictionary to describe our resource request for XSEDE Stampede
    res_dict = {

        'resource': 'xsede.comet',
        'walltime': 10,
        'cpus': 16,
        'project': 'unc100',
        'schema': 'gsissh'

    }

    # Assign resource request description to the Application Manager
    appman.resource_desc = res_dict

    # Run the Application Manager
    appman.run()
