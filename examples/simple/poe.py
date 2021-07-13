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
port = int(os.environ.get('RMQ_PORT', 5672))
username = os.environ.get('RMQ_USERNAME')
password = os.environ.get('RMQ_PASSWORD')


# ------------------------------------------------------------------------------
#
def generate_pipeline():

    # Create a Pipeline object
    p = Pipeline()
    p.name = 'p1'

    # Create a Stage object
    s1 = Stage()
    s1.name = 's1'

    # Create a Task object which creates a file named 'output.txt' of size 1 MB
    t1 = Task()
    t1.name = 't1'
    t1.executable = '/bin/bash'
    t1.arguments = ['-l', '-c', 'base64 /dev/urandom | head -c 1000000 > output.txt']

    # Add the Task to the Stage
    s1.add_tasks(t1)

    # Add Stage to the Pipeline
    p.add_stages(s1)

    # Create another Stage object to hold character count tasks
    s2 = Stage()
    s2.name = 's2'
    s2_task_uids = []

    for cnt in range(30):

        # Create a Task object
        t2 = Task()
        t2.name = 't%s' % (cnt + 1)
        t2.executable = '/bin/bash'
        t2.arguments = ['-l', '-c', 'grep -o . output.txt | sort | uniq -c > ccount.txt']
        # Copy data from the task in the first stage to the current task's location
        t2.copy_input_data = ['$Pipeline_%s_Stage_%s_Task_%s/output.txt' %
                (p.uid, s1.uid, t1.uid)]

        # Add the Task to the Stage
        s2.add_tasks(t2)
        s2_task_uids.append(t2.uid)

    # Add Stage to the Pipeline
    p.add_stages(s2)

    # Create another Stage object to hold checksum tasks
    s3 = Stage()
    s3.name = 's3'

    for cnt in range(30):

        # Create a Task object
        t3 = Task()
        t3.name = 't%s' % (cnt + 1)
        t3.executable = '/bin/bash'
        t3.arguments = ['-l', '-c', 'sha1sum ccount.txt > chksum.txt']
        # Copy data from the task in the first stage to the current task's location
        t3.copy_input_data = ['$Pipeline_%s_Stage_%s_Task_%s/ccount.txt' %
                (p.uid, s2.uid, s2_task_uids[cnt])]
        # Download the output of the current task to the current location
        t3.download_output_data = ['chksum.txt > chksum_%s.txt' % cnt]

        # Add the Task to the Stage
        s3.add_tasks(t3)

    # Add Stage to the Pipeline
    p.add_stages(s3)

    return p


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    # Create Application Manager
    appman = AppManager(hostname=hostname, port=port, username=username,
            password=password)

    # Create a dictionary describe four mandatory keys:
    # resource, walltime, and cpus
    # resource is 'local.localhost' to execute locally
    res_dict = {
            'resource': 'local.localhost',
            'walltime': 10,
            'cpus':2
    }

    # Assign resource request description to the Application Manager
    appman.resource_desc = res_dict

    # Assign the workflow as a set or list of Pipelines to the Application Manager
    # Note: The list order is not guaranteed to be preserved
    appman.workflow = set([generate_pipeline()])

    # Run the Application Manager
    appman.run()


# ------------------------------------------------------------------------------

