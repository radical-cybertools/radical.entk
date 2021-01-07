#!/usr/bin/env python

from radical.entk import Pipeline, Stage, Task, AppManager
import os

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

# Each task in this example prints the hostname of the node it executed. Tagged
# tasks should print the same hostname as the respective task in the first stage
# of the pipeline the following function returns.

def get_pipeline(n=2):

    # We create a pipeline which has 3 stages. The tasks from the second and
    # and third stage will execute at the same node as the respective tasks from
    # the first stage. 

    p = Pipeline()
    p.name = 'p'

    s1 = Stage()
    s1.name = 's1'
    for x in range(n):
        # The tasks from the first stage will execute at the first available node
        # they fit.
        t = Task()
        t.name = 't1.%04d' % x
        t.executable = 'hostname'
        t.cpu_reqs = {'cpu_processes': 1,
                      'cpu_threads': ,  # Set enough threads for this task to get a whole node
                      'cpu_process_type': None, 
                      'cpu_thread_type': None}
        t.lfs_per_process = 1024

        s1.add_tasks(t)

    p.add_stages(s1)

    s2 = Stage()
    s2.name = 's2'
    for x in range(2 * n):
        # Tasks from this stage will execute on the node the task from stage 1
        # it depends executed.
        t = Task()
        t.name = 't2.%04d' % x
        t.executable = 'hostname'
        t.cpu_reqs = {'cpu_processes': 1, 
                      'cpu_threads': 1,
                      'cpu_process_type': None,
                      'cpu_thread_type': None}
        t.lfs_per_process = 1024
        t.tag = 't1.%04d' % (x % 4)  # As a tag we use the name of the task this task depends upon.

        s2.add_tasks(t)


    p.add_stages(s2)

    s3 = Stage()
    s3.name = 's3'
    for x in range(n):
        # Tasks from this stage will execute on the node the task from stage 1
        # it depends executed.
        t = Task()
        t.name = 't3.%04d' % x
        t.executable = 'hostname'
        t.cpu_reqs = {'cpu_processes': 1, 
                      'cpu_threads': 1,
                      'cpu_process_type': None,
                      'cpu_thread_type': None}
        t.lfs_per_process = 1024
        t.tag = 't1.%04d' % x   # As a tag we use the name of the task this task depends upon.
        s3.add_tasks(t)


    p.add_stages(s3)

    return p



if __name__ == '__main__':

    # Request at least two nodes 
    res_dict = {
                'resource'      : '',
                'walltime'      : ,
                'cpus'          : ,
                'project'       : '',
                'queue'         : ''
            }

    appman = AppManager(hostname=hostname, port=port, username=username, password=password)
    appman.resource_desc = res_dict

    p = get_pipeline(n=2)  # Select n to be greater or equal to the number of nodes.
    appman.workflow = set([p])
    appman.run()
