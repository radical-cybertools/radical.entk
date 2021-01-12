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
    pipelines = list()
    for x in range(n):
        pipeline = Pipeline()
        pipeline.name = 'p.%04d' % x

        stage1 = Stage()
        stage1.name = 'stage1'
        # The tasks from the first stage will execute at the first available node
        # they fit.
        task1 = Task()
        task1.name = 'task1.%04d' % x
        task1.executable = 'hostname'
        task1.cpu_reqs = {'cpu_processes': 1,
                          'cpu_threads': 1,  # Set enough threads for this task to get a whole node
                          'cpu_process_type': None,
                          'cpu_thread_type': None}
        task1.lfs_per_process = 10
        stage1.add_tasks(task1)

        pipeline.add_stages(stage1)

        stage2 = Stage()
        stage2.name = 'stage2'
        # Tasks from this stage will execute on the node the task from stage 1
        # it depends executed.
        task2 = Task()
        task2.name = 'task2.%04d' % x
        task2.executable = 'hostname'
        task2.cpu_reqs = {'cpu_processes': 1,
                          'cpu_threads': 1,
                          'cpu_process_type': None,
                          'cpu_thread_type': None}
        task2.tag = task1.uid  # As a tag we use the ID of the first task this task depends upon.
        task2.lfs_per_process = 10
        stage2.add_tasks(task2)


        pipeline.add_stages(stage2)

        stage3 = Stage()
        stage3.name = 'stage3'
        # Tasks from this stage will execute on the node the task from stage 1
        # it depends executed.
        task3 = Task()
        task3.name = 'task3.%04d' % x
        task3.executable = 'hostname'
        task3.cpu_reqs = {'cpu_processes': 1,
                          'cpu_threads': 1,
                          'cpu_process_type': None,
                          'cpu_thread_type': None}
        task3.lfs_per_process = 10
        task3.tag = task1.uid   # As a tag we use the ID of the first task this task depends upon.
        stage3.add_tasks(task3)

        pipeline.add_stages(stage3)
        pipelines.append(pipeline)

    return pipelines


if __name__ == '__main__':

    # Request at least two nodes
    res_dict = {
                'resource'      : 'local.localhost',
                'walltime'      : 20,
                'cpus'          : 2,
            }

    appman = AppManager(hostname=hostname, port=port, username=username, password=password)
    appman.resource_desc = res_dict

    p = get_pipeline(n=2)  # Select n to be greater or equal to the number of nodes.
    appman.workflow = set(p)
    appman.run()
