#!/usr/bin/env python

from radical.entk import Pipeline, Stage, Task, AppManager
import os

if os.environ.get('RADICAL_ENTK_VERBOSE') is None:
    os.environ['RADICAL_ENTK_REPORT'] = 'True'

# No need to change/set any variables if you installed RabbitMQ has a system
# process. If you are running RabbitMQ in a Docker container or on a dedicated
# virtual machine, set the variables "RMQ_HOSTNAME" and "RMQ_PORT" in the shell
# environment in which you are running this script.
hostname = os.environ.get('RMQ_HOSTNAME', 'localhost')
port = int(os.environ.get('RMQ_PORT', '5672'))
username = os.environ.get('RMQ_USERNAME')
password = os.environ.get('RMQ_PASSWORD')


# Each task of this example prints the hostname of the node on which it is
# executed. Tagged tasks should print the same hostname.
def get_pipeline(n=2):
    '''
    We create a pipeline with three stages, each with 1 task. The tasks of the
    second and third stages are tagged to execute on the same compute node on
    which the first stage's task executed.
    '''

    pipelines = list()
    for x in range(n):
        pipeline = Pipeline()
        pipeline.name = 'p.%04d' % x

        stage1 = Stage()
        stage1.name = 'stage1'
        # task1 of stage1 will execute on the first available and suitable node.
        task1 = Task()
        task1.name = 'task1.%04d' % x
        task1.executable = 'hostname'
        # Set enough threads for task1 to get a whole compute node
        task1.cpu_reqs = {'cpu_processes': 1,
                          'cpu_threads': 1,
                          'cpu_process_type': None,
                          'cpu_thread_type': None}
        task1.lfs_per_process = 10
        stage1.add_tasks(task1)

        pipeline.add_stages(stage1)

        stage2 = Stage()
        stage2.name = 'stage2'

        task2 = Task()
        task2.name = 'task2.%04d' % x
        task2.executable = 'hostname'
        task2.cpu_reqs = {'cpu_processes': 1,
                          'cpu_threads': 1,
                          'cpu_process_type': None,
                          'cpu_thread_type': None}
        # We use the ID of task1 as the tag of task2. In this way, task2 will
        # execute on the same node on which task1 executed.
        task2.tags = {'colocate': task1.uid}
        task2.lfs_per_process = 10
        stage2.add_tasks(task2)


        pipeline.add_stages(stage2)

        stage3 = Stage()
        stage3.name = 'stage3'

        task3 = Task()
        task3.name = 'task3.%04d' % x
        task3.executable = 'hostname'
        task3.cpu_reqs = {'cpu_processes': 1,
                          'cpu_threads': 1,
                          'cpu_process_type': None,
                          'cpu_thread_type': None}
        task3.lfs_per_process = 10
        # We use the ID of task1 as the tag of task3. In this way, task3 will
        # execute on the same node on which task1 and task2 executed.
        task3.tag = {'colocate': task1.uid}
        stage3.add_tasks(task3)

        pipeline.add_stages(stage3)
        pipelines.append(pipeline)

    return pipelines


if __name__ == '__main__':

    # Request at least two compute nodes
    res_dict = {
                'resource'      : 'local.localhost',
                'walltime'      : 20,
                'cpus'          : 2,
            }

    appman = AppManager(hostname=hostname, port=port, username=username, password=password)
    appman.resource_desc = res_dict

    # Select n to be >= to the number of available compute nodes.
    p = get_pipeline(n=2)
    appman.workflow = set(p)
    appman.run()
