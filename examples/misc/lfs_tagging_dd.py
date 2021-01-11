
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
        p = Pipeline()
        p.name = 'p.%04d' % x

        s1 = Stage()
        s1.name = 's1'
        # The tasks from the first stage will execute at the first available node
        # they fit.
        t1 = Task()
        t1.name = 't1.%04d' % x
        t1.executable = 'hostname'
        t1.cpu_reqs = {'cpu_processes': 1,
                      'cpu_threads': 1,  # Set enough threads for this task to get a whole node
                      'cpu_process_type': None,
                      'cpu_thread_type': None}
        t1.lfs_per_process = 10
        s1.add_tasks(t1)

        p.add_stages(s1)

        s2 = Stage()
        s2.name = 's2'
        # Tasks from this stage will execute on the node the task from stage 1
        # it depends executed.
        t2 = Task()
        t2.name = 't2.%04d' % x
        t2.executable = 'hostname'
        t2.cpu_reqs = {'cpu_processes': 1,
                      'cpu_threads': 1,
                      'cpu_process_type': None,
                      'cpu_thread_type': None}
        t2.tag = t1.uid  # As a tag we use the ID of the first task this task depends upon.
        t2.lfs_per_process = 10
        s2.add_tasks(t2)


        p.add_stages(s2)

        s3 = Stage()
        s3.name = 's3'
        # Tasks from this stage will execute on the node the task from stage 1
        # it depends executed.
        t3 = Task()
        t3.name = 't3.%04d' % x
        t3.executable = 'hostname'
        t3.cpu_reqs = {'cpu_processes': 1,
                      'cpu_threads': 1,
                      'cpu_process_type': None,
                      'cpu_thread_type': None}
        t3.lfs_per_process = 10
        t3.tag = t1.uid   # As a tag we use the ID of the first task this task depends upon.
        s3.add_tasks(t3)

        p.add_stages(s3)
        pipelines.append(p)

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
