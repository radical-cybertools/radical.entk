from radical.entk import Pipeline, Stage, Task, AppManager
import os

'''
This example represents the ensemble of pipelines pattern created with the
EnTK API. The tasks are simple unix commands but the API and the usage of the
PST model should give the users an idea on how to compose their applications.

This application is a workflow composed of multiple pipelines and 2 stages per
pipeline. Each task of the first stage executes a /bin/echo command and writes
the output to a file. The second stage has one task which prints the output of
the task to another file.
'''

# ------------------------------------------------------------------------------
# USER VARIABLES
# ------------------------------------------------------------------------------
num_pipelines = 5
# ------------------------------------------------------------------------------


# ------------------------------------------------------------------------------
# Set default verbosity
if os.environ.get('RADICAL_ENTK_VERBOSE') == None:
    os.environ['RADICAL_ENTK_REPORT'] = 'True'

# RabbitMQ configuration
# No need to change/set any variables if you installed RabbitMQ has a system
# process. If you are running RabbitMQ under a docker container or another
# VM, set "RMQ_HOSTNAME" and "RMQ_PORT" in the session where you are running
# this script.
hostname = os.environ.get('RMQ_HOSTNAME', 'localhost')
port = int(os.environ.get('RMQ_PORT', 5672))
# ------------------------------------------------------------------------------

def generate_pipeline(i):

    # Create a Pipeline object
    p = Pipeline()
    p.name = 'p1'

    # Create another Stage object to hold character count tasks
    sim = Stage()
    sim.name = 'sim%s'%i
    sim_task_uids = None

    # Create a Task object
    t = Task()
    t.name = 't%s' % (i + 1)
    t.executable = ['/bin/echo']
    t.arguments = [ 'This is simulation %s from pipeline %s'%(i, i),
                    '>',
                    'output_sim_%s.txt'%i]

    # Add the Task to the Stage
    sim.add_tasks(t)
    sim_task_uids = t.name

    # Add Stage to the Pipeline
    p.add_stages(sim)

    # Create another Stage object to hold checksum tasks
    ana = Stage()

    # Create a Task object
    t = Task()
    t.executable = ['/bin/cat']
    t.arguments = ['./output_sim_*','>','output_ana_pipe_%s.txt'%i]
    t.copy_input_data = list()

    t.copy_input_data.append('$Pipeline_%s_Stage_%s_Task_%s/output_sim_%s.txt' % (  p.name,
                                                                                    sim.name,
                                                                                    sim_task_uids,
                                                                                    i))

    t.download_output_data = ['output_ana_pipe_%s.txt'%i]

    # Add the Task to the Stage
    ana.add_tasks(t)

    # Add Stage to the Pipeline
    p.add_stages(ana)

    return p

if __name__ == '__main__':

    # Create Application Manager
    appman = AppManager(hostname=hostname, port=port)

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
    pipelines = set()
    for x in range(num_pipelines):
        pipelines.add(generate_pipeline(x))
    appman.workflow = pipelines

    # Run the Application Manager
    appman.run()
