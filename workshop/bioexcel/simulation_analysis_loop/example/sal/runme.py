from radical.entk import Pipeline, Stage, Task, AppManager
import os

'''
This example represents the simulation analysis loop pattern created with the
EnTK API. The tasks are simple unix commands but the API and the usage of the
PST model should give the users an idea on how to compose their applications.

This application is a workflow composed of a pipeline and 4 stages. Each task of
the first and third stage executes a /bin/echo command and writes the output to
a file. The second and fourth stages have one task each which concatenate the
output of all tasks from the first and second stages respectively.
'''

# ------------------------------------------------------------------------------
# USER VARIABLES
# ------------------------------------------------------------------------------
num_sims = 16
num_iters = 2
# ------------------------------------------------------------------------------


# ------------------------------------------------------------------------------
# Set default verbosity
if os.environ.get('RADICAL_ENTK_VERBOSE') == None:
    os.environ['RADICAL_ENTK_REPORT'] = 'True'

# RabbitMQ configuration
# NOTE: No need to change/set any variables if you installed RabbitMQ as a
# system process. If you are running RabbitMQ under a Docker container or a VM,
# set "RMQ_HOSTNAME" and "RMQ_PORT" in the session where you are running this
# script.
hostname = os.environ.get('RMQ_HOSTNAME', 'localhost')
port = int(os.environ.get('RMQ_PORT', 5672))
# ------------------------------------------------------------------------------

def generate_pipeline():

    # Create a Pipeline object
    p = Pipeline()
    p.name = 'p1'

    for i in range(1,num_iters+1):

        # Create a Stage object to hold tasks that count characters
        sim = Stage()
        sim.name = 'sim%s'%i
        sim_task_uids = []

        for cnt in range(1, num_sims+1):

            # Create a Task object
            t = Task()
            t.name = 't%s' % (cnt + 1)
            t.executable = ['/bin/echo']
            t.arguments = [ 'This is simulation %s from iteration %s'%(cnt, i),
                            '>',
                            'output_sim_%s.txt'%cnt]

            # Add the Task to the Stage
            sim.add_tasks(t)
            sim_task_uids.append(t.name)

        # Add Stage to the Pipeline
        p.add_stages(sim)

        # Create another Stage object to hold checksum tasks
        ana = Stage()

        # Create a Task object
        t = Task()
        t.executable = ['/bin/cat']
        t.arguments = ['./output_sim_*','>','output_ana_iter_%s.txt'%i]
        t.copy_input_data = list()

        for cnt in range(1, num_sims+1):
            t.copy_input_data.append('$Pipeline_%s_Stage_%s_Task_%s/output_sim_%s.txt' % (p.name,
                                                                                          sim.name,
                                                                                          sim_task_uids[cnt-1],
                                                                                          cnt))

        t.download_output_data = ['output_ana_iter_%s.txt' % i]

        # Add the Task to the Stage
        ana.add_tasks(t)

        # Add Stage to the Pipeline
        p.add_stages(ana)

    return p

if __name__ == '__main__':

    # Create Application Manager
    appman = AppManager(hostname=hostname, port=port)

    # Create a dictionary with four mandatory keys:
    # resource, walltime, and cpus.
    # NOTE: Resource is 'local.localhost' to execute locally
    res_dict = {

        'resource': 'local.localhost',
        'walltime': 10,
        'cpus': 1
    }

    # Assign the resource request description to the Application Manager
    appman.resource_desc = res_dict

    # Assign the workflow expressed as a set or list of Pipelines to the
    # Application Manager.
    # NOTE: The list order is not guaranteed to be preserved
    appman.workflow = set([generate_pipeline()])

    # Run the Application Manager
    appman.run()
