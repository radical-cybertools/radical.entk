from radical.entk import Pipeline, Stage, Task, AppManager, states
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
port = int(os.environ.get('RMQ_PORT', 5672))


def generate_pipeline():

    # Create a Pipeline object
    p = Pipeline()
    p.name = 'p1'

    # Create a Stage object
    s1 = Stage()
    s1.name = 's1'

    # Create 4K tasks to ensure we don't hit any RMQ connection drops
    for _ in range(4096):
        t1 = Task()
        t1.executable = '/bin/echo'
        t1.arguments = ['"Hello World"']

        # Add the Task to the Stage
        s1.add_tasks(t1)

    # Add Stage to the Pipeline
    p.add_stages(s1)

    return p


def test_issue_270():

    # Create Application Manager
    appman = AppManager(hostname=hostname, port=port)

    # Create a dictionary describe four mandatory keys:
    # resource, walltime, and cpus
    # resource is 'local.localhost' to execute locally
    res_dict = {

        'resource': 'local.localhost',
        'walltime': 30,
        'cpus': 4
    }

    # Assign resource request description to the Application Manager
    appman.resource_desc = res_dict

    # Assign the workflow as a set or list of Pipelines to the Application Manager
    # Note: The list order is not guaranteed to be preserved
    p = generate_pipeline()
    appman.workflow = set([p])

    # Run the Application Manager
    appman.run()
