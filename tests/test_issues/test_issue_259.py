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
port = int(os.environ.get('RMQ_PORT', 5672))


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
    t1.executable = ['/bin/echo']
    t1.arguments = ['"Hello World"']
    t1.stdout = 'temp.txt'

    # Add the Task to the Stage
    s1.add_tasks(t1)

    # Add Stage to the Pipeline
    p.add_stages(s1)

    # Create a Stage object
    s2 = Stage()
    s2.name = 's2'

    # Create a Task object which creates a file named 'output.txt' of size 1 MB
    t2 = Task()
    t2.name = 't2'
    t2.executable = ['/bin/cat']
    t2.arguments = ['$Pipeline_%s_Stage_%s_Task_%s/temp.txt'%(p.name, s1.name, t1.name)]
    t2.stdout = 'output.txt'
    t2.download_output_data = ['output.txt']

    # Add the Task to the Stage
    s2.add_tasks(t2)

    # Add Stage to the Pipeline
    p.add_stages(s2)

    return p

def test_issue_259():

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
    appman.workflow = set([generate_pipeline()])

    # Run the Application Manager
    appman.run()

    # assert
    with open('output.txt','r') as f:
        assert '"Hello World"' == f.readlines()[0].strip()
