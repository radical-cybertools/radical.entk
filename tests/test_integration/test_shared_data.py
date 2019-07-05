from radical.entk import Pipeline, Stage, Task, AppManager
import os
from glob import glob
import shutil

# ------------------------------------------------------------------------------
# Set default verbosity

if not os.environ.get('RADICAL_ENTK_VERBOSE'):
    os.environ['RADICAL_ENTK_VERBOSE'] = 'INFO'

hostname = os.environ.get('RMQ_HOSTNAME','localhost')
port = int(os.environ.get('RMQ_PORT',5672))
cur_dir = os.path.dirname(os.path.abspath(__file__))
MLAB = os.environ.get('RADICAL_PILOT_DBURL')


def generate_pipeline():

    # Create a Pipeline object
    p = Pipeline()

    # Create a Stage object
    s1 = Stage()

    # Create a Task object which creates a file named 'output.txt' of size 1 MB
    t1 = Task()
    t1.executable = 'cat'
    t1.arguments = ['file1.txt','file2.txt']
    t1.stdout = 'output.txt'
    t1.copy_input_data = ['$SHARED/file1.txt', '$SHARED/file2.txt']
    t1.download_output_data = ['output.txt > %s/output.txt' %cur_dir]

    # Add the Task to the Stage
    s1.add_tasks(t1)

    # Add Stage to the Pipeline
    p.add_stages(s1)

    return p

def test_shared_data():

    for f in glob('%s/file*.txt' %cur_dir):
        os.remove(f)

    os.system('echo "Hello" > %s/file1.txt' %cur_dir)
    os.system('echo "World" > %s/file2.txt' %cur_dir)


    # Create a dictionary describe four mandatory keys:
    # resource, walltime, cpus and project
    # resource is 'local.localhost' to execute locally
    res_dict = {

            'resource': 'local.localhost',
            'walltime': 1,
            'cpus': 1
    }

    os.environ['RADICAL_PILOT_DBURL'] = MLAB

    # Create Application Manager
    appman = AppManager(hostname=hostname, port=port)

    # Assign resource manager to the Application Manager
    appman.resource_desc = res_dict
    appman.shared_data = ['%s/file1.txt' %cur_dir, '%s/file2.txt' %cur_dir]

    p = generate_pipeline()

    # Assign the workflow as a set of Pipelines to the Application Manager
    appman.workflow = [p]

    # Run the Application Manager
    appman.run()

    with open('%s/output.txt' %cur_dir, 'r') as fp:
        assert [d.strip() for d in fp.readlines()] == ['Hello', 'World']

    os.remove('%s/file1.txt' %cur_dir)
    os.remove('%s/file2.txt' %cur_dir)
    os.remove('%s/output.txt' %cur_dir)
