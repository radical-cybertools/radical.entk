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
    t1.executable = ['mv']
    t1.arguments = ['temp','/tmp/']
    t1.upload_input_data = ['%s/temp'%cur_dir]

    # Add the Task to the Stage
    s1.add_tasks(t1)

    # Add Stage to the Pipeline
    p.add_stages(s1)

    return p

def test_issue_236():

    '''
    Create folder temp to transfer as input to task:
    .
    ./temp
    ./temp/dir1
    ./temp/dir1/file2.txt
    ./temp/file1.txt
    '''

    os.makedirs('%s/temp' %cur_dir)
    os.makedirs('%s/temp/dir1' %cur_dir)
    os.system('echo "Hello world" > %s/temp/file1.txt' %cur_dir)
    os.system('echo "Hello world" > %s/temp/dir1/file2.txt' %cur_dir)


    # Create a dictionary describe four mandatory keys:
    # resource, walltime, cpus and project
    # resource is 'local.localhost' to execute locally
    res_dict = {

            'resource': 'local.localhost',
            'walltime': 10,
            'cpus': 1
    }

    os.environ['RADICAL_PILOT_DBURL'] = MLAB

    # Create Application Manager
    appman = AppManager(hostname=hostname, port=port)

    # Assign resource manager to the Application Manager
    appman.resource_desc = res_dict

    p = generate_pipeline()

    # Assign the workflow as a set of Pipelines to the Application Manager
    appman.workflow = [p]

    # Run the Application Manager
    appman.run()

    # Assert folder movement
    assert len(glob('/tmp/temp*')) >=1
    assert len(glob('/tmp/temp/dir*')) ==1
    assert len(glob('/tmp/temp/*.txt')) ==1
    assert len(glob('/tmp/temp/dir1/*.txt')) ==1

    # Cleanup
    shutil.rmtree('%s/temp' %cur_dir)
    shutil.rmtree('/tmp/temp')
