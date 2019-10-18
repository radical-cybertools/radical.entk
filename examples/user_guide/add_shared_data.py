#!/usr/bin/env python

from radical.entk import Pipeline, Stage, Task, AppManager
import os
from glob import glob
import shutil

# ------------------------------------------------------------------------------
# Set default verbosity

if os.environ.get('RADICAL_ENTK_VERBOSE') == None:
    os.environ['RADICAL_ENTK_REPORT'] = 'True'

hostname = os.environ.get('RMQ_HOSTNAME','localhost')
port = int(os.environ.get('RMQ_PORT',5672))
cur_dir = os.path.dirname(os.path.abspath(__file__))

def generate_pipeline():

    # Create a Pipeline object
    p = Pipeline()

    # Create a Stage object
    s1 = Stage()

    # Create a Task object which creates a file named 'output.txt' of size 1 MB
    for x in range(10):
        t1 = Task()
        t1.executable = 'cat'
        t1.arguments = ['file1.txt','file2.txt','>','output.txt']
        t1.copy_input_data = ['$SHARED/file1.txt', '$SHARED/file2.txt']
        t1.download_output_data = ['output.txt > %s/output_%s.txt' %(cur_dir,x+1)]

        # Add the Task to the Stage
        s1.add_tasks(t1)

    # Add Stage to the Pipeline
    p.add_stages(s1)

    return p

if __name__ == '__main__':

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

    os.environ['RADICAL_PILOT_DBURL'] = 'mongodb://entk:entk123@ds227821.mlab.com:27821/entk_0_7_0_release'

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

    for x in range(10):
        with open('%s/output_%s.txt' %(cur_dir,x+1), 'r') as fp:
            print('Output %s: '%(x+1), fp.readlines())
        os.remove('%s/output_%s.txt' %(cur_dir,x+1))


    os.remove('%s/file1.txt' %cur_dir)
    os.remove('%s/file2.txt' %cur_dir)
