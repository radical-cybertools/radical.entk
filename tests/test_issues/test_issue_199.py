from radical.entk import Pipeline, Stage, Task, AppManager
import os

# ------------------------------------------------------------------------------
# Set default verbosity

if not os.environ.get('RADICAL_ENTK_VERBOSE'):
    os.environ['RADICAL_ENTK_VERBOSE'] = 'INFO'

os.environ['RADICAL_PILOT_DBURL'] = 'mongodb://entk:entk123@ds227821.mlab.com:27821/entk_0_7_0_release'

hostname = os.environ.get('RMQ_HOSTNAME','localhost')
port = int(os.environ.get('RMQ_PORT',5672))

def generate_pipeline():
    
    # Create a Pipeline object
    p = Pipeline()

    # Create a Stage object 
    s1 = Stage()

    # Create a Task object which creates a file named 'output.txt' of size 1 MB
    t1 = Task()    
    t1.executable = ['/bin/sleep']   
    t1.arguments = ['300'] 

    # Add the Task to the Stage
    s1.add_tasks(t1)

    # Add Stage to the Pipeline
    p.add_stages(s1)

    return p   

def test_issue_199():


    # Create a dictionary describe four mandatory keys:
    # resource, walltime, cores and project
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

    p = generate_pipeline()
    
    # Assign the workflow as a set of Pipelines to the Application Manager
    appman.workflow = [p]

    # Run the Application Manager
    appman.run()