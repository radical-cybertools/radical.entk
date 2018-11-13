from radical.entk import Pipeline, Stage, Task, AppManager
import os
import sys
from time import sleep

# ------------------------------------------------------------------------------
# Set default verbosity

if not os.environ.get('RADICAL_ENTK_VERBOSE'):
    os.environ['RADICAL_ENTK_VERBOSE'] = 'INFO'

hostname = os.environ.get('RMQ_HOSTNAME', 'localhost')
port = int(os.environ.get('RMQ_PORT', 5672))
cur_dir = os.path.dirname(os.path.abspath(__file__))
MLAB = os.environ.get('RADICAL_PILOT_DBURL')


def generate_pipeline():

    def func_condition():

        p.suspend()
        print 'Suspending pipeline %s for 10 seconds' %p.uid
        sleep(10)
        return True

    def func_on_true():

        print 'Resuming pipeline %s' %p.uid
        p.resume()

    def func_on_false():
        pass

    # Create a Pipeline object
    p = Pipeline()

    # Create a Stage object
    s1 = Stage()

    for i in range(10):

        t1 = Task()
        t1.executable = ['sleep']
        t1.arguments = ['30']

        # Add the Task to the Stage
        s1.add_tasks(t1)

    # Add post-exec to the Stage
    s1.post_exec = {
        'condition': func_condition,
        'on_true': func_on_true,
        'on_false': func_on_false
    }

    # Add Stage to the Pipeline
    p.add_stages(s1)

    return p


if __name__ == '__main__':

    # Create a dictionary describe four mandatory keys:
    # resource, walltime, cores and project
    # resource is 'local.localhost' to execute locally
    res_dict = {

        'resource': 'local.localhost',
        'walltime': 15,
        'cpus': 2,
    }

    # Create Application Manager
    appman = AppManager(hostname=hostname, port=port)
    appman.resource_desc = res_dict

    p = generate_pipeline()

    # Assign the workflow as a set of Pipelines to the Application Manager
    appman.workflow = [p]

    # Run the Application Manager
    appman.run()
