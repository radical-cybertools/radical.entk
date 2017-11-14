from radical.entk import Pipeline, Stage, Task, AppManager, ResourceManager
import os

# ------------------------------------------------------------------------------
# Set default verbosity

if os.environ.get('RADICAL_ENTK_VERBOSE') == None:
    os.environ['RADICAL_ENTK_VERBOSE'] = 'INFO'


def generate_state():

    s = Stage()

    t = Task()
    t.executable = ['/bin/sleep']
    t.arguments = ['0']

    s.add_tasks(t)

    return s

def generate_pipeline():

    # Create a Pipeline object
    p = Pipeline()
 
    # Loop to generate 7 stages
    for cnt in range(7):        
        p.add_stages(generate_state())

    return p


if __name__ == '__main__':

    pipelines = []

    num_pipelines=2
    
    for cnt in range(num_pipelines):
        pipelines.append(generate_pipeline())


    # Create a dictionary describe four mandatory keys:
    # resource, walltime, cores and project
    # resource is 'local.localhost' to execute locally
    res_dict = {

            'resource': 'xsede.supermic',
            'walltime': 30,
            'cores': 20,
            'project': 'TG-MCB090174',
            #'queue': 'debug',
            #'access_schema': 'gsissh'
    }

    # Create Resource Manager object with the above resource description
    rman = ResourceManager(res_dict)

    # Create Application Manager
    appman = AppManager()

    # Assign resource manager to the Application Manager
    appman.resource_manager = rman

    # Assign the workflow as a set of Pipelines to the Application Manager
    appman.assign_workflow(set(pipelines))

    # Run the Application Manager
    appman.run()
