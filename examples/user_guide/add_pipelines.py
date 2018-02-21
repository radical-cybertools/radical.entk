from radical.entk import Pipeline, Stage, Task, AppManager, ResourceManager
import os

# ------------------------------------------------------------------------------
# Set default verbosity

if os.environ.get('RADICAL_ENTK_VERBOSE') == None:
    os.environ['RADICAL_ENTK_VERBOSE'] = 'INFO'


def generate_pipeline(name, stages):

    # Create a Pipeline object
    p = Pipeline()
    p.name = name


    for s_cnt in range(stages):

        # Create a Stage object 
        s = Stage()
        s.name = 'Stage %s'%s_cnt

        for t_cnt in range(5):

            # Create a Task object
            t = Task()
            t.name = 'my-task'        # Assign a name to the task (optional)
            t.executable = ['/bin/echo']   # Assign executable to the task   
            # Assign arguments for the task executable
            t.arguments = ['I am task %s in %s in %s'%(t_cnt, s_cnt, name)]  

            # Add the Task to the Stage
            s.add_tasks(t)

        # Add Stage to the Pipeline
        p.add_stages(s)

    return p



if __name__ == '__main__':

    p1 = generate_pipeline(name='Pipeline 1', stages=1)
    p2 = generate_pipeline(name='Pipeline 2', stages=2)


    # Create a dictionary describe four mandatory keys:
    # resource, walltime, cores and project
    # resource is 'local.localhost' to execute locally
    res_dict = {

            'resource': 'local.localhost',
            'walltime': 10,
            'cores': 1,
            'project': '',
    }

    # Create Resource Manager object with the above resource description
    rman = ResourceManager(res_dict)

    # Create Application Manager
    appman = AppManager()

    # Assign resource manager to the Application Manager
    appman.resource_manager = rman

    # Assign the workflow as a set of Pipelines to the Application Manager
    appman.assign_workflow(set([p1, p2]))

    # Run the Application Manager
    appman.run()
