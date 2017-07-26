from radical.entk import Pipeline, Stage, Task, AppManager, ResourceManager, Profiler
import os
from glob import glob

'''
EnTK 0.6 script - Seisflow application

'''


if __name__ == '__main__':


    # Our application currently will contain only one pipeline
    p = Pipeline()

    # Currently, we have two stages in each pipeline - meshfem, specfem
    mesh_stage = Stage()
    spec_stage = Stage()

    # List catch all the task uids to be used in the Profiler 
    mesh_task_uids = list()
    spec_task_uids = list()


    # Create task to be added to the mesh stage
    t = Task()
    # Executable for the task
    t.executable = ['./bin/xmeshfem3D']
    # Environment to be setup before execution
    t.pre_exec = [  'cp /lustre/atlas/scratch/vivekb/bip149/specfem3d_globe/DATA . -r',
                    'cp /lustre/atlas/scratch/vivekb/bip149/specfem3d_globe/bin . -r',
                    'mkdir OUTPUT_FILES',
                    'mkdir DATABASES_MPI']
    # Operation to perform after execution
    t.post_exec = ['tar cf opdata.tar DATA bin OUTPUT_FILES DATABASES_MPI']
    # Move to data to a shared location
    t.copy_output_data = ['opdata.tar > $SHARED/opdata.tar']
    # Arguments to the executable
    t.arguments = []
    # Number of cores to be used by this executable
    t.cores = 4
    # Enable MPI launch of this task executable
    t.mpi = True

    # Add task to first stage
    mesh_stage.add_tasks(t)
    mesh_task_uids = ['.'.join(t.uid.split('.')[2:])] 


    for ind in range(16):

        # Create task to be added to the mesh stage
        t = Task()
        # Executable for the task
        t.executable = ['./bin/xspecfem3D']
        # Operation to perform before execution
        t.pre_exec = [  'tar xf opdata.tar']
        # Move to data to a shared location
        t.copy_input_data = ['$SHARED/opdata.tar > opdata.tar']
        # Arguments to the executable
        t.arguments = []
        # Number of cores to be used by this executable
        t.cores = 4
        # Enable MPI launch of this task executable
        t.mpi = True

        spec_stage.add_tasks(t)
        spec_task_uids.append('.'.join(t.uid.split('.')[2:]))


    # Add stages to pipeline in order
    p.add_stages(mesh_stage)
    p.add_stages(spec_stage)

    # Create a dictionary to describe our resource request
    res_dict = {

            'resource': 'ornl.titan_aprun',
            'walltime': 60,
            'cores': 64,
            'project': 'BIP149',
            'queue': 'debug',
            'schema': 'local'

    }

    try:

        # Create a Resource Manager using the above description
        rman = ResourceManager(res_dict)

        # Create an Application Manager for our application
        appman = AppManager()

        # Assign the resource manager to be used by the application manager
        appman.resource_manager = rman

        # Assign the workflow to be executed by the application manager
        appman.assign_workflow(set([p]))

        # Run the application manager -- blocking call
        appman.run()


        # Once completed, use EnTK profiler to get the time between 'SCHEDULING' and 'EXECUTING' states for all
        # tasks. This is the execution time of the tasks as seen by EnTK.
        #p = Profiler()
        #print 'Task uids: ', task_uids
        #print 'Total execution time for all tasks: ', p.duration(objects = task_uids, states=['SCHEDULING', 'EXECUTED'])

    except Exception, ex:

        print 'Execution failed, error: %s'%ex

    finally:

        profs = glob('./*.prof')
        for f in profs:
            os.remove(f)