from radical.entk import Pipeline, Stage, Task, AppManager, ResourceManager
import os


def create_single_task():

    t1 = Task()
    t1.name = 'simulation'
    t1.executable = ['/bin/echo']
    t1.arguments = ['hello']
    t1.copy_input_data = []
    t1.copy_output_data = []

    return t1

if __name__ == '__main__':

    p1 = Pipeline()
    p1.name = 'p1'
    p2 = Pipeline()
    p2.name = 'p2'

    stages=1

    for cnt in range(stages):
        s = Stage()
        s.name = 's_%s'%cnt
        s.tasks = create_single_task()
        #s.add_tasks(create_single_task())

        p1.add_stages(s)

    
    for cnt in range(stages-1):
        s = Stage()
        s.name = 's-%s'%cnt
        s.tasks = create_single_task()
        s.add_tasks(create_single_task())

        p2.add_stages(s)
    

    res_dict = {

            'resource': 'local.localhost',
            'walltime': 15,
            'cores': 2,
            'project': ''

    }

    rman = ResourceManager(res_dict)


    appman = AppManager()
    appman.resource_manager = rman
    appman.assign_workflow(set([p1]))
    appman.run()

