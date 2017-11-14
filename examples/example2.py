from radical.entk import Pipeline, Stage, Task, AppManager, ResourceManager
import os

if __name__ == '__main__':

    p = Pipeline()

    s1 = Stage()
    t1 = Task()
    t1.name = 'mkfile'
    t1.executable = ['dd']
    t1.arguments = ['if=/dev/zero', 'of=test.txt', 'bs=1M', 'count=1']
    s1.tasks = t1
    p.add_stages(s1)


    s2 = Stage()
    t2 = Task()
    t2.name = 'cat'
    t2.executable = ['/bin/cat']
    t2.arguments = ['test.txt']
    t2.copy_input_data = ['$Pipeline_%s_Stage_%s_Task_%s/test.txt'%(
                                                                p.uid,
                                                                s1.uid,
                                                                t1.uid
                                )]
    s2.tasks = t2
    p.add_stages(s2)

    res_dict = {

            'resource': 'local.localhost',
            'walltime': 15,
            'cores': 2,
            'project': ''

    }

    rman = ResourceManager(res_dict)


    appman = AppManager()
    appman.resource_manager = rman
    appman.assign_workflow(set([p]))
    appman.run()

   