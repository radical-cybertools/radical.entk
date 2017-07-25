from radical.entk import Pipeline, Stage, Task, AppManager, ResourceManager, Profiler
import os

if __name__ == '__main__':


    p = Pipeline()

    s = Stage()

    for ind in range(8):

        t = Task()
        t.executable = ['canalogs']
        t.pre_exec = ['module load gcc','module load boost','export PATH=/home1/04672/tg839717/git/CAnalogsV2/build:$PATH']
        t.cores = 4
        t.arguments =   [
                            '-L','-l',
                            '-d', '/home1/04672/tg839717/data/Wind_Luca_duplicated/',
                            '-o', './results_for_station.txt',
                            '--stations-ID',str(ind),
                            '--number-of-cores', '4',
                            '--test-ID-start', '0',
                            '--test-ID-end', '3649',
                            '--train-ID-start', '3650',
                            '--train-ID-end', '4014'
                        ]

        s.add_tasks(t)

    p.add_stages(s)

    res_dict = {

            'resource': 'xsede.stampede',
            'walltime': 30,
            'cores': 64,
            'project': 'TG-MCB090174',
            'queue': 'development',
            'schema': 'gsissh'

    }

    rman = ResourceManager(res_dict)


    appman = AppManager()
    appman.resource_manager = rman
    appman.assign_workflow(set([p]))
    appman.run()



    #p = Profiler()
    #print p.duration(objects= ['task.0000', 'task.0001'], states=['SCHEDULING', 'EXECUTED'])