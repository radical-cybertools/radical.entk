'''
We have in the past had a huge dictionary in memory that keeps track of all the task's remote
paths. The current understanding is that this is most definitely a necessary and useful feature to
continue with. This script tests its cost in terms of memory usage and performance (to retrieve 
stored values) as a function of the pipelines, stages, tasks.
'''

from pympler import asizeof
import string
import random
import time

def dir_generator(size):
    return ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(size))

if __name__ == '__main__':


    

    pipe = 1
    stage = 1
    task_list = [1,10,100,1000,10000,100000,1000000,10000000]

    f = open('bookkeeper_task_variation.csv','w')
    f.write('Pipelines, Stages, Tasks, Memory(MB), Time(secs)\n')

    for task in task_list:
    
        start = time.time()
        master_dict = dict()
        master_dict['pipe_%s'%pipe] = dict()
        master_dict['pipe_%s'%pipe]['stage_%s'%stage] = dict()

        for t in range(task):

            master_dict['pipe_%s'%pipe]['stage_%s'%stage]['task_%s'%t] = dir_generator(50)

        end = time.time()

        mem = float(asizeof.asizeof(master_dict))/(1024*1024)
        dur = end - start
        
        print 'Pipes: %s, Stages: %s, Tasks: %s'%(pipe, stage, task)
        print 'Size of dict: %s MB'%(mem)
        print 'Time taken: %s'%(dur)

        f.write('%s, %s, %s, %s, %s\n'%(pipe,stage,task,mem,dur))

    f.close()


    pipe = 1
    stage_list = [1,10,100,1000,10000,100000,1000000,10000000]
    task = 1

    f = open('bookkeeper_stage_variation.csv','w')
    f.write('Pipelines, Stages, Tasks, Memory(MB), Time(secs)\n')

    for stage in stage_list:
    
        start = time.time()
        master_dict = dict()
        master_dict['pipe_%s'%pipe] = dict()

        for s in range(stage):

            master_dict['pipe_%s'%pipe]['stage_%s'%stage] = dict()
            master_dict['pipe_%s'%pipe]['stage_%s'%stage]['task_%s'%task] = dir_generator(50)

        end = time.time()

        mem = float(asizeof.asizeof(master_dict))/(1024*1024)
        dur = end - start
        
        print 'Pipes: %s, Stages: %s, Tasks: %s'%(pipe, stage, task)
        print 'Size of dict: %s MB'%(mem)
        print 'Time taken: %s'%(dur)

        f.write('%s, %s, %s, %s, %s\n'%(pipe,stage,task,mem,dur))

    f.close()



    pipe_list = [1,10,100,1000,10000,100000,1000000]
    stage = 1
    task = 1

    f = open('bookkeeper_pipeline_variation.csv','w')
    f.write('Pipelines, Stages, Tasks, Memory(MB), Time(secs)\n')

    for pipe in pipe_list:
    
        start = time.time()
        master_dict = dict()

        for p in range(pipe):
            
            master_dict['pipe_%s'%p] = dict()
            master_dict['pipe_%s'%p]['stage_%s'%stage] = dict()
            master_dict['pipe_%s'%p]['stage_%s'%stage]['task_%s'%task] = dir_generator(50)

        end = time.time()

        mem = float(asizeof.asizeof(master_dict))/(1024*1024)
        dur = end - start
        
        print 'Pipes: %s, Stages: %s, Tasks: %s'%(pipe, stage, task)
        print 'Size of dict: %s MB'%(mem)
        print 'Time taken: %s'%(dur)

        f.write('%s, %s, %s, %s, %s\n'%(pipe,stage,task,mem,dur))

    f.close()

