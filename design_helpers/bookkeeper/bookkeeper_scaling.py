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


    '''
    
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
        path = dir_generator(50)

        for t in range(task):

            master_dict['pipe_%s'%pipe]['stage_%s'%stage]['task_%s'%t] = path

        end = time.time()

        mem = float(asizeof.asizeof(master_dict))/(1024*1024)
        dur = end - start
        
        print 'Pipes: %s, Stages: %s, Tasks: %s'%(pipe, stage, task)
        print 'Size of dict: %s MB'%(mem)
        print 'Time taken: %s'%(dur)

        f.write('%s, %s, %s, %s, %s\n'%(pipe,stage,task,mem,dur))

    f.close()


    pipe = 1
    stage_list = [1,10,100,1000,10000,100000,1000000]
    task = 1

    f = open('bookkeeper_stage_variation.csv','w')
    f.write('Pipelines, Stages, Tasks, Memory(MB), Time(secs)\n')

    for stage in stage_list:
    
        start = time.time()
        master_dict = dict()
        master_dict['pipe_%s'%pipe] = dict()
        path = dir_generator(50)

        for s in range(stage):

            master_dict['pipe_%s'%pipe]['stage_%s'%s] = dict()
            master_dict['pipe_%s'%pipe]['stage_%s'%s]['task_%s'%task] = path

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
        path = dir_generator(50)

        for p in range(pipe):
            
            master_dict['pipe_%s'%p] = dict()
            master_dict['pipe_%s'%p]['stage_%s'%stage] = dict()
            master_dict['pipe_%s'%p]['stage_%s'%stage]['task_%s'%task] = path

        end = time.time()

        mem = float(asizeof.asizeof(master_dict))/(1024*1024)
        dur = end - start
        
        print 'Pipes: %s, Stages: %s, Tasks: %s'%(pipe, stage, task)
        print 'Size of dict: %s MB'%(mem)
        print 'Time taken: %s'%(dur)

        f.write('%s, %s, %s, %s, %s\n'%(pipe,stage,task,mem,dur))

    f.close()

    '''


    pipe  = 100
    stage = 100
    task  = 100

    char_list = [1,10,100,1000,10000,100000,1000000, 10000000]

    f = open('bookkeeper_path_variation.csv','w')
    f.write('Pipelines, Stages, Tasks, Chars, Memory(MB), Time(secs)\n')

    for chars in char_list:
    
        
        master_dict = dict()

        path = dir_generator(chars)

        start = time.time()

        for p in range(pipe):            
            master_dict['pipe_%s'%p] = dict()
            
            for s in range(stage):
                master_dict['pipe_%s'%p]['stage_%s'%s] = dict()

                for t in range(task):
                    master_dict['pipe_%s'%p]['stage_%s'%s]['task_%s'%t] = path

        end = time.time()

        mem = float(asizeof.asizeof(master_dict))/(1024*1024)
        dur = end - start
        
        print 'Pipes: %s, Stages: %s, Tasks: %s, Char: %s'%(pipe, stage, task, chars)
        print 'Size of dict: %s MB'%(mem)
        print 'Time taken: %s'%(dur)

        f.write('%s, %s, %s, %s, %s, %s\n'%(pipe,stage,task,chars,mem,dur))

    f.close()


    '''

    pipe  = 100
    stage = 100
    task  = 100

    chars = 1000

    N = [1,10,100,1000,10000,100000,1000000, 10000000]

    master_dict = dict()

    path = dir_generator(chars)

    for p in range(pipe):            
        master_dict['pipe_%s'%p] = dict()
            
        for s in range(stage):
            master_dict['pipe_%s'%p]['stage_%s'%s] = dict()

            for t in range(task):
                master_dict['pipe_%s'%p]['stage_%s'%s]['task_%s'%t] = path

    

    f = open('bookkeeper_random_accesses.csv','w')
    f.write('N, Time(secs)\n')


    for n in N:
       
        p = random.randint(0,99)
        s = random.randint(0,99)
        t = random.randint(0,99)

        
        start = time.time()

        for i in range(n):
            temp = master_dict['pipe_%s'%p]['stage_%s'%s]['task_%s'%t]
        
        end = time.time()

        dur = end - start
        print 'Time taken: %s'%(dur)

        f.write('%s, %s\n'%(n,dur))

    f.close()
    '''