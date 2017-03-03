from radical.entk import Pipeline, Stage, Task
import time, sys
import psutil
from pympler import asizeof

def create_single_task():

    t1 = Task(name='simulation')
    t1.environment = ['module load gromacs']
    t1.executable = ['gmx mdrun']
    t1.arguments = ['a','b','c']
    t1.copy_input_data = []
    t1.copy_output_data = []

    return t1

def create_single_stage(tasks):

    stage = frozenset([create_single_task() for _ in range(tasks)])
    return stage

def create_single_pipe(tasks, stages):

    pipe = tuple([create_single_stage(tasks) for _ in range(stages)])
    return pipe

def create_workload(tasks, stages, pipelines):

    start = time.time()
    wl = set([create_single_pipe(tasks, stages) for _ in range(pipelines)])
    end = time.time()

    return [wl, end - start]
    
if __name__ == '__main__':


    
    f = open('separate_data_objects/monitor_pipeline_variation.csv','w')
    header = 'tasks, stages, pipelines, cpu, memory\n'
    f.write(header)
    #task_list = [1,10,100,1000,10000,100000,1000000]
    task_list = [1]
    #stage_list = [1,10,100,1000,10000,100000,1000000]
    stage_list = [1]
    pipe_list = [1,10,100,1000,10000,100000,1000000] 
    #pipe_list = [1]

    for pipelines in pipe_list:
        for stages in stage_list:
            for tasks in task_list:

                #if pipelines*stages*tasks <= 1000000:

                    [wl, t] = create_workload(tasks=tasks,stages=stages,pipelines=pipelines)
                    mem = asizeof.asizeof(wl)
                    print '%s, %s, %s, %s, %0.3f\n'%(tasks, stages, pipelines, t, float(mem)/(1024*1024))
                    f.write('%s, %s, %s, %s, %0.3f\n'%(tasks, stages, pipelines, t, float(mem)/(1024*1024)))
                    del wl
        
    f.close()
    
