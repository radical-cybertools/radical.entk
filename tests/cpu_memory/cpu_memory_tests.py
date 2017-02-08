from radical.entk import Pipeline, Stage, Task
import time, sys
import psutil
from pympler import asizeof

def create_task():

    t1 = Task(name='simulation')
    t1.environment = ['module load gromacs']
    t1.executable = ['gmx mdrun']
    t1.arguments = ['a','b','c']
    t1.copy_input_data = []
    t1.copy_output_data = []

    return t1


def create_workload(tasks, stages, pipelines):
    
    start = time.time()

    set_of_tasks = frozenset([create_task() for _ in range(tasks)]) # equivalent to one stage
    list_stages = tuple([set_of_tasks for _ in range(stages)]) # equivalent to one pipe
    set_of_pipelines = set([list_stages for _ in range(pipelines)]) # equivalent to one application

    end = time.time()

    #print 'pipes: %s, stages: %s, tasks: %s, time: %s\n'%(pipelines, stages, tasks, end-start)

    return [set_of_pipelines, end - start]


if __name__ == '__main__':


    
    f = open('monitor_task_variation.csv','w')
    header = 'tasks, stages, pipelines, cpu, memory\n'
    f.write(header)
    task_list = [1,10,100,1000,10000,100000,1000000]
    stages=1
    pipelines=1

    for tasks in task_list:
        [wl, t] = create_workload(tasks=tasks,stages=stages,pipelines=pipelines)
        mem = asizeof.asizeof(wl)
        f.write('%s, 1, 1, %s, %0.3f\n'%(tasks, t, float(mem)/(1024*1024)))
        del wl

    f.close()

    
    f = open('monitor_stage_variation.csv','w')
    header = 'tasks, stages, pipelines, cpu, memory\n'
    f.write(header)
    tasks = 1000000
    stage_list = [1,10,100,1000,10000,100000,1000000]
    pipelines = 1

    for stages in stage_list:
        [wl, t] = create_workload(tasks=tasks,stages=stages,pipelines=pipelines)
        mem = asizeof.asizeof(wl)
        f.write('%s, %s, %s, %s, %0.3f\n'%(tasks, stages, pipelines, t, float(mem)/(1024*1024)))
        del wl
        
    f.close()
    

    f = open('monitor_pipeline_variation.csv','w')
    header = 'tasks, stages, pipelines, cpu, memory\n'
    f.write(header)
    tasks = 1000
    stages = 1000000
    pipeline_list = [1,10,100,1000,10000,100000,1000000]

    for pipelines in pipeline_list:
        [wl, t] = create_workload(tasks=tasks,stages=stages,pipelines=pipelines)
        mem = asizeof.asizeof(wl)
        f.write('%s, %s, %s, %s, %0.3f\n'%(tasks, stages, pipelines, t, float(mem)/(1024*1024)))
        print '%s, %s, %s, %s, %0.3f\n'%(tasks, stages, pipelines, t, float(mem)/(1024*1024))
        del wl
        
    f.close()