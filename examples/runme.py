from profiler import Profiler
import pprint

def get_task_uids(num_pipelines):
    
    num_tasks = num_pipelines*7*1
    task_uids = []
    for t in range(num_tasks):
        task_uids.append('radical.entk.task.%04d'%t)

    return task_uids

if __name__ == '__main__':

    task_uids = get_task_uids(16)

    p = Profiler(src='./16_replicas/')
    print p.duration(task_uids, states=['SCHEDULING','DONE'])
