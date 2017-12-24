from profiler import Profiler
import pprint

if __name__ == '__main__':

    p = Profiler()

    print p.duration(objects= ['task.0000', 'task.0001'], states=['SCHEDULING', 'EXECUTED'])
    print p.duration(
        objects= ['wfprocessor.0000'], 
        events=['starting dequeue-thread', 'terminating dequeue-thread'])


    print p.duration(
        objects= ['task.0000','wfprocessor.0000',], 
        events=['starting dequeue-thread'], states=['EXECUTED'])

