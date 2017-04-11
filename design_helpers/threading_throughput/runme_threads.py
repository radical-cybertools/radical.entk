from radical.entk import Task
import threading
import Queue
import sys
import time
from pympler import asizeof
from datetime import datetime
from multiprocessing import Process

kill = threading.Event()
DATA = ''

def push_function(q, name):

    try:

        start_time = time.time()
        tasks_pushed = 0

        f = open('thread_%s.txt'%name,'w')
        #header = 'timestamp\n'
        #f.write(header)
        while not kill.is_set():

            #t = Task()
            t = DATA
            q.put(t)

            #tasks_pushed +=1
            cur_time = time.time()

            #if tasks_pushed%10000 == 0:
            #print '%s: Push average throughput: %s tasks/sec'%(name, float(tasks_pushed/(cur_time - start_time)))
            #print '%s: Size of queue: %s, %s B'%(name, q.qsize(), asizeof.asizeof(q))
            line = '%s\n' %cur_time
            f.write(line)

        #f.close()

    except KeyboardInterrupt:
        print 'Push thread killed'
        #f.close()

    except Exception,ex:
        print 'Unexpected error: %s'%ex
        #f.close()


def pop_function(q, name):

    try:

        start_time = time.time()
        tasks_popped = 0

        f = open('thread_%s.txt'%name,'w')
        #header = 'timestamp\n'
        #f.write(header)
        while not kill.is_set():

            try:
                #t = q.get(timeout=2)            

                #tasks_popped +=1
                #cur_time = time.time()
                cur_time = time.time()

                #if tasks_popped%10000 == 0:
                #    print '%s: Pop average throughput: %s tasks/sec'%(name, float(tasks_popped/(cur_time - start_time)))
                #    print '%s: Size of queue: %s, %s B'%(name, q.qsize(), asizeof.asizeof(q))

                line = '%s\n' %cur_time
                f.write(line)

            except Queue.Empty:
                pass

        f.close()

    except KeyboardInterrupt:
        print 'Push thread killed'
        f.close()

    except Exception,ex:
        print 'Unexpected error: %s'%ex
        f.close()


if __name__ == '__main__':

    
    num_push_threads = 1
    num_pop_threads = 1
    num_queues = 1

    print 'Size of DATA: %s'%(asizeof.asizeof(DATA))
    print 'Size of DATA: %s'%(sys.getsizeof(DATA))
    print 'Size of Task: %s'%(asizeof.asizeof(Task()))

    if (num_queues>num_pop_threads)or(num_queues>num_push_threads):
        print 'Check number of queues, threads'
        sys.exit(1)

    # Create the queues first
    q_list = []
    pop_threads = []
    push_threads = []

    try:
        for n in range(num_queues):
            q_list.append(Queue.Queue())

        print 'Queues created'

        
        # Start popping threads and assign queues
        for t in range(num_pop_threads):
            cur_q = t%len(q_list)   # index of queue to be used
            name = 'pop_%s_queue_%s'%(t,cur_q)
            #t1 = threading.Thread(target=pop_function, args=(q_list[cur_q],name), name=name)
            t1 = Process(target=pop_function, args=(q_list[cur_q],name), name=name)
            t1.start()
            pop_threads.append(t1)

        print 'Pop threads created'
        

        print 'start time: ', time.time()
        # Start pushing threads and assign queues
        for t in range(num_push_threads):
            cur_q = t%len(q_list)   # index of queue to be used
            name = 'push_%s_queue_%s'%(t,cur_q)
            #t1 = threading.Thread(target=push_function, args=(q_list[cur_q],name), name=name)
            t1 = Process(target=push_function, args=(q_list[cur_q],name), name=name)
            t1.start()
            push_threads.append(t1)

        print 'Push threads created '


        while True:
            #time.sleep(1)
            pass

    except KeyboardInterrupt:
        print 'Main process killed'
        kill.set()
        for t in pop_threads:
            t.join()

        for t in push_threads:
            t.join()

    except Exception, ex:
        print 'Unknown error: %s' %ex
        kill.set()
        for t in pop_threads:
            t.join()

        for t in push_threads:
            t.join()    