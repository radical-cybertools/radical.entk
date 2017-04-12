from radical.entk import Task
import sys
import time
from multiprocessing import Process
import multiprocessing as mp
import os
import shutil
from Queue import Empty
from pympler import asizeof

kill_pusher = mp.Event()
kill_popper = mp.Event()

num_push_procs = 1
num_pop_procs = 1
num_queues = 1

DATA = './push_%s_pop_%s_q_%s'%(num_push_procs, num_pop_procs, num_queues)

try:
    shutil.rmtree(DATA)
except:
    pass


os.makedirs(DATA)

def push_function(q, name):

    try:

        start_time = time.time()
        
        tasks_pushed = 0

        push_times = []
        q_len = []
        q_sizes = []

        while (tasks_pushed < 1000000)and(not kill_pusher.is_set()):

            #t = Task()
            t = DATA
            q.put(t)

            tasks_pushed +=1
            cur_time = time.time()

            push_times.append(cur_time)
            q_len.append(q.qsize())
            q_sizes.append(asizeof.asizeof(q))

            if tasks_pushed%100000 == 0:
                print tasks_pushed
            #    print '%s: Push average throughput: %s tasks/sec'%(name, 
            #        float(tasks_pushed/(cur_time - start_time)))

        print len(push_times), len(q_len), len(q_sizes)
        

        f = open(DATA + '/%s.txt'%name,'w')
        for ind in range(len(push_times)):
            f.write('%s %s %s\n'%(push_times[ind],q_len[ind],q_sizes[ind]))
            #f.write('%s\n'%(push_times[ind]))
        f.close()

        print 'Push proc killed'



    except KeyboardInterrupt:

        print len(push_times), len(q_len), len(q_sizes)

        f = open(DATA + '/%s.txt'%name,'w')
        for ind in range(min(len(push_times),len(q_len),len(q_sizes))):
            f.write('%s %s %s\n'%(push_times[ind],q_len[ind],q_sizes[ind]))
        f.close()

        print 'Push proc killed'

    except Exception,ex:

        print len(push_times), len(q_len), len(q_sizes)

        f = open(DATA + '/%s.txt'%name,'w')
        for ind in range(min(len(push_times),len(q_len),len(q_sizes))):
            f.write('%s %s %s\n'%(push_times[ind],q_len[ind],q_sizes[ind]))
        f.close()

        print 'Unexpected error: %s'%ex


def pop_function(q, name):

    try:

        start_time = time.time()

        tasks_popped = 0

        pop_times = []
        q_len = []
        q_sizes = []
        while (tasks_popped < 1000000)and(not kill_popper.is_set()):

            try:
                t = q.get()            

                tasks_popped +=1
                cur_time = time.time()

                pop_times.append(cur_time)
                q_len.append(q.qsize())
                q_sizes.append(asizeof.asizeof(q))

                if tasks_popped%100000 == 0:
                    print tasks_popped
                #    print '%s: Pop average throughput: %s tasks/sec'%(name, 
                #        float(tasks_popped/(cur_time - start_time)))
            
            except Empty:
                pass

        f = open(DATA + '/%s.txt'%name,'w')
        for ind in range(len(pop_times)):
            f.write('%s %s %s\n'%(pop_times[ind],q_len[ind],q_sizes[ind]))
        f.close()

        print 'Pop proc killed'


    except KeyboardInterrupt:

        print len(pop_times), len(q_len), len(q_sizes)

        f = open(DATA + '/%s.txt'%name,'w')
        for ind in range(min(len(pop_times),len(q_len),len(q_sizes))):
            f.write('%s %s %s\n'%(pop_times[ind],q_len[ind],q_sizes[ind]))
        f.close()

        print 'Pop proc killed'

    except Exception,ex:

        print len(push_times), len(q_len), len(q_sizes)

        f = open(DATA + '/%s.txt'%name,'w')
        for ind in range(min(len(pop_times),len(q_len),len(q_sizes))):
            f.write('%s %s %s\n'%(pop_times[ind],q_len[ind],q_sizes[ind]))
        f.close()

        print 'Unexpected error: %s'%ex


if __name__ == '__main__':

    
    if (num_queues>num_pop_procs)or(num_queues>num_push_procs):
        print 'Check number of queues, procs'
        sys.exit(1)

    # Create the queues first
    q_list = []
    pop_procs = []
    push_procs = []

    try:
        for n in range(num_queues):
            q_list.append(mp.Queue())

        print 'Queues created'

        
        # Start popping procs and assign queues
        for t in range(num_pop_procs):
            cur_q = t%len(q_list)   # index of queue to be used
            name = 'pop_%s_queue_%s'%(t,cur_q)
            #t1 = procing.Thread(target=pop_function, args=(q_list[cur_q],name), name=name)
            t1 = Process(target=pop_function, args=(q_list[cur_q],name), name=name)
            t1.start()
            pop_procs.append(t1)

        print 'Pop procs created'
        

        print 'start time: ', time.time()
        # Start pushing procs and assign queues
        for t in range(num_push_procs):
            cur_q = t%len(q_list)   # index of queue to be used
            name = 'push_%s_queue_%s'%(t,cur_q)
            #t1 = procing.Thread(target=push_function, args=(q_list[cur_q],name), name=name)
            t1 = Process(target=push_function, args=(q_list[cur_q],name), name=name)
            t1.start()
            push_procs.append(t1)

        print 'Push procs created '


        for t in push_procs:
            t.join()

        for t in pop_procs:
            t.join()

    except KeyboardInterrupt:
        print 'Main process killed'
        kill_pusher.set()
        kill_popper.set()
        for t in pop_procs:
            t.join()

        for t in push_procs:
            t.join()

    except Exception, ex:
        print 'Unknown error: %s' %ex
        kill_pusher.set()
        kill_popper.set()
        for t in pop_procs:
            t.join()

        for t in push_procs:
            t.join()