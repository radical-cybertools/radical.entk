from radical.entk import Task
import sys
import time
from multiprocessing import Process
import multiprocessing as mp
import os
import shutil
from Queue import Empty
from pympler import asizeof
import psutil
import traceback

kill_pusher = mp.Event()
kill_popper = mp.Event()

num_push_procs = 32
num_pop_procs = 32
num_queues = 32

def push_function(q, name):

    try:

        start_time = time.time()
        
        tasks_pushed = 0

        push_times = []
        q_len = []
        q_sizes = []
        proc_mem = []
        t = Task()
        t.arguments = ["--template=PLCpep7_template.mdp",
                        "--newname=PLCpep7_run.mdp",
                        "--wldelta=100",
                        "--equilibrated=False",
                        "--lambda_state=0",
                        "--seed=1"]

        t.cores = 20
        t.copy_input_data = ['$STAGE_2_TASK_1/PLCpep7.tpr']
        t.download_output_data = ['PLCpep7.xtc > PLCpep7_run1_gen0.xtc',
                                    'PLCpep7.log > PLCpep7_run1_gen0.log',
                                    'PLCpep7_dhdl.xvg > PLCpep7_run1_gen0_dhdl.xvg',
                                    'PLCpep7_pullf.xvg > PLCpep7_run1_gen0_pullf.xvg',
                                    'PLCpep7_pullx.xvg > PLCpep7_run1_gen0_pullx.xvg',
                                    'PLCpep7.gro > PLCpep7_run1_gen0.gro'
                                ]
        print 'Size of task: ', asizeof.asizeof(t)

        while (tasks_pushed < 1000000)and(not kill_pusher.is_set()):

            #t = DATA
            q.put(t)

            tasks_pushed +=1
            cur_time = time.time()

            push_times.append(cur_time)
            q_len.append(q.qsize())
            q_sizes.append(asizeof.asizeof(q))
            mem = psutil.Process(os.getpid()).memory_info().rss / float(2 ** 20) # MB
            proc_mem.append(mem)

            #    print '%s: Push average throughput: %s tasks/sec'%(name, 
            #        float(tasks_pushed/(cur_time - start_time)))

        print len(push_times), len(q_len), len(q_sizes)
        

        f = open(DATA + '/%s.txt'%name,'w')
        for ind in range(len(push_times)):
            f.write('%s %s %s %s\n'%(push_times[ind],q_len[ind],q_sizes[ind], proc_mem[ind]))
            #f.write('%s\n'%(push_times[ind]))
        f.close()

        print 'Push proc killed'



    except KeyboardInterrupt:

        print len(push_times), len(q_len), len(q_sizes)

        f = open(DATA + '/%s.txt'%name,'w')
        for ind in range(min(len(push_times),len(q_len),len(q_sizes), len(proc_mem))):
            f.write('%s %s %s %s\n'%(push_times[ind],q_len[ind],q_sizes[ind], proc_mem[ind]))
        f.close()

        print 'Push proc killed'
        print traceback.format_exc()

    except Exception,ex:

        print len(push_times), len(q_len), len(q_sizes)

        f = open(DATA + '/%s.txt'%name,'w')
        for ind in range(min(len(push_times),len(q_len),len(q_sizes), len(proc_mem))):
            f.write('%s %s %s %s\n'%(push_times[ind],q_len[ind],q_sizes[ind], proc_mem[ind]))
        f.close()

        print 'Unexpected error: %s'%ex
        print traceback.format_exc()


def pop_function(q, name):

    try:

        start_time = time.time()
        tasks_popped = 0

        pop_times = []
        q_len = []
        q_sizes = []
        proc_mem = []

        while (tasks_popped < 1000000)and(not kill_popper.is_set()):
            try:
                t = q.get()            

                tasks_popped +=1
                cur_time = time.time()

                pop_times.append(cur_time)
                q_len.append(q.qsize())                
                q_sizes.append(asizeof.asizeof(q))
                mem = psutil.Process(os.getpid()).memory_info().rss / float(2 ** 20) # MB
                proc_mem.append(mem)

                #    print '%s: Pop average throughput: %s tasks/sec'%(name, 
                #        float(tasks_popped/(cur_time - start_time)))
            
            except Empty:
                pass

        f = open(DATA + '/%s.txt'%name,'w')

        for ind in range(len(pop_times)):
            f.write('%s %s %s %s\n'%(pop_times[ind],q_len[ind],q_sizes[ind], proc_mem[ind]))
        f.close()

        print 'Pop proc killed'


    except KeyboardInterrupt:

        print len(pop_times), len(q_len), len(q_sizes)

        f = open(DATA + '/%s.txt'%name,'w')
        for ind in range(min(len(pop_times),len(q_len),len(q_sizes), len(proc_mem))):
            f.write('%s %s %s %s\n'%(pop_times[ind],q_len[ind],q_sizes[ind]), proc_mem[ind])
        f.close()

        print 'Pop proc killed'
        print traceback.format_exc()

    except Exception,ex:

        print len(push_times), len(q_len), len(q_sizes)

        f = open(DATA + '/%s.txt'%name,'w')
        for ind in range(min(len(pop_times),len(q_len),len(q_sizes), len(proc_mem))):
            f.write('%s %s %s %s\n'%(pop_times[ind],q_len[ind],q_sizes[ind]), proc_mem[ind])
        f.close()

        print 'Unexpected error: %s'%ex
        print traceback.format_exc()


if __name__ == '__main__':

    
    if (num_queues>num_pop_procs)or(num_queues>num_push_procs):
        print 'Check number of queues, procs'
        sys.exit(1)

    
    trials=3

    try:


        for i in range(trials):

            DATA = './push_%s_pop_%s_q_%s_trial_%s'%(num_push_procs, num_pop_procs, num_queues,i)

            try:
                shutil.rmtree(DATA)
            except:
                pass

            os.makedirs(DATA)

            # Create the queues first
            q_list = []
            pop_procs = []
            push_procs = []

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

        kill_popper.set()
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
