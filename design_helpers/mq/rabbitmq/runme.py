#!/usr/bin/env python

import time
from multiprocessing import Process
from threading import Thread
import traceback
import pika
from radical.entk import Task
import json
import os
import shutil

def worker(queue, ind):

    try:

        global DATA

        connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
        channel = connection.channel()

        pending_data = True

        times = []

        start = time.time()
        while pending_data:

            method_frame, header_frame, body = channel.basic_get(queue=queue)

            if body:
                channel.basic_ack(delivery_tag=method_frame.delivery_tag)
                times.append(time.time())
                
                #mem = psutil.Process(os.getpid()).memory_info().rss / float(2 ** 20) # MB
                #mems.append(mem)

                #print 'Proc %s: '%ind, json.loads(body)['uid']
            else:
                #print 'Proc %s: '%ind,'Done'
                pending_data = False
                #connection.close()

        f = open(DATA + '/consumer_%s.txt'%ind,'w')
        f.write('Consumer %s start time: %s %s\n'%(ind, start, len(times)))
        f.write('Consumer %s end time: %s %s\n'%(ind, time.time(), len(times)))
        f.close()
        #f = open('%s/profile_proc%s.txt'%(DATA,ind),'w')
        #for val in times:
        #    f.write('%s %s\n'%(val, mems[times.index(val)]))
        #f.close()

    except:

        print 'Process failed'
        print traceback.format_exc()
    

num_tasks = 10000
DATA = ''
trials = [1]
if __name__ == '__main__':

    for trial in trials:

        for num_queues in [1]:

            for num_workers in [2]:

                if num_workers>=num_queues:

                    DATA = './workers_%s_queues_%s_trial_%s'%(num_workers, num_queues, trial)
            
                    try:
                        shutil.rmtree(DATA)
                    except:
                        pass

                    os.makedirs(DATA)

                    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
                    channel = connection.channel()

                    worker_queue = []
                    for i in range(num_queues):
                        worker_queue.append('n1-worker-%s'%i)
                        channel.queue_delete(queue=worker_queue[i%num_queues])
                        channel.queue_declare(queue=worker_queue[i%num_queues])

                    # Start worker process 
                    workers = []
                    for i in range(num_workers):
        
                        w = Thread(target=worker, name='worker', args=(worker_queue[i%num_queues],i))                        
                        w.start()
                        workers.append(w)
        
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

                    t = json.dumps(t.to_dict())

                    msg_num = 0
                    start = time.time()
                    while msg_num < num_tasks:

                        #message = 'message_%s'%msg_num
                        msg_num+=1

                        channel.basic_publish(exchange='',
                                      routing_key=worker_queue[msg_num%num_queues],
                                      body=t)


                    #print(" [x] Sent %r" % message)
                    f = open(DATA +'/producer.txt','w')
                    f.write('Producer start time: %s\n'%start)
                    f.write('Producer end time: %s\n'%time.time())
                    f.close()

                    connection.close()

                    for w in workers:
                        w.join()
