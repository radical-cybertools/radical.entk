#!/usr/bin/env python

from multiprocessing import Process
import zmq
import time
import pprint
from radical.entk import Task
import traceback

def worker():

    try:
        
        context = zmq.Context()
        worker_recv = context.socket(zmq.SUB)
        worker_recv.connect("tcp://127.0.0.1:5556")
        worker_recv.setsockopt(zmq.SUBSCRIBE, "")

        msg_num = 0
        while msg_num<9:
            #  Wait for next request from client
            message = worker_recv.recv_pyobj()
            print "Received request: %s" % message
            msg_num+=1

    except:

        print 'Unexpected error: %s'%ex
        print traceback.format_exc()


if __name__ == '__main__':


    context = zmq.Context()
    print "Connecting to hello world server"
    sender = context.socket(zmq.PUB)
    sender.bind("tcp://127.0.0.1:5556")


    worker = []
    for i in range(1):
        worker.append(Process(target=worker))
        worker[i].start()


    # Create task dict

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
    #print 'Size of task: ', asizeof.asizeof(t)

    #  Do 10 requests, waiting each time for a response
    time.sleep(1)
    for request in range(10):
        print "Sending request %s " % request
        sender.send_pyobj(t)

    for i in range(1):
        worker[i].join()