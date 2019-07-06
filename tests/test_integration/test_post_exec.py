from radical.entk import Pipeline, Stage, Task, AppManager
import pytest
from radical.entk.exceptions import *
import os

hostname = os.environ.get('RMQ_HOSTNAME','localhost')
port = int(os.environ.get('RMQ_PORT',5672))
MLAB = os.environ.get('RADICAL_PILOT_DBURL')

def create_single_task():

    t1 = Task()
    t1.name = 'simulation'
    t1.executable = '/bin/echo'
    t1.arguments = ['hello']
    t1.copy_input_data = []
    t1.copy_output_data = []

    return t1

NUM_TASKS = 2
MAX_STAGES = 5
CUR_STAGE = 1

def condition():

    global CUR_STAGE, MAX_STAGES
    
    if CUR_STAGE < MAX_STAGES:
        CUR_STAGE += 1
        on_true()

    on_false()

def on_true():

    global NUM_TASKS, CUR_STAGE

    NUM_TASKS *= 2

    s = Stage()
    s.name = 's%s'%CUR_STAGE

    for t in range(NUM_TASKS):
        s.add_tasks(create_single_task())

    s.post_exec = condition

    p1.add_stages(s)


def on_false():
    pass


p1 = Pipeline()

def test_stage_post_exec():

    global p1
    
    p1.name = 'p1'

    s = Stage()
    s.name = 's1'

    for t in range(NUM_TASKS):
        s.add_tasks(create_single_task())

    s.post_exec = condition

    p1.add_stages(s)

    res_dict = {

            'resource': 'local.localhost',
            'walltime': 30,
            'cpus': 1,
    }

    os.environ['RADICAL_PILOT_DBURL'] = MLAB
    appman = AppManager(rts='radical.pilot', hostname=hostname, port=port)
    appman.resource_desc = res_dict
    appman.workflow = [p1]
    appman.run()
