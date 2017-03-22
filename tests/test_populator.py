from radical.entk.appman.populator import Populator
from radical.entk import Pipeline, Stage, Task
import pytest
from radical.entk.exceptions import *
from Queue import Queue

def test_workload_type():

    data_type = [1]
    q = Queue()

    for data in data_type:

        with pytest.raises(TypeError):
            p = Populator(data,q)

def test_pending_queue_type():


    def create_single_task():

        t1 = Task()
        t1.environment = ['module load gromacs']
        t1.executable = ['gmx mdrun']
        t1.arguments = ['a','b','c']
        t1.copy_input_data = []
        t1.copy_output_data = []

        return t1

    data_type = [1,'a',True, [1], set([1])]
    p1 = Pipeline()
    stages=3

    for cnt in range(stages):
        s = Stage()
        s.name = 's%s'%cnt
        s.tasks = create_single_task()
        s.add_tasks(create_single_task())

        p1.add_stages(s)

    for data in data_type:

        with pytest.raises(TypeError):
            p = Populator(p1,data)

def test_thread_termination():

    def create_single_task():

        t1 = Task()
        t1.environment = ['module load gromacs']
        t1.executable = ['gmx mdrun']
        t1.arguments = ['a','b','c']
        t1.copy_input_data = []
        t1.copy_output_data = []

        return t1

    q = Queue()
    p1 = Pipeline()
    stages=3

    for cnt in range(stages):
        s = Stage()
        s.name = 's%s'%cnt
        s.tasks = create_single_task()
        s.add_tasks(create_single_task())

        p1.add_stages(s)

    p = Populator(p1,q)
    p.start_population()
    p.terminate()
    assert p.check_alive() == False