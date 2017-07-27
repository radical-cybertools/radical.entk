from radical.entk.appman.wfprocessor import WFprocessor
from radical.entk import Pipeline, Stage, Task
import pytest
from radical.entk.exceptions import *
from Queue import Queue

def test_wfp_attributes():

    """
    **Purpose**: Test the attributes that the WFprocessor object accepts upon instantiation
    """

    def create_single_task():

        t1 = Task()
        t1.environment = ['module load gromacs']
        t1.executable = ['gmx mdrun']
        t1.arguments = ['a','b','c']
        t1.copy_input_data = []
        t1.copy_output_data = []

        return t1

    queue_type = [1,'a',True, set([1])]

    p1 = Pipeline()
    stages=3

    for cnt in range(stages):
        s = Stage()
        s.name = 's%s'%cnt
        s.tasks = create_single_task()
        s.add_tasks(create_single_task())

        p1.add_stages(s)

    for queue in queue_type:

        with pytest.raises(TypeError):
            p = WFprocessor(p1, queue, queue, 'localhost')


    def create_single_task():

        t1 = Task()
        t1.environment = ['module load gromacs']
        t1.executable = ['gmx mdrun']
        t1.arguments = ['a','b','c']
        t1.copy_input_data = []
        t1.copy_output_data = []

        return t1

    queue = ['test']
    hostname_type = [1,True, set([1])]

    p1 = Pipeline()
    stages=3

    for cnt in range(stages):
        s = Stage()
        s.name = 's%s'%cnt
        s.tasks = create_single_task()
        s.add_tasks(create_single_task())

        p1.add_stages(s)

    for hostname in hostname_type:

        with pytest.raises(TypeError):
            p = WFprocessor(p1, queue, queue, hostname)



def test_wfp_process():

    """
    **Purpose**: Test the functions to start and terminate the WFP process
    """

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

    p = WFprocessor(p1, ['pendingq'], ['completedq'], 'localhost')
    p.start_processor()
    assert p.check_alive() == True
    p.end_processor()
    assert p.check_alive() == False