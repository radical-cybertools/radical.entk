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
            p = WFprocessor(sid = 'xyz',
                            workflow = p1, 
                            pending_queue = queue, 
                            completed_queue = queue, 
                            mq_hostname = 'localhost', 
                            port = 5672, 
                            resubmit_failed = False)


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
            p = WFprocessor(sid = 'xyz',
                            workflow = p1, 
                            pending_queue = queue, 
                            completed_queue = queue, 
                            mq_hostname = hostname, 
                            port = 5672, 
                            resubmit_failed = False)



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



    p = WFprocessor(sid = 'xyz',
                    workflow = p1,
                    pending_queue = ['pendingq'],
                    completed_queue = ['completedq'],
                    mq_hostname = 'localhost',
                    port = 5672,
                    resubmit_failed = False)

    p.start_processor()
    assert p.check_processor() == True
    p.terminate_processor()
    assert p.check_processor() == False