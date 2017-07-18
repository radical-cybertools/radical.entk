from radical.entk import Pipeline, Stage, Task
from radical.entk import states
from radical.entk.exceptions import *
import pytest

def test_stage_initialization():

    """
    ***Purpose***: Test if all attributes have, thus expect, the correct data types
    """

    s = Stage()

    assert type(s._uid) == str
    assert type(s.tasks) == set
    assert type(s.name) == str
    assert type(s.state) == str
    assert s.state == states.INITIAL
    assert s._task_count == 0
    assert s._parent_pipeline == None

def test_assignment_exceptions():

    """
    ***Purpose***: Test if correct exceptions are raised when attributes are assigned unacceptable values.
    """

    s = Stage()

    data_type = [1,'a',True, [1], set([1])]

    for data in data_type:


        if not isinstance(data,str):
            with pytest.raises(TypeError):
                s.name = data

        with pytest.raises(TypeError):
            s.tasks = data

        with pytest.raises(TypeError):
            s.add_tasks(data)

        if not isinstance(data,str):
            with pytest.raises(TypeError):
                s.remove_tasks(data)


            with pytest.raises(TypeError):
                s._set_tasks_state(data)


def test_task_assignment_in_stage():

    """
    ***Purpose***: Test if necessary attributes are automatically updates upon task assignment
    """

    s = Stage()
    t = Task()
    s.tasks = t

    assert type(s.tasks) == set
    assert s._task_count == 1
    assert t in s.tasks 


def test_task_addition_in_stage():

    """
    ***Purpose***: Test if necessary attributes are automatically updates upon task addition
    """

    s = Stage()
    t1 = Task()
    t2 = Task()
    s.add_tasks(set([t1,t2]))

    assert type(s.tasks) == set
    assert s._task_count == 2
    assert t1 in s.tasks
    assert t2 in s.tasks


    s = Stage()
    t1 = Task()
    t2 = Task()
    s.add_tasks([t1,t2])

    assert type(s.tasks) == set
    assert s._task_count == 2
    assert t1 in s.tasks
    assert t2 in s.tasks


def test_task_removal_from_stage():

    """
    ***Purpose***: Test if necessary attributes are automatically updates upon task removal
    """

    s = Stage()
    t1 = Task()
    t1.name = 't1'
    t2 = Task()
    t2.name = 't2'
    t3 = Task()
    t3.name = 't3'
    s.add_tasks([t1,t2,t3])

    assert type(s.tasks) == set
    assert s._task_count == 3

    s.remove_tasks('t2')
    assert s._task_count == 2    

    s.remove_tasks('t1')
    assert s._task_count == 1

    s.remove_tasks('t1')
    assert s._task_count == 1
    

def test_uid_passing():

    """
    ***Purpose***: Test automatic uid assignments of all Stages and Tasks of a Pipeline
    """

    p = Pipeline()
    s = Stage()
    t = Task()
    s.tasks = t
    p.stages = s

    assert s._parent_pipeline == p.uid
    assert t._parent_pipeline == s._parent_pipeline
    assert t._parent_stage == s.uid

def test_task_set_state():

    """
    ***Purpose***: Test method to set state of all Tasks of a Stage
    """

    s = Stage()
    t1 = Task()
    t2 = Task()
    s.add_tasks([t1,t2])

    with pytest.raises(TypeError):
        s._set_tasks_state(2)

    s._set_tasks_state(states.DONE)
    assert t1.state == states.DONE
    assert t2.state == states.DONE

def test_check_stage_complete():
    """
    ***Purpose***: Test method to test if all Tasks of a Stage are DONE and hence the Stage is DONE
    """

    s = Stage()
    t1 = Task()
    t2 = Task()
    s.add_tasks([t1,t2])

    assert s._check_stage_complete() == False
    s._set_tasks_state(states.DONE)
    assert s._check_stage_complete() == True