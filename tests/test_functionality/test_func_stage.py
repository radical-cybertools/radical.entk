from radical.entk import Pipeline, Stage, Task
from radical.entk import states
from radical.entk.exceptions import *
import pytest


def test_task_assignment_in_stage():

    """
    ***Purpose***: Test if necessary attributes are automatically updates upon task assignment
    """

    s = Stage()
    t = Task()
    t.executable = ['/bin/date']
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
    t1.executable = ['/bin/date']
    t2 = Task()
    t2.executable = ['/bin/date']
    s.add_tasks(set([t1,t2]))

    assert type(s.tasks) == set
    assert s._task_count == 2
    assert t1 in s.tasks
    assert t2 in s.tasks


    s = Stage()
    t1 = Task()
    t1.executable = ['/bin/date']
    t2 = Task()
    t2.executable = ['/bin/date']
    s.add_tasks([t1,t2])

    assert type(s.tasks) == set
    assert s._task_count == 2
    assert t1 in s.tasks
    assert t2 in s.tasks


def test_uid_passing():

    """
    ***Purpose***: Test automatic uid assignments of all Stages and Tasks of a Pipeline
    """

    p = Pipeline()
    s = Stage()
    t = Task()
    t.executable = ['/bin/date']
    s.tasks = t
    p.stages = s

    p._initialize('temp')

    assert s.parent_pipeline['uid'] == p.uid
    assert t.parent_pipeline['uid'] == s.parent_pipeline['uid']
    assert t.parent_stage['uid'] == s.uid

def test_task_set_state():

    """
    ***Purpose***: Test method to set state of all Tasks of a Stage
    """

    s = Stage()
    t1 = Task()
    t1.executable = ['/bin/date']
    t2 = Task()
    t2.executable = ['/bin/date']
    s.add_tasks([t1,t2])

    with pytest.raises(ValueError):
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
    t1.executable = ['/bin/date']
    t2 = Task()
    t2.executable = ['/bin/date']
    s.add_tasks([t1,t2])

    assert s._check_stage_complete() == False
    s._set_tasks_state(states.DONE)
    assert s._check_stage_complete() == True