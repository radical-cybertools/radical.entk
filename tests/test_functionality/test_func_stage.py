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


def test_from_dict():
    s1 = Stage()
    s1.name = ['test']
    s1._assign_uid('s1')
    p = Pipeline()
    p.add_stages(s1)
    p._assign_uid('p1')
    p._pass_uid()

    stage_dict = s1.to_dict()
    s2 = Stage()
    s2.from_dict(stage_dict)

    assert s1.uid == s2.uid
    assert s1.name == s2.name
    assert s1.state == s2.state
    assert s1.state_history == s2.state_history
    assert s1.parent_pipeline == s2.parent_pipeline


def test_to_dict():
    s = Stage()
    s.name = ['test']
    s._assign_uid('s1')
    p = Pipeline()
    p.add_stages(s)
    p._assign_uid('p1')
    p._pass_uid()

    stage_dict = s.to_dict()
    assert stage_dict['uid'] == s.uid
    assert stage_dict['name'] == s.name
    assert stage_dict['state'] == s.state
    assert stage_dict['state_history'] == s.state_history
    assert stage_dict['parent_pipeline'] == s.parent_pipeline

@given(s=st.text(), l=st.lists(st.text()), i=st.integers().filter(lambda x: type(x) == int), b=st.booleans())
def test_task_from_dict_exceptions(s,l,i,b):
    ## Check exceptions

    s = Stage()
    s.name = ['test']
    s._assign_uid('s1')
    p = Pipeline()
    p.add_stages(s)
    p._assign_uid('p1')
    p._pass_uid()

    stage_dict = s.to_dict()
    
    data_type = [s,l,i,b]

    for data in data_type:

        stage_dict_copy = stage_dict

        if not isinstance(data,str):

            stage_dict_copy['uid'] = data
            s1 = Stage()
            with pytest.raises(TypeError):
                s1.from_dict(stage_dict_copy)

            stage_dict_copy['name'] = data
            s1 = Stage()
            with pytest.raises(TypeError):
                s1.from_dict(stage_dict_copy)

            stage_dict_copy['state'] = data
            s1 = Stage()
            with pytest.raises(TypeError):
                s1.from_dict(stage_dict_copy)

            task_dict_copy['state_history'] = data
            s1 = Stage()
            with pytest.raises(TypeError):
                s1.from_dict(stage_dict_copy)

            task_dict_copy['parent_pipeline'] = data
            s1 = Stage()
            with pytest.raises(TypeError):
                s1.from_dict(stage_dict_copy)

        