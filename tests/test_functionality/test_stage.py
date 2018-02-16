from radical.entk import Task, Stage, Pipeline
from radical.entk import states
from radical.entk.exceptions import *
import pytest
from hypothesis import strategies as st, given


# ------------------------------------------------------------------------------------------------------------------
# Public methods
# ------------------------------------------------------------------------------------------------------------------

def test_add_tasks():

    s = Stage()

    t = Task()

    with pytest.raises(MissingError):
        s.tasks = t


    s = Stage()
    t_uids = []
    for i in range(5):
        t = Task()
        t.executable = 'date'

        s.add_tasks(t)
        t_uids.append(t.uid)

    assert len(s.tasks) == 5
    assert set([t.uid for t in s.tasks]) == set(t_uids)


    s = Stage()
    t_uids = []
    tasks = list()
    for i in range(5):
        t = Task()
        t.executable = 'date'
        tasks.append(t)
        t_uids.append(t.uid)

    s.tasks = tasks
    s.tasks = set(tasks)

    assert len(s.tasks) == 5
    assert set([t.uid for t in s.tasks]) == set(t_uids)

def test_stage_to_dict():

    p = Pipeline()

    s = Stage()
    s.name = 'temp-stage'    

    t = Task()
    t.executable = 'date'

    
    s.tasks = t
    p.stages = s

    stage_dict = s.to_dict()

    print s.parent_pipeline

    assert stage_dict['uid']                == s.uid
    assert stage_dict['name']               == s.name
    assert stage_dict['state']              == s.state
    assert stage_dict['state_history']      == s.state_history
    assert stage_dict['parent_pipeline']    == p.uid



def test_stage_from_dict():

    p = Pipeline()

    s = Stage()
    s.name = 'temp-stage'    

    t = Task()
    t.executable = 'date'

    
    s.tasks = t
    p.stages = s

    stage_dict = s.to_dict()


    s1 = Stage(duplicate=True)
    s1.from_dict(stage_dict)

    assert s1.uid              == stage_dict['uid']            
    assert s1.name             == stage_dict['name']           
    assert s1.state            == stage_dict['state']          
    assert s1.state_history    == stage_dict['state_history']  
    assert s1.parent_pipeline  == stage_dict['parent_pipeline']

# ------------------------------------------------------------------------------------------------------------------
# Private methods
# ------------------------------------------------------------------------------------------------------------------

def test_pass_uid():

    """
    ***Purpose***: Test automatic uid assignments of all Stages and Tasks of a Pipeline
    """

    p = Pipeline()
    s = Stage()
    t = Task()
    t.executable = 'date'

    s.tasks = t
    p.stages = s

    assert s.parent_pipeline == p.uid
    assert t.parent_pipeline == p.uid
    assert t.parent_stage == s.uid

def test_set_tasks_state():

    """
    ***Purpose***: Test method to set state of all Tasks of a Stage
    """

    s = Stage()
    t1 = Task()
    t1.executable = 'date'
    t2 = Task()
    t2.executable = 'date'
    s.add_tasks([t1,t2])

    with pytest.raises(ValueError):
        s._set_tasks_state(2)

    with pytest.raises(ValueError):
        s._set_tasks_state('RANDOM')

    s._set_tasks_state(states.DONE)
    assert t1.state == states.DONE
    assert t2.state == states.DONE

def test_check_stage_complete():
    """
    ***Purpose***: Test method to test if all Tasks of a Stage are DONE and hence the Stage is DONE
    """

    s = Stage()
    t1 = Task()
    t1.executable = 'date'
    t2 = Task()
    t2.executable = 'date'
    s.add_tasks([t1,t2])

    assert s._check_stage_complete() == False
    s._set_tasks_state(states.DONE)
    assert s._check_stage_complete() == True

@given( txt=st.text(), 
        l=st.lists(st.text()), 
        i=st.integers().filter(lambda x: type(x) == int), 
        b=st.booleans())
def test_validate_tasks(txt,l,i,b):
    
    t = Task()
    t.executable = 'date'

    s = Stage()

    assert isinstance(s._validate_tasks(t), set)

    for d in [txt,l,i,b]:

        if type(d) == list or type(d) == set:
            with pytest.raises(TypeError):
                s._validate_tasks(d)
        else:
            with pytest.raises(TypeError):
                s._validate_tasks(d)

def test_validate():

    s = Stage()

    with pytest.raises(ValueError):
        s.state = 'RANDOM'
        t = Task()
        t.executable = 'date'
        s._validate()

    with pytest.raises(MissingError):
        s.state = states.INITIAL
        s._validate()