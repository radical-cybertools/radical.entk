from radical.entk import Pipeline, Stage, Task
from radical.entk import states
from radical.entk.exceptions import *
import pytest, threading

def test_pipeline_initialization():

    """
    ***Purpose***: Test if pipeline attributes are correctly initialized upon creation
    """

    p = Pipeline()

    assert type(p._uid) == str
    assert type(p.stages) == list
    assert type(p.name) == str
    assert type(p.state) == str
    assert p.state == states.INITIAL
    assert p._stage_count == 0
    assert p._cur_stage == 0
    assert type(p._stage_lock) == type(threading.Lock())
    assert p._completed_flag.is_set() == False

def test_assignment_exceptions():

    """
    ***Purpose***: Test values that can be assigned to Pipeline attributes
    """

    p = Pipeline()

    data_type = [1,'a',True, [1], set([1])]

    for data in data_type:

        if not isinstance(data,str):
            with pytest.raises(TypeError):
                p.name = data

        with pytest.raises(TypeError):
            p.stages = data

        with pytest.raises(TypeError):
            p.add_stages(data)

        if not isinstance(data,str):
            with pytest.raises(TypeError):
                p.remove_stages(data)
   
def test_stage_assignment_in_pipeline():

    """
    ***Purpose***: Test stage assignment to Pipeline
    """

    p = Pipeline()
    s = Stage()
    p.stages = s

    assert type(p.stages) == list
    assert p._stage_count == 1
    assert p._cur_stage == 1
    assert p.stages[0] == s
    

def test_stage_addition_in_pipeline():

    """
    ***Purpose***: Test stage addition to Pipeline
    """

    p = Pipeline()
    s1 = Stage()
    s2 = Stage()
    p.add_stages([s1,s2])

    assert type(p.stages) == list
    assert p._stage_count == 2
    assert p._cur_stage == 1
    assert p.stages[0] == s1
    assert p.stages[1] == s2


def test_stage_removal_from_pipeline():

    """
    ***Purpose***: Test stage removal from Pipeline
    """

    p = Pipeline()
    s1 = Stage()
    s1.name = 's1'
    s2 = Stage()
    s2.name = 's2'
    s3 = Stage()
    s3.name = 's3'
    p.add_stages([s1,s2,s3])

    assert type(p.stages) == list
    assert p._stage_count == 3
    assert p._cur_stage == 1

    p.remove_stages('s2')
    assert p._stage_count == 2    
    assert p._cur_stage == 1

    p.remove_stages('s1')
    assert p._stage_count == 1
    assert p._cur_stage == 1
    assert p.stages[0] == s3

    p.remove_stages('s1')
    assert p._stage_count == 1
    assert p._cur_stage == 1
    assert p.stages[0] == s3

    p.remove_stages('s3')
    assert p._stage_count == 0
    assert p._cur_stage == 0


def test_pipeline_increment():

    """
    ***Purpose***: Test automatic increment of stage pointer in Pipeline
    """

    p = Pipeline()
    s1 = Stage()
    s2 = Stage()
    p.add_stages([s1,s2])

    assert p._stage_count == 2
    assert p._cur_stage == 1
    assert p._completed_flag.is_set() == False

    p._increment_stage()
    assert p._stage_count == 2
    assert p._cur_stage == 2
    assert p._completed_flag.is_set() == False

    p._increment_stage()
    assert p._stage_count == 2
    assert p._cur_stage == 2
    assert p._completed_flag.is_set() == True


def test_pipeline_decrement():

    """
    ***Purpose***: Test automatic decrement of stage pointer in Pipeline
    """

    p = Pipeline()
    s1 = Stage()
    s2 = Stage()
    p.add_stages([s1,s2])

    p._increment_stage()
    p._increment_stage()
    assert p._stage_count == 2
    assert p._cur_stage == 2
    assert p._completed_flag.is_set() == True

    p._decrement_stage()
    assert p._stage_count == 2
    assert p._cur_stage == 1
    assert p._completed_flag.is_set() == False
    
    p._decrement_stage()
    assert p._stage_count == 2
    assert p._cur_stage == 0
    assert p._completed_flag.is_set() == False


def test_uid_passing():

    """
    ***Purpose***: Test automatic uid assignments of all Stages of a Pipeline
    """

    p = Pipeline()
    s = Stage()
    p.stages    = s

    assert s._parent_pipeline == p.uid
