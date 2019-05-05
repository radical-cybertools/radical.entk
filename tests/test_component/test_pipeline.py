from radical.entk import Pipeline, Stage, Task
from radical.entk import states
from radical.entk.exceptions import *
import pytest
from hypothesis import given, settings
import hypothesis.strategies as st
import threading

# Hypothesis settings
settings.register_profile("travis", max_examples=100, deadline=None)
settings.load_profile("travis")

def test_pipeline_initialization():

    p = Pipeline()

    assert p.uid == None
    assert p.name == None
    assert p.stages == list()
    assert p.state == states.INITIAL
    assert p.state_history == [states.INITIAL]
    assert p._stage_count == 0
    assert p.current_stage == 0
    assert type(p.lock) == type(threading.Lock())
    assert type(p._completed_flag) == type(threading.Event())
    assert p.completed == False


@given(t=st.text(),
       l=st.lists(st.text()),
       i=st.integers().filter(lambda x: type(x) == int),
       b=st.booleans(),
       se=st.sets(st.text()))
def test_pipeline_assignment_exceptions(t, l, i, b, se):

    p = Pipeline()

    data_type = [t, l, i, b, se]

    for data in data_type:

        if not isinstance(data, str):
            with pytest.raises(TypeError):
                p.name = data

        with pytest.raises(TypeError):
            p.stages = data

        with pytest.raises(TypeError):
            p.add_stages(data)


def test_pipeline_stage_assignment():

    p = Pipeline()
    s = Stage()
    t = Task()
    t.executable = ['/bin/date']
    s.tasks = t
    p.stages = s

    assert type(p.stages) == list
    assert p._stage_count == 1
    assert p._cur_stage == 1
    assert p.stages[0] == s


@given(t=st.text(),
       l=st.lists(st.text()),
       i=st.integers().filter(lambda x: type(x) == int),
       b=st.booleans())
def test_pipeline_state_assignment(t, l, i, b):

    p = Pipeline()

    data_type = [l, i, b]

    for data in data_type:
        with pytest.raises(TypeError):
            p.state = data

    if isinstance(t,str):
        with pytest.raises(ValueError):
            p.state = t

    for val in states._pipeline_state_values.keys():
        p.state = val


def test_pipeline_stage_addition():

    p = Pipeline()
    s1 = Stage()
    t = Task()
    t.executable = ['/bin/date']
    s1.tasks = t
    s2 = Stage()
    t = Task()
    t.executable = ['/bin/date']
    s2.tasks = t
    p.add_stages([s1, s2])

    assert type(p.stages) == list
    assert p._stage_count == 2
    assert p._cur_stage == 1
    assert p.stages[0] == s1
    assert p.stages[1] == s2


def test_pipeline_to_dict():

    p = Pipeline()
    d = p.to_dict()
    assert d == {'uid': None,
                 'name': None,
                 'state': states.INITIAL,
                 'state_history': [states.INITIAL],
                 'completed': False}


def test_pipeline_from_dict():

    d = {'uid': 're.Pipeline.0000',
         'name': 'p1',
         'state': states.DONE,
         'state_history': [states.INITIAL, states.DONE],
         'completed': True}

    p = Pipeline()
    p.from_dict(d)

    assert p.uid == d['uid']
    assert p.name == d['name']
    assert p.state == d['state']
    assert p.state_history == d['state_history']
    assert p.completed == d['completed']


def test_pipeline_increment_stage():

    p = Pipeline()
    s1 = Stage()
    t = Task()
    t.executable = ['/bin/date']
    s1.tasks = t
    s2 = Stage()
    t = Task()
    t.executable = ['/bin/date']
    s2.tasks = t
    p.add_stages([s1, s2])

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


def test_pipeline_decrement_stage():

    p = Pipeline()
    s1 = Stage()
    t = Task()
    t.executable = ['/bin/date']
    s1.tasks = t
    s2 = Stage()
    t = Task()
    t.executable = ['/bin/date']
    s2.tasks = t
    p.add_stages([s1, s2])

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


@given(t=st.text(),
       l=st.lists(st.text()),
       i=st.integers().filter(lambda x: type(x) == int),
       b=st.booleans(),
       se=st.sets(st.text()))
def test_pipeline_validate_entities(t, l, i, b, se):

    p = Pipeline()

    data_type = [t, l, i, b, se]

    for data in data_type:
        with pytest.raises(TypeError):
            p._validate_entities(data)

    s = Stage()
    assert isinstance(p._validate_entities(s), list)

    s1 = Stage()
    s2 = Stage()
    assert [s1,s2] == p._validate_entities([s1,s2])



def test_pipeline_validate():

    p = Pipeline()
    p._state = 'test'
    with pytest.raises(ValueError):
        p._validate()

    p = Pipeline()
    with pytest.raises(MissingError):
        p._validate()


def test_pipeline_assign_uid():

    p = Pipeline()
    try:
        import glob
        import shutil
        import os
        home = os.environ.get('HOME','/home')
        test_fold = glob.glob('%s/.radical/utils/test*'%home)
        for f in test_fold:
            shutil.rmtree(f)
    except:
        pass
    p._assign_uid('test')
    assert p.uid == 'pipeline.0000'


def test_pipeline_pass_uid():

    p = Pipeline()
    p._uid = 'test'
    p.name = 'p1'

    s1 = Stage()
    s2 = Stage()
    p.add_stages([s1,s2])

    p._pass_uid()

    assert s1.parent_pipeline['uid'] == p.uid
    assert s1.parent_pipeline['name'] == p.name
    assert s2.parent_pipeline['uid'] == p.uid
    assert s2.parent_pipeline['name'] == p.name

def test_pipeline_suspend_resume():

    p = Pipeline()
    assert p.state == states.INITIAL
    p.suspend()
    assert p.state == states.SUSPENDED
    p.resume()
    assert p.state == states.INITIAL


    p.state = states.SCHEDULING
    assert p.state == states.SCHEDULING
    p.suspend()
    assert p.state == states.SUSPENDED
    p.resume()
    assert p.state == states.SCHEDULING

    with pytest.raises(EnTKError):
        p.resume()

    p.suspend()

    with pytest.raises(EnTKError):
        p.suspend()
