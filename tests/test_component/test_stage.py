
import pytest

from   hypothesis import given, settings
import hypothesis.strategies as st

from radical.entk import Pipeline, Stage, Task
from radical.entk import states
from radical.entk.exceptions import *


# ------------------------------------------------------------------------------
#

# Hypothesis settings
settings.register_profile("travis", max_examples=100, deadline=None)
settings.load_profile("travis")

def test_stage_initialization():
    """
    ***Purpose***: Test if all attributes have, thus expect, the
    correct data types
    """

    s = Stage()

    assert s.uid == None
    assert s.name == None
    assert s.tasks == set()
    assert s.state == states.INITIAL
    assert s.state_history == [states.INITIAL]
    assert s._task_count == 0
    assert s.parent_pipeline['uid'] == None
    assert s.parent_pipeline['name'] == None
    assert s.post_exec == None


# ------------------------------------------------------------------------------
#
@given(t=st.text(),
       l=st.lists(st.text()),
       i=st.integers().filter(lambda x: type(x) == int),
       b=st.booleans(),
       se=st.sets(st.text()))
def test_stage_exceptions(t, l, i, b, se):
    """
    ***Purpose***: Test if correct exceptions are raised when attributes are
    assigned unacceptable values.
    """

    s = Stage()

    data_type = [t, l, i, b, se]

    for data in data_type:

        print 'Using: %s, %s' % (data, type(data))

        if not isinstance(data, str):
            with pytest.raises(TypeError):
                s.name = data

        with pytest.raises(TypeError):
            s.tasks = data

        with pytest.raises(TypeError):
            s.add_tasks(data)


# ------------------------------------------------------------------------------
#
def test_stage_task_assignment():
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


# ------------------------------------------------------------------------------
#
@given(l=st.lists(st.text()),
       i=st.integers().filter(lambda x: type(x) == int),
       b=st.booleans())
def test_stage_parent_pipeline_assignment(l, i, b):

    s = Stage()
    data_type = [l, i, b]
    for data in data_type:
        with pytest.raises(TypeError):
            s.parent_pipeline = data


# ------------------------------------------------------------------------------
#
@given(t=st.text(),
       l=st.lists(st.text()),
       i=st.integers().filter(lambda x: type(x) == int),
       b=st.booleans())
def test_stage_state_assignment(t, l, i, b):

    s = Stage()

    data_type = [l, i, b]

    for data in data_type:
        with pytest.raises(TypeError):
            s.state = data

    if isinstance(t, str):
        with pytest.raises(ValueError):
            s.state = t

    for val in states._stage_state_values.keys():
        s.state = val


# ------------------------------------------------------------------------------
#
@given(l=st.lists(st.text()),
       d=st.dictionaries(st.text(), st.text()))
def test_stage_post_exec_assignment(l, d):

    s = Stage()

    def func():
        return True

    with pytest.raises(TypeError):
        s.post_exec = l

    with pytest.raises(TypeError):
        s.post_exec = d


    s.post_exec = func

    class Tmp(object):

        def func(self):
            return True


    tmp = Tmp()
    s.post_exec = tmp.func


# ------------------------------------------------------------------------------
#
def test_stage_task_addition():

    s = Stage()
    t1 = Task()
    t1.executable = ['/bin/date']
    t2 = Task()
    t2.executable = ['/bin/date']
    s.add_tasks(set([t1, t2]))

    assert type(s.tasks) == set
    assert s._task_count == 2
    assert t1 in s.tasks
    assert t2 in s.tasks

    s = Stage()
    t1 = Task()
    t1.executable = ['/bin/date']
    t2 = Task()
    t2.executable = ['/bin/date']
    s.add_tasks([t1, t2])

    assert type(s.tasks) == set
    assert s._task_count == 2
    assert t1 in s.tasks
    assert t2 in s.tasks


# ------------------------------------------------------------------------------
#
def test_stage_to_dict():

    s = Stage()
    d = s.to_dict()

    assert d == {'uid': None,
                 'name': None,
                 'state': states.INITIAL,
                 'state_history': [states.INITIAL],
                 'parent_pipeline': {'uid': None, 'name': None}}


# ------------------------------------------------------------------------------
#
def test_stage_from_dict():

    d = {'uid': 're.Stage.0000',
         'name': 's1',
         'state': states.DONE,
         'state_history': [states.INITIAL, states.DONE],
         'parent_pipeline': {'uid': 'p1',
                             'name': 'pipe1'}
         }

    s = Stage()
    s.from_dict(d)

    assert s.uid == d['uid']
    assert s.name == d['name']
    assert s.state == d['state']
    assert s.state_history == d['state_history']
    assert s.parent_pipeline == d['parent_pipeline']


# ------------------------------------------------------------------------------
#
def test_stage_set_tasks_state():

    s = Stage()
    t1 = Task()
    t1.executable = ['/bin/date']
    t2 = Task()
    t2.executable = ['/bin/date']
    s.add_tasks([t1, t2])

    with pytest.raises(ValueError):
        s._set_tasks_state(2)

    s._set_tasks_state(states.DONE)
    assert t1.state == states.DONE
    assert t2.state == states.DONE


# ------------------------------------------------------------------------------
#
def test_stage_check_complete():

    s = Stage()
    t1 = Task()
    t1.executable = ['/bin/date']
    t2 = Task()
    t2.executable = ['/bin/date']
    s.add_tasks([t1, t2])

    assert s._check_stage_complete() == False
    s._set_tasks_state(states.DONE)
    assert s._check_stage_complete() == True


# ------------------------------------------------------------------------------
#
@given(t=st.text(),
       l=st.lists(st.text()),
       i=st.integers().filter(lambda x: type(x) == int),
       b=st.booleans(),
       se=st.sets(st.text()))
def test_stage_validate_entities(t, l, i, b, se):

    s = Stage()

    data_type = [t, l, i, b, se]

    for data in data_type:
        with pytest.raises(TypeError):
            s._validate_entities(data)

    t = Task()
    assert isinstance(s._validate_entities(t), set)

    t1 = Task()
    t2 = Task()
    assert set([t1, t2]) == s._validate_entities([t1, t2])


# ------------------------------------------------------------------------------
#
def test_stage_validate():

    s = Stage()
    s._state = 'test'
    with pytest.raises(ValueError):
        s._validate()

    s = Stage()
    with pytest.raises(MissingError):
        s._validate()


# ------------------------------------------------------------------------------
#
def test_stage_assign_uid():

    s = Stage()
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
    s._assign_uid('test')
    assert s.uid == 'stage.0000'


# ------------------------------------------------------------------------------
#
def test_stage_pass_uid():

    s = Stage()
    s._uid = 's'
    s.name = 's1'
    s.parent_pipeline['uid'] = 'p'
    s.parent_pipeline['name'] = 'p1'

    t1 = Task()
    t2 = Task()
    s.add_tasks([t1,t2])

    s._pass_uid()

    assert t1.parent_stage['uid'] == s.uid
    assert t1.parent_stage['name'] == s.name
    assert t1.parent_pipeline['uid'] == s.parent_pipeline['uid']
    assert t1.parent_pipeline['name'] == s.parent_pipeline['name']

    assert t2.parent_stage['uid'] == s.uid
    assert t2.parent_stage['name'] == s.name
    assert t2.parent_pipeline['uid'] == s.parent_pipeline['uid']
    assert t2.parent_pipeline['name'] == s.parent_pipeline['name']


# ------------------------------------------------------------------------------

