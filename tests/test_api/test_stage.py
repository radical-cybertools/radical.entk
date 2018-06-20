from radical.entk import Pipeline, Stage, Task
from radical.entk import states
from radical.entk.exceptions import *
import pytest
from hypothesis import given
import hypothesis.strategies as st


def test_initialization():
    """
    ***Purpose***: Test if all attributes have, thus expect, the correct data types
    """

    s = Stage()

    assert s.uid == None
    assert s.name == None
    assert s.tasks == set()    
    assert s.state == states.INITIAL
    assert s._task_count == 0
    assert s.parent_pipeline['uid'] == None
    assert s.parent_pipeline['name'] == None


def test_stage_uid():

    s = Stage()
    s._assign_uid('temp')
    assert isinstance(s.uid, str)

@given(t=st.text(),
       l=st.lists(st.text()),
       i=st.integers().filter(lambda x: type(x) == int),
       b=st.booleans(),
       se=st.sets(st.text()))
def test_stage_exceptions(t, l, i, b, se):
    """
    ***Purpose***: Test if correct exceptions are raised when attributes are assigned unacceptable values.
    """

    s = Stage()

    data_type = [t,l,i,b,se]

    for data in data_type:

        print 'Using: %s, %s'%(data, type(data))

        if not isinstance(data, str):
            with pytest.raises(TypeError):
                s.name = data

        with pytest.raises(TypeError):
            s.tasks = data

        with pytest.raises(TypeError):
            s.add_tasks(data)
