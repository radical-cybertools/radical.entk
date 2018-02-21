from radical.entk import Pipeline, Stage, Task
from radical.entk import states
from radical.entk.exceptions import *
import pytest
from hypothesis import given
import hypothesis.strategies as st
import threading

def test_initialization():

    """
    ***Purpose***: Test if pipeline attributes are correctly initialized upon creation
    """

    p = Pipeline()

    assert type(p._uid) == str
    assert p.stages == list()
    assert p.name == str()
    assert p.state == states.INITIAL
    assert p._stage_count == 0
    assert p._cur_stage == 0
    assert type(p._stage_lock) == type(threading.Lock())
    assert type(p._completed_flag) == type(threading.Event())
    assert p._completed_flag.is_set() == False


@given(t=st.text(),
       l=st.lists(st.text()),
       i=st.integers().filter(lambda x: type(x) == int),
       b=st.booleans(),
       se=st.sets(st.text()))
def test_assignment_exceptions(t,l,i,b,se):

    """
    ***Purpose***: Test values that can be assigned to Pipeline attributes
    """

    p = Pipeline()

    data_type = [t,l,i,b,se]

    for data in data_type:

        print 'Using: %s, %s'%(data, type(data))

        if not isinstance(data,str):
            with pytest.raises(TypeError):
                p.name = data

        with pytest.raises(TypeError):
            p.stages = data

        with pytest.raises(TypeError):
            p.add_stages(data)