from radical.entk import Stage
from radical.entk import Task
from radical.entk import states
from radical.entk.exceptions import *
import pytest

def test_attribute_types():

    s = Stage()

    assert type(s.name) == str
    assert type(s.state) == str
    assert type(s.tasks) == set
    


def test_assignment_exceptions():

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
                s._set_task_state(data)

def test_init_state():
    s = Stage()
    assert s.state == states.NEW
