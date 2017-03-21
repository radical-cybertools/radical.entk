from radical.entk import Pipeline
from radical.entk import states
from radical.entk.exceptions import *
import pytest

def test_attribute_types():

    p = Pipeline()

    assert type(p.name) == str
    assert type(p.state) == str
    assert type(p.stages) == list
    

def test_assignment_exceptions():

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
