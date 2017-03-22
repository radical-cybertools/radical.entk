from radical.entk import Task
from radical.entk import states
from radical.entk.exceptions import *
import pytest

def test_attribute_types():

    t = Task()

    assert type(t.name) == str
    assert type(t.state) == str
    assert type(t.pre_exec) == list
    assert type(t.executable) == list
    assert type(t.arguments) == list
    assert type(t.post_exec) == list
    assert type(t.upload_input_data) == list
    assert type(t.copy_input_data) == list
    assert type(t.link_input_data) == list
    assert type(t.copy_output_data) == list
    assert type(t.download_output_data) == list


def test_assignment_exceptions():

    t = Task()

    data_type = [1,'a',True, list()]

    for data in data_type:


        if not isinstance(data,str):
            with pytest.raises(TypeError):
                t.name = data

        if not isinstance(data,list):

            with pytest.raises(TypeError):
                t.pre_exec = data

            with pytest.raises(TypeError):
                t.executable = data

            with pytest.raises(TypeError):
                t.arguments = data

            with pytest.raises(TypeError):
                t.post_exec = data

            with pytest.raises(TypeError):
                t.upload_input_data = data

            with pytest.raises(TypeError):
                t.copy_input_data = data

            with pytest.raises(TypeError):
                t.link_input_data = data

            with pytest.raises(TypeError):
                t.copy_output_data = data

            with pytest.raises(TypeError):
                t.download_output_data = data


def test_init_state():
    t = Task()
    assert t.state == states.NEW
