from radical.entk import Pipeline, Stage, Task
from radical.entk import states
from radical.entk.exceptions import *
import pytest
from hypothesis import given
import hypothesis.strategies as st

def test_initialization():

    """
    **Purpose**: Test if the task attributes have, thus expect, the correct data types
    """

    t = Task()
    assert t._uid == None
    assert t.name == None
    assert t.state == states.INITIAL
    assert t.pre_exec == list()
    assert t.executable == list()
    assert t.arguments == list()
    assert t.post_exec == list()
    assert t.cpu_reqs['processes'] == 1
    assert t.cpu_reqs['process_type'] == None
    assert t.cpu_reqs['threads_per_process'] == 1
    assert t.cpu_reqs['thread_type'] == None
    assert t.gpu_reqs['processes'] == 0
    assert t.gpu_reqs['process_type'] == None
    assert t.gpu_reqs['threads_per_process'] == 0
    assert t.gpu_reqs['thread_type'] == None
    assert t.upload_input_data == list()
    assert t.copy_input_data == list()
    assert t.link_input_data == list()
    assert t.copy_output_data == list()
    assert t.download_output_data == list()
    assert t.exit_code == None
    assert t.path == None
    assert t.state_history == [states.INITIAL]
    assert t.parent_pipeline['uid'] == None
    assert t.parent_pipeline['name'] == None
    assert t.parent_stage['uid'] == None
    assert t.parent_stage['name'] == None

def test_task_uid():

    t = Task()
    t._assign_uid('temp')
    assert isinstance(t.uid, str)

@given(s=st.text(), l=st.lists(st.text()), i=st.integers().filter(lambda x: type(x) == int), b=st.booleans())
def test_task_exceptions(s,l,i,b):

    """
    **Purpose**: Test if all attribute assignments raise exceptions for invalid values
    """

    t = Task()

    data_type = [s,l,i,b]

    for data in data_type:


        if not isinstance(data,str):
            with pytest.raises(TypeError):
                t.name = data

            with pytest.raises(TypeError):
                t.path = data

            with pytest.raises(TypeError):
                t.parent_stage = data

            with pytest.raises(TypeError):
                t.parent_pipeline = data

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

        if not isinstance(data, str) and not isinstance(data, unicode):

            with pytest.raises(ValueError):
                t.cpu_reqs = {
                                'processes': 1,
                                'process_type': data,
                                'threads_per_process': 1,
                                'thread_type': None
                            }
                t.cpu_reqs = {
                                'processes': 1,
                                'process_type': None,
                                'threads_per_process': 1,
                                'thread_type': data
                            }
                t.gpu_reqs = {
                                'processes': 1,
                                'process_type': data,
                                'threads_per_process': 1,
                                'thread_type': None
                            }
                t.gpu_reqs = {
                                'processes': 1,
                                'process_type': None,
                                'threads_per_process': 1,
                                'thread_type': data
                            }
                

        if not isinstance(data, int):

            with pytest.raises(TypeError):
                t.cpu_reqs = {
                                'processes': data,
                                'process_type': None,
                                'threads_per_process': 1,
                                'thread_type': None
                            }
                t.cpu_reqs = {
                                'processes': 1,
                                'process_type': None,
                                'threads_per_process': data,
                                'thread_type': None
                            }
                t.gpu_reqs = {
                                'processes': data,
                                'process_type': None,
                                'threads_per_process': 1,
                                'thread_type': None
                            }
                t.gpu_reqs = {
                                'processes': 1,
                                'process_type': None,
                                'threads_per_process': data,
                                'thread_type': None
                            }

