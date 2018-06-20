from radical.entk import Pipeline, Stage, Task
from radical.entk import states
from radical.entk.exceptions import *
import pytest
from hypothesis import given
import hypothesis.strategies as st

def test_task_to_dict():

    """
    **Purpose**: Test if the 'to_dict' function of Task class converts all expected attributes of the Task into a
    dictionary
    """

    t1 = Task()
    t1.name = 'simulation'
    t1.pre_exec = ['module load gromacs']
    t1.executable = ['grompp']
    t1.arguments = ['hello']
    t1.cpu_reqs = {
                    'processes': 4,
                    'process_type': 'MPI',
                    'threads_per_process': 1,
                    'thread_type': 'OpenMP'
                }
    t1.post_exec = ['echo test']
    
    t1.upload_input_data    = ['upload_input.dat']
    t1.copy_input_data      = ['copy_input.dat']
    t1.link_input_data      = ['link_input.dat']
    t1.copy_output_data     = ['copy_output.dat']
    t1.download_output_data = ['download_output.dat']

    task_dict = t1.to_dict()

    assert type(task_dict)                      == dict
    assert task_dict['uid']                     == t1._uid
    assert task_dict['name']                    == t1.name
    assert task_dict['pre_exec']                == t1.pre_exec
    assert task_dict['executable']              == t1.executable
    assert task_dict['arguments']               == t1.arguments
    assert task_dict['cpu_reqs']                == t1.cpu_reqs
    assert task_dict['gpu_reqs']                == t1.gpu_reqs
    assert task_dict['post_exec']               == t1.post_exec
    assert task_dict['upload_input_data']       == t1.upload_input_data
    assert task_dict['copy_input_data']         == t1.copy_input_data
    assert task_dict['link_input_data']         == t1.link_input_data
    assert task_dict['copy_output_data']        == t1.copy_output_data
    assert task_dict['download_output_data']    == t1.download_output_data


def test_task_from_dict():

    """
    **Purpose**: Test if the 'from_dict' function of Task class converts a dictionary into a Task correctly with all
    the expected attributes
    """

    t1 = Task()
    t1.name = 'simulation'
    t1.pre_exec = ['module load gromacs']
    t1.executable = ['grompp']
    t1.arguments = ['hello']
    t1.cpu_reqs = {
                    'processes': 4,
                    'process_type': 'MPI',
                    'threads_per_process': 1,
                    'thread_type': 'OpenMP'
                }
    t1.post_exec = ['echo test']
    
    t1.upload_input_data    = ['upload_input.dat']
    t1.copy_input_data      = ['copy_input.dat']
    t1.link_input_data      = ['link_input.dat']
    t1.copy_output_data     = ['copy_output.dat']
    t1.download_output_data = ['download_output.dat']

    t1._assign_uid('sid')

    p = Pipeline()
    s = Stage()
    s.tasks = t1
    p.stages = s

    task_dict = t1.to_dict()
    t2 = Task()
    t2.from_dict(task_dict)

    assert type(task_dict)                      == dict
    assert task_dict['uid']                     == t2._uid
    assert task_dict['name']                    == t2.name
    assert task_dict['pre_exec']                == t2.pre_exec
    assert task_dict['executable']              == t2.executable
    assert task_dict['arguments']               == t2.arguments
    assert task_dict['cpu_reqs']                == t2.cpu_reqs
    assert task_dict['gpu_reqs']                == t2.gpu_reqs
    assert task_dict['post_exec']               == t2.post_exec
    assert task_dict['upload_input_data']       == t2.upload_input_data
    assert task_dict['copy_input_data']         == t2.copy_input_data
    assert task_dict['link_input_data']         == t2.link_input_data
    assert task_dict['copy_output_data']        == t2.copy_output_data
    assert task_dict['download_output_data']    == t2.download_output_data
    assert task_dict['exit_code']               == t2.exit_code
    assert task_dict['path']                    == t2.path
    assert task_dict['parent_stage']            == t2.parent_stage
    assert task_dict['parent_pipeline']         == t2.parent_pipeline


@given(s=st.text(), l=st.lists(st.text()), i=st.integers().filter(lambda x: type(x) == int), b=st.booleans())
def test_task_from_dict_exceptions(s,l,i,b):
    ## Check exceptions

    t = Task()
    t.name = 'simulation'
    t.pre_exec = ['module load gromacs']
    t.executable = ['grompp']
    t.arguments = ['hello']
    t.cores = 4
    t.mpi = True
    t.post_exec = ['echo test']
    
    t.upload_input_data    = ['upload_input.dat']
    t.copy_input_data      = ['copy_input.dat']
    t.link_input_data      = ['link_input.dat']
    t.copy_output_data     = ['copy_output.dat']
    t.download_output_data = ['download_output.dat']

    t._assign_uid('sid')

    task_dict = t.to_dict()
    
    data_type = [s,l,i,b]

    for data in data_type:

        task_dict_copy = task_dict

        if not isinstance(data,str):
            task_dict_copy['state'] = data
            t1 = Task()
            with pytest.raises(TypeError):
                t1.from_dict(task_dict_copy)

            task_dict_copy['path'] = data
            t1 = Task()
            with pytest.raises(TypeError):
                t1.from_dict(task_dict_copy)

            task_dict_copy['parent_stage'] = data
            t1 = Task()
            with pytest.raises(TypeError):
                t1.from_dict(task_dict_copy) 

            task_dict_copy['parent_pipeline'] = data
            t1 = Task()
            with pytest.raises(TypeError):
                t1.from_dict(task_dict_copy)

        if not isinstance(data, list):
            task_dict_copy['state_history'] = data
            t1 = Task()
            with pytest.raises(TypeError):
                t1.from_dict(task_dict_copy) 

            task_dict_copy['pre_exec'] = data
            t1 = Task()
            with pytest.raises(TypeError):
                t1.from_dict(task_dict_copy) 

            task_dict_copy['executable'] = data
            t1 = Task()
            with pytest.raises(TypeError):
                t1.from_dict(task_dict_copy) 

            task_dict_copy['arguments'] = data
            t1 = Task()
            with pytest.raises(TypeError):
                t1.from_dict(task_dict_copy) 

            task_dict_copy['post_exec'] = data
            t1 = Task()
            with pytest.raises(TypeError):
                t1.from_dict(task_dict_copy) 

            task_dict_copy['upload_input_data'] = data
            t1 = Task()
            with pytest.raises(TypeError):
                t1.from_dict(task_dict_copy) 

            task_dict_copy['copy_input_data'] = data
            t1 = Task()
            with pytest.raises(TypeError):
                t1.from_dict(task_dict_copy) 

            task_dict_copy['link_input_data'] = data
            t1 = Task()
            with pytest.raises(TypeError):
                t1.from_dict(task_dict_copy)

            task_dict_copy['copy_output_data'] = data
            t1 = Task()
            with pytest.raises(TypeError):
                t1.from_dict(task_dict_copy) 

            task_dict_copy['download_output_data'] = data
            t1 = Task()
            with pytest.raises(TypeError):
                t1.from_dict(task_dict_copy) 

            task_dict_copy['cores'] = data
            t1 = Task()
            with pytest.raises(TypeError):
                t1.from_dict(task_dict_copy) 

            task_dict_copy['exit_code'] = data
            t1 = Task()
            with pytest.raises(TypeError):
                t1.from_dict(task_dict_copy) 

        if not isinstance(data, bool):
            task_dict_copy['mpi'] = data
            t1 = Task()
            with pytest.raises(TypeError):
                t1.from_dict(task_dict_copy) 


