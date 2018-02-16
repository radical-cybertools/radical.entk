from radical.entk import Task, Stage, Pipeline
from radical.entk import states
from radical.entk.exceptions import *
import pytest

# ------------------------------------------------------------------------------------------------------------------
# Public methods
# ------------------------------------------------------------------------------------------------------------------

def test_task_to_dict():
    """
    **Purpose**: Test if the 'to_dict' function of Task class converts all 
    expected attributes of the Task into a dictionary
    """

    t1 = Task()

    t1.name = 'simulation'
    t1.pre_exec = ['module load gromacs']
    t1.executable = ['grompp']
    t1.arguments = ['hello']
    t1.cores = 4
    t1.mpi = True
    t1.post_exec = ['echo test']

    t1.upload_input_data = ['upload_input.dat']
    t1.copy_input_data = ['copy_input.dat']
    t1.link_input_data = ['link_input.dat']
    t1.copy_output_data = ['copy_output.dat']
    t1.download_output_data = ['download_output.dat']

    t1.path = 'some_path'
    t1.exit_code = 1

    p = Pipeline()
    s = Stage()
    s.tasks = t1
    p.stages = s

    task_dict = t1.to_dict()

    assert type(task_dict) == dict
    assert task_dict['uid'] == t1.uid
    assert task_dict['name'] == t1.name
    assert task_dict['pre_exec'] == t1.pre_exec
    assert task_dict['executable'] == t1.executable
    assert task_dict['arguments'] == t1.arguments
    assert task_dict['cores'] == t1.cores
    assert task_dict['mpi'] == t1.mpi
    assert task_dict['post_exec'] == t1.post_exec
    assert task_dict['upload_input_data'] == t1.upload_input_data
    assert task_dict['copy_input_data'] == t1.copy_input_data
    assert task_dict['link_input_data'] == t1.link_input_data
    assert task_dict['copy_output_data'] == t1.copy_output_data
    assert task_dict['download_output_data'] == t1.download_output_data
    assert task_dict['path'] == t1.path
    assert task_dict['exit_code'] == t1.exit_code
    assert task_dict['parent_stage'] == s.uid
    assert task_dict['parent_pipeline'] == p.uid


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
    t1.cores = 4
    t1.mpi = True
    t1.post_exec = ['echo test']

    t1.upload_input_data = ['upload_input.dat']
    t1.copy_input_data = ['copy_input.dat']
    t1.link_input_data = ['link_input.dat']
    t1.copy_output_data = ['copy_output.dat']
    t1.download_output_data = ['download_output.dat']

    t1.path = 'some_path'
    t1.exit_code = 1

    p = Pipeline()
    s = Stage()
    s.tasks = t1
    p.stages = s

    task_dict = t1.to_dict()
    t2 = Task()
    t2.from_dict(task_dict)

    assert type(task_dict) == dict
    assert task_dict['uid'] == t2.uid
    assert task_dict['name'] == t2.name
    assert task_dict['pre_exec'] == t2.pre_exec
    assert task_dict['executable'] == t2.executable
    assert task_dict['arguments'] == t2.arguments
    assert task_dict['cores'] == t2.cores
    assert task_dict['mpi'] == t2.mpi
    assert task_dict['post_exec'] == t2.post_exec
    assert task_dict['upload_input_data'] == t2.upload_input_data
    assert task_dict['copy_input_data'] == t2.copy_input_data
    assert task_dict['link_input_data'] == t2.link_input_data
    assert task_dict['copy_output_data'] == t2.copy_output_data
    assert task_dict['download_output_data'] == t2.download_output_data
    assert task_dict['path'] == t2.path
    assert task_dict['exit_code'] == t2.exit_code
    assert task_dict['parent_stage'] == s.uid
    assert task_dict['parent_pipeline'] == p.uid


# ------------------------------------------------------------------------------------------------------------------
# Private methods
# ------------------------------------------------------------------------------------------------------------------

def test_task_validate():

    with pytest.raises(ValueError):

        t1 = Task()
        t1._state = 'DONE'
        t1._validate()

    with pytest.raises(MissingError):

        t1 = Task()
        t1.executable = list()
        t1._validate()

    with pytest.raises(MissingError):

        t1 = Task()
        t1._validate()