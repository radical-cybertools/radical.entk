
import os
import pytest

import radical.utils as ru
import radical.pilot as rp

from radical.entk.execman.rp.task_processor import get_input_list_from_task
from radical.entk.execman.rp.task_processor import get_output_list_from_task
from radical.entk.execman.rp.task_processor import create_cud_from_task
from radical.entk.execman.rp.task_processor import create_task_from_cu
from radical.entk.execman.rp.task_processor import resolve_placeholders

from radical.entk.exceptions                import TypeError, ValueError
from radical.entk                           import Task, Stage, Pipeline


# ------------------------------------------------------------------------------
#
def test_input_list_from_task():
    """
    **Purpose**: Test if the 'get_input_list_from_task' function generates the
                 correct RP input transfer directives when given a Task.
    """

    pipeline = str(ru.generate_id('pipeline'))
    stage    = str(ru.generate_id('stage'))
    task     = str(ru.generate_id('task'))

    placeholders = {
        pipeline: {
            stage: {
                task: '/home/vivek/some_file.txt'
            }
        }
    }

    for t in [1, 'a', list(), dict(), True]:
        with pytest.raises(TypeError):
            t = list()
            get_input_list_from_task(t, placeholders)

    # Test link input data
    t = Task()
    t.link_input_data = ['/home/vivek/test.dat']
    ip_list = get_input_list_from_task(t, placeholders)

    assert ip_list[0]['action'] == rp.LINK
    assert ip_list[0]['source'] == t.link_input_data[0]
    assert ip_list[0]['target'] == os.path.basename(t.link_input_data[0])

    t = Task()
    t.link_input_data = ['/home/vivek/test.dat > new_test.dat']
    ip_list = get_input_list_from_task(t, placeholders)

    assert ip_list[0]['action'] == rp.LINK
    assert ip_list[0]['source'] == t.link_input_data[0].split('>')[0].strip()
    assert ip_list[0]['target'] == os.path.basename(t.link_input_data[0]
                                                     .split('>')[1].strip())

    # Test copy input data
    t = Task()
    t.copy_input_data = ['/home/vivek/test.dat']
    ip_list = get_input_list_from_task(t, placeholders)

    assert ip_list[0]['action'] == rp.COPY
    assert ip_list[0]['source'] == t.copy_input_data[0]
    assert ip_list[0]['target'] == os.path.basename(t.copy_input_data[0])

    t = Task()
    t.copy_input_data = ['/home/vivek/test.dat > new_test.dat']
    ip_list = get_input_list_from_task(t, placeholders)

    assert ip_list[0]['action'] == rp.COPY
    assert ip_list[0]['source'] == t.copy_input_data[0].split('>')[0].strip()
    assert ip_list[0]['target'] == os.path.basename(t.copy_input_data[0]
                                                     .split('>')[1].strip())


    # Test move input data
    t = Task()
    t.move_input_data = ['/home/vivek/test.dat']
    ip_list = get_input_list_from_task(t, placeholders)

    assert ip_list[0]['action'] == rp.MOVE
    assert ip_list[0]['source'] == t.move_input_data[0]
    assert ip_list[0]['target'] == os.path.basename(t.move_input_data[0])

    t = Task()
    t.move_input_data = ['/home/vivek/test.dat > new_test.dat']
    ip_list = get_input_list_from_task(t, placeholders)

    assert ip_list[0]['action'] == rp.MOVE
    assert ip_list[0]['source'] == t.move_input_data[0].split('>')[0].strip()
    assert ip_list[0]['target'] == os.path.basename(t.move_input_data[0]
                                                     .split('>')[1].strip())

    # Test upload input data

    t = Task()
    t.upload_input_data = ['/home/vivek/test.dat']
    ip_list = get_input_list_from_task(t, placeholders)

    assert 'action' not in ip_list[0]
    assert ip_list[0]['source'] == t.upload_input_data[0]
    assert ip_list[0]['target'] == os.path.basename(t.upload_input_data[0])

    t = Task()
    t.upload_input_data = ['/home/vivek/test.dat > new_test.dat']
    ip_list = get_input_list_from_task(t, placeholders)

    assert 'action' not in ip_list[0]
    assert ip_list[0]['source'] == t.upload_input_data[0].split('>')[0].strip()
    assert ip_list[0]['target'] == os.path.basename(t.upload_input_data[0]
                                                     .split('>')[1].strip())


# ------------------------------------------------------------------------------
#
def test_output_list_from_task():
    """
    **Purpose**: Test if the 'get_output_list_from_task' function generates the
                 correct RP output transfer directives when given a Task.
    """

    pipeline = str(ru.generate_id('pipeline'))
    stage    = str(ru.generate_id('stage'))
    task     = str(ru.generate_id('task'))

    placeholders = {
        pipeline: {
            stage: {
                task: '/home/vivek/some_file.txt'
            }
        }
    }

    for t in [1, 'a', list(), dict(), True]:
        with pytest.raises(TypeError):
            t = list()
            get_output_list_from_task(t, placeholders)

    # Test copy output data
    t = Task()
    t.copy_output_data = ['/home/vivek/test.dat']
    ip_list = get_output_list_from_task(t, placeholders)

    assert ip_list[0]['action'] == rp.COPY
    assert ip_list[0]['source'] == t.copy_output_data[0]
    assert ip_list[0]['target'] == os.path.basename(t.copy_output_data[0])

    t = Task()
    t.copy_output_data = ['/home/vivek/test.dat > new_test.dat']
    ip_list = get_output_list_from_task(t, placeholders)

    assert ip_list[0]['action'] == rp.COPY
    assert ip_list[0]['source'] == t.copy_output_data[0].split('>')[0].strip()
    assert ip_list[0]['target'] == os.path.basename(t.copy_output_data[0]
                                                     .split('>')[1].strip())


    # Test move output data
    t = Task()
    t.move_output_data = ['/home/vivek/test.dat']
    ip_list = get_output_list_from_task(t, placeholders)

    assert ip_list[0]['action'] == rp.MOVE
    assert ip_list[0]['source'] == t.move_output_data[0]
    assert ip_list[0]['target'] == os.path.basename(t.move_output_data[0])

    t = Task()
    t.move_output_data = ['/home/vivek/test.dat > new_test.dat']
    ip_list = get_output_list_from_task(t, placeholders)

    assert ip_list[0]['action'] == rp.MOVE
    assert ip_list[0]['source'] == t.move_output_data[0].split('>')[0].strip()
    assert ip_list[0]['target'] == os.path.basename(t.move_output_data[0]
                                                     .split('>')[1].strip())


    # Test download input data

    t = Task()
    t.download_output_data = ['/home/vivek/test.dat']
    ip_list = get_output_list_from_task(t, placeholders)

    assert 'action' not in ip_list[0]
    assert ip_list[0]['source'] == t.download_output_data[0]
    assert ip_list[0]['target'] == os.path.basename(t.download_output_data[0])

    t = Task()
    t.download_output_data = ['/home/vivek/test.dat > new_test.dat']
    ip_list = get_output_list_from_task(t, placeholders)

    assert 'action' not in ip_list[0]
    assert ip_list[0]['source'] == t.download_output_data[0].split('>')[0] \
                                                            .strip()
    assert ip_list[0]['target'] == os.path.basename(t.download_output_data[0]
                                                     .split('>')[1].strip())


# ------------------------------------------------------------------------------
#
def test_create_cud_from_task():
    """
    **Purpose**: Test if the 'create_cud_from_task' function generates a RP
                 ComputeUnitDescription with the complete Task description.
    """

    pipeline = 'p1'
    stage    = 's1'
    task     = 't1'

    placeholders = {
        pipeline: {
            stage: {
                task: '/home/vivek/some_file.txt'
            }
        }
    }

    t1 = Task()
    t1.name                 = 't1'
    t1.pre_exec             = ['module load gromacs']
    t1.executable           = 'grompp'
    t1.arguments            = ['hello']
    t1.cpu_reqs             = {'processes'           : 4,
                               'process_type'        : 'MPI',
                               'threads_per_process' : 1,
                               'thread_type'         : 'OpenMP'
                               }
    t1.gpu_reqs             = {'processes'           : 4,
                               'process_type'        : 'MPI',
                               'threads_per_process' : 2,
                               'thread_type'         : 'OpenMP'
                               }
    t1.post_exec            = ['echo test']
    t1.upload_input_data    = ['upload_input.dat']
    t1.copy_input_data      = ['copy_input.dat']
    t1.link_input_data      = ['link_input.dat']
    t1.copy_output_data     = ['copy_output.dat']
    t1.download_output_data = ['download_output.dat']

    p = Pipeline()
    p.name   = 'p1'

    s = Stage()
    s.name   = 's1'
    s.tasks  = t1
    p.stages = s

    p._assign_uid('test')

    cud = create_cud_from_task(t1, placeholders)

    assert cud.name == '%s,%s,%s,%s,%s,%s' % (t1.uid, t1.name,
                                              t1.parent_stage['uid'],
                                              t1.parent_stage['name'],
                                              t1.parent_pipeline['uid'],
                                              t1.parent_pipeline['name'])
    assert cud.pre_exec == t1.pre_exec

    # rp returns executable as a string regardless of whether assignment was using string or list
    assert cud.executable       == t1.executable
    assert cud.arguments        == t1.arguments
    assert cud.post_exec        == t1.post_exec
    assert cud.cpu_processes    == t1.cpu_reqs['processes']
    assert cud.cpu_threads      == t1.cpu_reqs['threads_per_process']
    assert cud.cpu_process_type == t1.cpu_reqs['process_type']
    assert cud.cpu_thread_type  == t1.cpu_reqs['thread_type']
    assert cud.gpu_processes    == t1.gpu_reqs['processes']
    assert cud.gpu_threads      == t1.gpu_reqs['threads_per_process']
    assert cud.gpu_process_type == t1.gpu_reqs['process_type']
    assert cud.gpu_thread_type  == t1.gpu_reqs['thread_type']

    assert {'source': 'upload_input.dat',                         'target': 'upload_input.dat'}    in cud.input_staging
    assert {'source': 'copy_input.dat',      'action':   rp.COPY, 'target': 'copy_input.dat'}      in cud.input_staging
    assert {'source': 'link_input.dat',      'action':   rp.LINK, 'target': 'link_input.dat'}      in cud.input_staging
    assert {'source': 'copy_output.dat',     'action':   rp.COPY, 'target': 'copy_output.dat'}     in cud.output_staging
    assert {'source': 'download_output.dat',                      'target': 'download_output.dat'} in cud.output_staging


# ------------------------------------------------------------------------------
#
def test_create_task_from_cu():
    """
    **Purpose**: Test if the 'create_task_from_cu' function generates a Task
                 with the correct uid, parent_stage and parent_pipeline from
                 a RP ComputeUnit.
    """

    session        = rp.Session()
    umgr           = rp.UnitManager(session=session)
    cud            = rp.ComputeUnitDescription()
    cud.name       = 'uid, name, parent_stage_uid, parent_stage_name, ' \
                     'parent_pipeline_uid, parent_pipeline_name'
    cud.executable = '/bin/echo'

    cu = rp.ComputeUnit(umgr, cud)

    t = create_task_from_cu(cu)

    assert t.uid                     == 'uid'
    assert t.name                    == 'name'
    assert t.parent_stage['uid']     == 'parent_stage_uid'
    assert t.parent_stage['name']    == 'parent_stage_name'
    assert t.parent_pipeline['uid']  == 'parent_pipeline_uid'
    assert t.parent_pipeline['name'] == 'parent_pipeline_name'

    session.close()


# ------------------------------------------------------------------------------
#
def test_resolve_placeholder():
    """
    **Purpose**: Test if the 'resolve_placeholder' function resolves expected
                 placeholders correctly. These placeholders are used to refer to
                 files in different task folders.
    """

    pipeline = str(ru.generate_id('pipeline'))
    stage    = str(ru.generate_id('stage'))
    task     = str(ru.generate_id('task'))

    placeholders = {
        pipeline: {
            stage: {
                task: {
                    'path'   : '/home/vivek',
                    'rts_uid': 'unit.0002'
                }
            }
        }
    }

    # Test only strings are accepted
    raw_paths = [1, list(), dict()]

    for raw_path in raw_paths:
        with pytest.raises(TypeError):
            resolve_placeholders(raw_path, placeholders)

    # Test when no placeholders to resolve
    raw_path = '/home/vivek/some_file.txt'
    new_path = resolve_placeholders(raw_path, placeholders)
    assert new_path == raw_path

    # Test for shared data location
    raw_path = '$SHARED/test.txt'
    new_path = resolve_placeholders(raw_path, placeholders)
    assert new_path == 'pilot:///test.txt'

    # Test for shared data location with rename
    raw_path = '$SHARED/test.txt > new.txt'
    new_path = resolve_placeholders(raw_path, placeholders)
    assert new_path == 'pilot:///test.txt > new.txt'

    # Test for resolving relative data references
    raw_path = '$Pipeline_%s_Stage_%s_Task_%s/some_file.txt' \
             % (pipeline, stage, task)
    new_path = resolve_placeholders(raw_path, placeholders)
    assert new_path == '/home/vivek/some_file.txt'

    # Test for resolving relative data references with rename
    raw_path = '$Pipeline_%s_Stage_%s_Task_%s/some_file.txt > new.txt' \
             % (pipeline, stage, task)
    new_path = resolve_placeholders(raw_path, placeholders)
    assert new_path == '/home/vivek/some_file.txt > new.txt'

    # Test only placeholders in $Pipeline_%s_Stage_%s_Task_%s are accepted
    raw_path = '$Task_2'
    with pytest.raises(ValueError):
        resolve_placeholders(raw_path, placeholders)


# ------------------------------------------------------------------------------

