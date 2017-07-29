from radical.entk.execman.task_processor import *
from radical.entk.exceptions import *
from radical.entk import Task, Stage, Pipeline
import radical.pilot as rp
import os, pytest, shutil, glob
import radical.utils as ru

def test_input_list_from_task():

    """
    **Purpose**: Test if the 'get_input_list_from_task' function generates the correct RP input transfer directives
    when given a Task
    """

    pipeline    = str(ru.generate_id('radical.entk.pipeline'))
    stage       = str(ru.generate_id('radical.entk.stage'))
    task        = str(ru.generate_id('radical.entk.task'))

    placeholder_dict = {
                            pipeline: {
                                        stage:{
                                                task: '/home/vivek/some_file.txt'
                                        }
                            }
                    }

    for t in [1,'a',list(), dict(),True]:
        with pytest.raises(TypeError):
            t = list()
            get_input_list_from_task(t, placeholder_dict)


    # Test link input data
    t = Task()
    t.link_input_data = ['/home/vivek/test.dat']
    ip_list = get_input_list_from_task(t, placeholder_dict)

    assert ip_list[0]['source'] == t.link_input_data[0]
    assert ip_list[0]['action'] == rp.LINK
    assert ip_list[0]['target'] == os.path.basename(t.link_input_data[0])


    t = Task()
    t.link_input_data = ['/home/vivek/test.dat > new_test.dat']
    ip_list = get_input_list_from_task(t, placeholder_dict)

    assert ip_list[0]['source'] == t.link_input_data[0].split('>')[0].strip()
    assert ip_list[0]['action'] == rp.LINK
    assert ip_list[0]['target'] == os.path.basename(t.link_input_data[0].split('>')[1].strip())


    # Test copy input data
    t = Task()
    t.copy_input_data = ['/home/vivek/test.dat']
    ip_list = get_input_list_from_task(t, placeholder_dict)

    assert ip_list[0]['source'] == t.copy_input_data[0]
    assert ip_list[0]['action'] == rp.COPY
    assert ip_list[0]['target'] == os.path.basename(t.copy_input_data[0])


    t = Task()
    t.copy_input_data = ['/home/vivek/test.dat > new_test.dat']
    ip_list = get_input_list_from_task(t, placeholder_dict)

    assert ip_list[0]['source'] == t.copy_input_data[0].split('>')[0].strip()
    assert ip_list[0]['action'] == rp.COPY
    assert ip_list[0]['target'] == os.path.basename(t.copy_input_data[0].split('>')[1].strip())


    # Test upload input data

    t = Task()
    t.upload_input_data = ['/home/vivek/test.dat']
    ip_list = get_input_list_from_task(t, placeholder_dict)

    assert ip_list[0]['source'] == t.upload_input_data[0]
    assert 'action' not in ip_list[0]
    assert ip_list[0]['target'] == os.path.basename(t.upload_input_data[0])


    t = Task()
    t.upload_input_data = ['/home/vivek/test.dat > new_test.dat']
    ip_list = get_input_list_from_task(t, placeholder_dict)

    assert ip_list[0]['source'] == t.upload_input_data[0].split('>')[0].strip()
    assert 'action' not in ip_list[0]
    assert ip_list[0]['target'] == os.path.basename(t.upload_input_data[0].split('>')[1].strip())


def test_output_list_from_task():

    """
    **Purpose**: Test if the 'get_output_list_from_task' function generates the correct RP output transfer directives
    when given a Task
    """

    pipeline    = str(ru.generate_id('radical.entk.pipeline'))
    stage       = str(ru.generate_id('radical.entk.stage'))
    task        = str(ru.generate_id('radical.entk.task'))

    placeholder_dict = {
                            pipeline: {
                                        stage:{
                                                task: '/home/vivek/some_file.txt'
                                        }
                            }
                    }

    for t in [1,'a',list(), dict(),True]:
        with pytest.raises(TypeError):
            t = list()
            get_output_list_from_task(t, placeholder_dict)


    # Test copy output data
    t = Task()
    t.copy_output_data = ['/home/vivek/test.dat']
    ip_list = get_output_list_from_task(t, placeholder_dict)

    assert ip_list[0]['source'] == t.copy_output_data[0]
    assert ip_list[0]['action'] == rp.COPY
    assert ip_list[0]['target'] == os.path.basename(t.copy_output_data[0])


    t = Task()
    t.copy_output_data = ['/home/vivek/test.dat > new_test.dat']
    ip_list = get_output_list_from_task(t, placeholder_dict)

    assert ip_list[0]['source'] == t.copy_output_data[0].split('>')[0].strip()
    assert ip_list[0]['action'] == rp.COPY
    assert ip_list[0]['target'] == os.path.basename(t.copy_output_data[0].split('>')[1].strip())


    # Test download input data

    t = Task()
    t.download_output_data = ['/home/vivek/test.dat']
    ip_list = get_output_list_from_task(t, placeholder_dict)

    assert ip_list[0]['source'] == t.download_output_data[0]
    assert 'action' not in ip_list[0]
    assert ip_list[0]['target'] == os.path.basename(t.download_output_data[0])


    t = Task()
    t.download_output_data = ['/home/vivek/test.dat > new_test.dat']
    ip_list = get_output_list_from_task(t, placeholder_dict)

    assert ip_list[0]['source'] == t.download_output_data[0].split('>')[0].strip()
    assert 'action' not in ip_list[0]
    assert ip_list[0]['target'] == os.path.basename(t.download_output_data[0].split('>')[1].strip())

def test_create_cud_from_task():


    """
    **Purpose**: Test if the 'create_cud_from_task' function generates a RP ComputeUnitDescription with the complete
    Task description
    """

    pipeline    = str(ru.generate_id('radical.entk.pipeline'))
    stage       = str(ru.generate_id('radical.entk.stage'))
    task        = str(ru.generate_id('radical.entk.task'))

    placeholder_dict = {
                            pipeline: {
                                        stage:{
                                                task: '/home/vivek/some_file.txt'
                                        }
                            }
                    }

    t1 = Task()
    t1.name = 'simulation'
    t1.pre_exec = ['module load gromacs']
    t1.executable = ['grompp']
    t1.arguments = ['hello']
    t1.cores = 4
    t1.mpi = True
    t1.post_exec = ['echo test']
    
    t1.upload_input_data    = ['upload_input.dat']
    t1.copy_input_data      = ['copy_input.dat']
    t1.link_input_data      = ['link_input.dat']
    t1.copy_output_data     = ['copy_output.dat']
    t1.download_output_data = ['download_output.dat']

    p = Pipeline()
    s = Stage()
    p.stages = s
    s.tasks = t1

    cud = create_cud_from_task(t1, placeholder_dict)

    assert cud.name == '%s,%s,%s'%(t1.uid, t1._parent_stage, t1._parent_pipeline)
    assert cud.pre_exec == t1.pre_exec

    # rp returns executable as a string regardless of whether assignment was using string or list
    assert cud.executable == t1.executable[0] 
    assert cud.arguments == t1.arguments
    assert cud.cores == t1.cores
    assert cud.mpi == t1.mpi
    assert cud.post_exec == t1.post_exec

    assert {'source':'upload_input.dat', 'target':'upload_input.dat'} in cud.input_staging
    assert {'source':'copy_input.dat', 'action': rp.COPY, 'target':'copy_input.dat'} in cud.input_staging
    assert {'source':'link_input.dat', 'action': rp.LINK, 'target':'link_input.dat'} in cud.input_staging
    assert {'source':'copy_output.dat', 'action': rp.COPY, 'target':'copy_output.dat'} in cud.output_staging
    assert {'source':'download_output.dat', 'target':'download_output.dat'} in cud.output_staging

def test_create_task_from_cu():

    """
    **Purpose**: Test if the 'create_task_from_cu' function generates a Task with the correct uid, parent_stage and
    parent_pipeline from a RP ComputeUnit
    """


    os.environ['RADICAL_PILOT_DBURL'] = 'mongodb://entk:entk@ds129010.mlab.com:29010/test_entk'
    session = rp.Session()
    umgr = rp.UnitManager(session=session)
    cud = rp.ComputeUnitDescription()
    cud.name = 'uid, parent_stage, parent_pipeline'
    cud.executable = '/bin/echo'

    cu = rp.ComputeUnit(umgr, cud)

    t = create_task_from_cu(cu)

    assert t.uid == 'uid'
    assert t._parent_stage == 'parent_stage'
    assert t._parent_pipeline == 'parent_pipeline'


def test_resolve_placeholder():

    """
    **Purpose**: Test if the 'resolve_placeholder' function resolves expected placeholders correctly. These 
    placeholders are used to refer to files in different task folders.
    """

    pipeline    = str(ru.generate_id('radical.entk.pipeline'))
    stage       = str(ru.generate_id('radical.entk.stage'))
    task        = str(ru.generate_id('radical.entk.task'))

    placeholder_dict = {
                            pipeline: {
                                        stage:{
                                                task: '/home/vivek'
                                        }
                            }
                    }

    # Test only strings are accepted
    raw_paths = [1, list(), dict()]

    for raw_path in raw_paths:
        with pytest.raises(TypeError):
            resolve_placeholders(raw_path, placeholder_dict)

    # Test when no placeholders to resolve
    raw_path = '/home/vivek/some_file.txt'
    processed_path = resolve_placeholders(raw_path, placeholder_dict)
    assert processed_path == raw_path

    # Test for shared data location
    raw_path = '$SHARED/test.txt'
    processed_path = resolve_placeholders(raw_path, placeholder_dict)
    assert processed_path == 'staging:///test.txt'

    # Test for shared data location with rename
    raw_path = '$SHARED/test.txt > new.txt'
    processed_path = resolve_placeholders(raw_path, placeholder_dict)
    assert processed_path == 'staging:///test.txt > new.txt'

    # Test for resolving relative data references
    raw_path = '$Pipeline_%s_Stage_%s_Task_%s/some_file.txt'%(pipeline, stage, task)
    processed_path = resolve_placeholders(raw_path, placeholder_dict)
    assert processed_path == '/home/vivek/some_file.txt'

    # Test for resolving relative data references with rename
    raw_path = '$Pipeline_%s_Stage_%s_Task_%s/some_file.txt > new.txt'%(pipeline, stage, task)
    processed_path = resolve_placeholders(raw_path, placeholder_dict)
    assert processed_path == '/home/vivek/some_file.txt > new.txt'
    
    # Test only placeholders in $Pipeline_%s_Stage_%s_Task_%s are accepted
    raw_path='$Task_2'
    with pytest.raises(ValueError):
        resolve_placeholders(raw_path, placeholder_dict)