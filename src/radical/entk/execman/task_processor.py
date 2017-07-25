import radical.pilot as rp
from radical.entk import Task
import radical.utils as ru
import traceback
from radical.entk.exceptions import *
import os

logger = ru.get_logger('radical.entk.task_processor')

def resolve_placeholders(path, placeholder_dict):

    # Substitute placeholders in task descriptipons with actual paths to
    # the corresponding tasks

    if '$' not in path:
        return path

    # Extract placeholder from path
    if len(path.split('>'))==1:
        placeholder = path.split('/')[0]
    else:
        if path.split('>')[0].strip().startswith('$'):
            placeholder = path.split('>')[0].strip().split('/')[0]
        else:
            placeholder = path.split('>')[1].strip().split('/')[0]

    # SHARED
    if placeholder == "$SHARED":
        return path.replace(placeholder, 'staging://')

    # Expected placeholder format:
    # $Pipeline_{pipeline.uid}_Stage_{stage.uid}_Task_{task.uid}

    broken_placeholder = placeholder.split('_')

    pipeline_uid    = broken_placeholder[1]
    stage_uid       = broken_placeholder[3]
    task_uid        = broken_placeholder[5]

    return path.replace(placeholder,placeholder_dict[pipeline_uid][stage_uid][task_uid])
    

def get_input_list_from_task(task, placeholder_dict):

    if not isinstance(task, Task):
        raise TypeError(expected_type=Task, actual_type=type(task))

    input_data = []

    if task.link_input_data:

        for path in task.link_input_data:

            path = resolve_placeholders(path, placeholder_dict)

            if len(path.split('>')) > 1:

                temp = {
                            'source': path.split('>')[0].strip(),                            
                            'target': path.split('>')[1].strip(),
                            'action': rp.LINK
                        }
            else:
                temp = {
                            'source': path.split('>')[0].strip(),                            
                            'target': os.path.basename(path.split('>')[0].strip()),
                            'action': rp.LINK
                        }
            input_data.append(temp)

    if task.upload_input_data:

        for data in task.upload_input_data:

            path = resolve_placeholders(path, placeholder_dict)

            if len(path.split('>')) > 1:

                temp = {
                            'source': path.split('>')[0].strip(),
                            'target': path.split('>')[1].strip()
                        }
            else:
                temp = {
                            'source': path.split('>')[0].strip(),
                            'target': os.path.basename(path.split('>')[0].strip())
                        }
            input_data.append(temp)


    if task.copy_input_data:

        for path in task.copy_input_data:

            path = resolve_placeholders(path, placeholder_dict)

            if len(path.split('>')) > 1:

                temp = {
                            'source': path.split('>')[0].strip(),
                            'target': path.split('>')[1].strip(),
                            'action': rp.COPY
                        }
            else:
                temp = {
                            'source': path.split('>')[0].strip(),
                            'target': os.path.basename(path.split('>')[0].strip()),
                            'action': rp.COPY
                        }
            input_data.append(temp)

    return input_data


def get_output_list_from_task(task, placeholder_dict):

    if not isinstance(task, Task):
        raise TypeError(expected_type=Task, actual_type=type(task))


    output_data = []

    if task.copy_output_data:

        for path in task.copy_output_data:

            path = resolve_placeholders(path, placeholder_dict)

            if len(path.split('>')) > 1:

                temp = {
                            'source': path.split('>')[0].strip(),
                            'target': path.split('>')[1].strip(),
                            'action': rp.COPY
                        }
            else:
                temp = {
                            'source': path.split('>')[0].strip(),
                            'target': os.path.basename(path.split('>')[0].strip()),
                            'action': rp.COPY
                        }
            output_data.append(temp)


    if task.download_output_data:

        for path in task.download_output_data:

            path = resolve_placeholders(path, placeholder_dict)

            if len(path.split('>')) > 1:

                temp = {
                            'source': path.split('>')[0].strip(),
                            'target': path.split('>')[1].strip()
                        }
            else:
                temp = {
                            'source': path.split('>')[0].strip(),
                            'target': os.path.basename(path.split('>')[0].strip())
                        }
            output_data.append(temp)


    return output_data

def create_cud_from_task(task, placeholder_dict, prof=None):

    try:
        
        logger.debug('Creating CU from Task %s'%(task.uid))

        if prof:
            prof.prof('cud from task - create', uid=task.uid)

        cud = rp.ComputeUnitDescription()
        cud.name        = '%s,%s,%s'%(task.uid, task._parent_stage, task._parent_pipeline)
        cud.pre_exec    = task.pre_exec
        cud.executable  = task.executable
        cud.arguments   = task.arguments
        cud.post_exec   = task.post_exec
        cud.cores       = task.cores
        cud.mpi         = task.mpi

        cud.input_staging   = get_input_list_from_task(task, placeholder_dict)
        cud.output_staging  = get_output_list_from_task(task, placeholder_dict)

        if prof:
            prof.prof('cud from task - done', uid=task.uid)

        logger.debug('CU %s created from Task %s'%(cud.name, task.uid))

        return cud

    except Exception, ex:
        logger.error('CU creation failed, error: %s'%ex)
        raise


def create_task_from_cu(cu, prof=None):

    try:

        logger.debug('Create Task from CU %s'%cu.name)

        if prof:
            prof.prof('task from cu - create', uid=cu.name.split(',')[0].strip())

        task = Task()
        task.uid                = cu.name.split(',')[0].strip()
        task._parent_stage       = cu.name.split(',')[1].strip()
        task._parent_pipeline    = cu.name.split(',')[2].strip()

        if prof:
            prof.prof('task from cu - done', uid=cu.name.split(',')[0].strip())

        logger.debug('Task %s created from CU %s'%(task.uid, cu.name))

        return task

    except Exception, ex:
        logger.error('Task creation from CU failed, error: %s'%ex)
        print traceback.format_exc()
        raise