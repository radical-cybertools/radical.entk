import radical.pilot as rp
from radical.entk import Task
import radical.utils as ru
from radical.entk.exceptions import *
import os

logger = ru.Logger('radical.entk.task_processor')


def resolve_placeholders(path, placeholder_dict):
    """
    **Purpose**: Substitute placeholders in staging attributes of a Task with actual paths to the corresponding tasks.

    :arguments:
        :path: string describing the staging paths, possibly containing a placeholder
        :placeholder_dict: dictionary holding the values for placeholders

    """

    try:

        if isinstance(path, unicode):
            path = str(path)

        if not isinstance(path, str):
            raise TypeError(expected_type=str, actual_type=type(path))

        if '$' not in path:
            return path

        # Extract placeholder from path
        if len(path.split('>')) == 1:
            placeholder = path.split('/')[0]
        else:
            if path.split('>')[0].strip().startswith('$'):
                placeholder = path.split('>')[0].strip().split('/')[0]
            else:
                placeholder = path.split('>')[1].strip().split('/')[0]

        # SHARED
        if placeholder == "$SHARED":
            return path.replace(placeholder, 'pilot://')

        # Expected placeholder format:
        # $Pipeline_{pipeline.uid}_Stage_{stage.uid}_Task_{task.uid}

        broken_placeholder = placeholder.split('/')[0].split('_')

        if not len(broken_placeholder) == 6:
            raise ValueError(
                obj='placeholder',
                attribute='length',
                expected_value='$Pipeline_{pipeline.uid}_Stage_{stage.uid}_Task_{task.uid} or $SHARED',
                actual_value=broken_placeholder)

        pipeline_name = broken_placeholder[1]
        stage_name = broken_placeholder[3]
        task_name = broken_placeholder[5]

        if pipeline_name in placeholder_dict.keys():
            if stage_name in placeholder_dict[pipeline_name].keys():
                if task_name in placeholder_dict[pipeline_name][stage_name].keys():
                    resolved_placeholder = path.replace(placeholder, placeholder_dict[
                                                        pipeline_name][stage_name][task_name]['path'])
                else:
                    logger.warning('%s not assigned to any task in Stage %s Pipeline %s' %
                                   (task_name, stage_name, pipeline_name))
            else:
                logger.warning('%s not assigned to any Stage in Pipeline %s' % (
                    stage_name, pipeline_name))
        else:
            logger.warning('%s not assigned to any Pipeline' % (pipeline_name))

        return resolved_placeholder

    except Exception, ex:

        logger.error('Failed to resolve placeholder %s, error: %s' %
                     (path, ex))
        raise


def resolve_arguments(args, placeholder_dict):

    resolved_args = list()

    for entry in args:

        # If entry starts with $, it has a placeholder
        # and needs to be resolved based after a lookup in
        # the placeholder_dict
        if (isinstance(entry, str) or isinstance(entry, unicode)) and entry.startswith('$'):

            placeholder = entry.split('/')[0]

            if placeholder == "$SHARED":
                entry = entry.replace(placeholder, '$RP_PILOT_STAGING')

            else:
                broken_placeholder = placeholder.split('_')

                if not len(broken_placeholder) == 6:
                    raise ValueError(
                        obj='placeholder',
                        attribute='length',
                        expected_value='$Pipeline_{pipeline.uid}_Stage_{stage.uid}_Task_{task.uid} or $SHARED',
                        actual_value=broken_placeholder)

                pipeline_name = broken_placeholder[1]
                stage_name = broken_placeholder[3]
                task_name = broken_placeholder[5]

                try:
                    entry = entry.replace(
                        placeholder, placeholder_dict[pipeline_name][stage_name][task_name]['path'])

                except Exception as ex:
                    logger.warning('Argument parsing failed. Task %s of Stage %s in Pipeline %s does not exist' %
                                   (task_name, stage_name, pipeline_name))

        resolved_args.append(entry)

    return resolved_args


def get_input_list_from_task(task, placeholder_dict):
    """
    Purpose: Parse a Task object to extract the files to be staged as the output. 

    Details: The extracted data is then converted into the appropriate RP directive depending on whether the data
    is to be copied/downloaded.

    :arguments: 
        :task: EnTK Task object
        :placeholder_dict: dictionary holding the values for placeholders

    :return: list of RP directives for the files that need to be staged out
    """

    try:

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

            for path in task.upload_input_data:

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

        if task.move_input_data:

            for path in task.move_input_data:

                path = resolve_placeholders(path, placeholder_dict)

                if len(path.split('>')) > 1:

                    temp = {
                        'source': path.split('>')[0].strip(),
                        'target': path.split('>')[1].strip(),
                        'action': rp.MOVE
                    }
                else:
                    temp = {
                        'source': path.split('>')[0].strip(),
                        'target': os.path.basename(path.split('>')[0].strip()),
                        'action': rp.MOVE
                    }

                input_data.append(temp)

        return input_data

    except Exception, ex:

        logger.error(
            'Failed to get input list of files from task, error: %s' % ex)
        raise


def get_output_list_from_task(task, placeholder_dict):
    """
    Purpose: Parse a Task object to extract the files to be staged as the output. 

    Details: The extracted data is then converted into the appropriate RP directive depending on whether the data
    is to be copied/downloaded.

    :arguments: 
        :task: EnTK Task object
        :placeholder_dict: dictionary holding the values for placeholders

    :return: list of RP directives for the files that need to be staged out

    """

    try:

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

        if task.move_output_data:

            for path in task.move_output_data:

                path = resolve_placeholders(path, placeholder_dict)

                if len(path.split('>')) > 1:

                    temp = {
                        'source': path.split('>')[0].strip(),
                        'target': path.split('>')[1].strip(),
                        'action': rp.MOVE
                    }
                else:
                    temp = {
                        'source': path.split('>')[0].strip(),
                        'target': os.path.basename(path.split('>')[0].strip()),
                        'action': rp.MOVE
                    }
                    
                output_data.append(temp)


        return output_data

    except Exception, ex:
        logger.error(
            'Failed to get output list of files from task, error: %s' % ex)
        raise


def create_cud_from_task(task, placeholder_dict, prof=None):
    """
    Purpose: Create a Compute Unit description based on the defined Task.

    :arguments: 
        :task: EnTK Task object
        :placeholder_dict: dictionary holding the values for placeholders

    :return: ComputeUnitDescription
    """

    try:

        logger.debug('Creating CU from Task %s' % (task.uid))

        if prof:
            prof.prof('cud from task - create', uid=task.uid)

        cud = rp.ComputeUnitDescription()
        cud.name = '%s,%s,%s,%s,%s,%s' % (task.uid, task.name,
                                          task.parent_stage['uid'], task.parent_stage['name'],
                                          task.parent_pipeline['uid'], task.parent_pipeline['name'])
        cud.pre_exec = task.pre_exec
        cud.executable = task.executable
        cud.arguments = resolve_arguments(task.arguments, placeholder_dict)
        cud.post_exec = task.post_exec
        if task.tag:
            cud.tag = resolve_tags(task.tag, placeholder_dict)

        cud.cpu_processes = task.cpu_reqs['processes']
        cud.cpu_threads = task.cpu_reqs['threads_per_process']
        cud.cpu_process_type = task.cpu_reqs['process_type']
        cud.cpu_thread_type = task.cpu_reqs['thread_type']
        cud.gpu_processes = task.gpu_reqs['processes']
        cud.gpu_threads = task.gpu_reqs['threads_per_process']
        cud.gpu_process_type = task.gpu_reqs['process_type']
        cud.gpu_thread_type = task.gpu_reqs['thread_type']
        if task.lfs_per_process:
            cud.lfs_per_process = task.lfs_per_process

        if task.stdout:
            cud.stdout = task.stdout
        if task.stderr:
            cud.stderr = task.stderr

        cud.input_staging = get_input_list_from_task(task, placeholder_dict)
        cud.output_staging = get_output_list_from_task(task, placeholder_dict)

        if prof:
            prof.prof('cud from task - done', uid=task.uid)

        logger.debug('CU %s created from Task %s' % (cud.name, task.uid))

        return cud

    except Exception, ex:
        logger.error('CU creation failed, error: %s' % ex)
        raise


def create_task_from_cu(cu, prof=None):
    """
    Purpose: Create a Task based on the Compute Unit.

    Details: Currently, only the uid, parent_stage and parent_pipeline are retrieved. The exact initial Task (that was
    converted to a CUD) cannot be recovered as the RP API does not provide the same attributes for a CU as for a CUD. 
    Also, this is not required for the most part.

    TODO: Add exit code, stdout, stderr and path attributes to a Task. These can be extracted from a CU

    :arguments: 
        :cu: RP Compute Unit

    :return: Task
    """

    try:

        logger.debug('Create Task from CU %s' % cu.name)

        if prof:
            prof.prof('task from cu - create',
                      uid=cu.name.split(',')[0].strip())

        task = Task()
        task.uid = cu.name.split(',')[0].strip()
        task.name = cu.name.split(',')[1].strip()
        task.parent_stage['uid'] = cu.name.split(',')[2].strip()
        task.parent_stage['name'] = cu.name.split(',')[3].strip()
        task.parent_pipeline['uid'] = cu.name.split(',')[4].strip()
        task.parent_pipeline['name'] = cu.name.split(',')[5].strip()
        task.rts_uid = cu.uid

        if cu.state == rp.DONE:
            task.exit_code = 0
        else:
            task.exit_code = 1

        task.path = ru.Url(cu.sandbox).path

        if prof:
            prof.prof('task from cu - done', uid=cu.name.split(',')[0].strip())

        logger.debug('Task %s created from CU %s' % (task.uid, cu.name))

        return task

    except Exception, ex:
        logger.error('Task creation from CU failed, error: %s' % ex)
        raise
