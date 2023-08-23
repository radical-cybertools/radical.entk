
import os
import pickle

import radical.pilot as rp
import radical.utils as ru

from ...task       import Task
from ...exceptions import EnTKTypeError, EnTKValueError


# ------------------------------------------------------------------------------
#
def resolve_placeholders(path, placeholders, logger):
    """
    **Purpose**: Substitute placeholders in staging attributes of a Task with
                 actual paths to the corresponding tasks.

    :arguments:
        :path:             string describing the staging paths, possibly
                           containing a placeholder
        :placeholders: dictionary holding the values for placeholders
    """

    try:

        if isinstance(path, str):
            path = str(path)

        if not isinstance(path, str):
            raise EnTKTypeError(expected_type=str, actual_type=type(path))

        if '$' not in path:
            return path

        # Extract placeholder from path
        path_placeholders = list()
        if len(path.split('>')) == 1:
            path_placeholders.append(path.split('/')[0])

        else:
            if path.split('>')[0].strip().startswith('$'):
                path_placeholders.append(
                        path.split('>')[0].strip().split('/')[0])

            if path.split('>')[1].strip().startswith('$'):
                path_placeholders.append(
                        path.split('>')[1].strip().split('/')[0])

        resolved = path
        for placeholder in path_placeholders:

            # SHARED
            if placeholder == "$SHARED":
                resolved = resolved.replace(placeholder, 'pilot://')
                continue

            # Expected placeholder format:
            # $Pipeline_{pipeline.uid}_Stage_{stage.uid}_Task_{task.uid}
            elems = placeholder.split('/')[0].split('_')
            if not len(elems) == 6:

                expected = '$Pipeline_(pipeline_name)_' \
                           'Stage_(stage_name)_' \
                           'Task_(task_name) or $SHARED',
                raise EnTKValueError(obj='placeholder', attribute='task',
                                    expected_value=expected, actual_value=elems)

            pname    = elems[1]
            sname    = elems[3]
            tname    = elems[5]
            is_resolved_by_uid = False

            if pname in placeholders:
                if sname in placeholders[pname]:
                    if tname in placeholders[pname][sname]:
                        resolved = resolved.replace(placeholder,
                                   placeholders[pname][sname][tname]['path'])
                        is_resolved_by_uid = True
                    else:
                        logger.warning('%s not assigned to any task in '
                                'Stage %s Pipeline %s' % (tname, sname, pname))
                else:
                    logger.warning('%s not assigned to any Stage in Pipeline %s'
                                  % (sname, pname))
            else:
                logger.warning('%s not assigned to any Pipeline' % (pname))

            if is_resolved_by_uid is False:
                placeholders_by_name = placeholders["__by_name__"]
                if pname in placeholders_by_name:
                    if sname in placeholders_by_name[pname]:
                        if tname in placeholders_by_name[pname][sname]:
                            resolved = resolved.replace(placeholder,
                              placeholders_by_name[pname][sname][tname]['path'])
                        else:
                            logger.warning('%s not assigned to any task in '
                                 'Stage %s Pipeline %s' % (tname, sname, pname))
                    else:
                        logger.warning('%s not assigned to any Stage in '
                                       'Pipeline %s' % (sname, pname))
                else:
                    logger.warning('%s not assigned to any Pipeline' % (pname))


            if not resolved:
                logger.warning('No placeholder could be found for task name %s \
                            stage name %s and pipeline name %s. Please be sure \
                            to use object names and not uids in your \
                            references,i.e, \
                 $Pipeline_(pipeline_name)_Stage_(stage_name)_Task_(task_name)')
                expected = '$Pipeline_(pipeline_name)_' \
                           'Stage_(stage_name)_' \
                           'Task_(task_name) or $SHARED'
                raise EnTKValueError(obj='placeholder', attribute='task',
                                    expected_value=expected, actual_value=elems)

        return resolved

    except Exception as ex:

        logger.exception('Failed to resolve placeholder %s, error: %s' %
                         (path, ex))
        raise


# ------------------------------------------------------------------------------
#
def resolve_arguments(args, placeholders, logger):

    resolved_args = list()

    for entry in args:

        # If entry starts with $, it has a placeholder
        # and needs to be resolved based after a lookup in
        # the placeholders
        if not isinstance(entry, str) or \
           not entry.startswith('$'):

            resolved_args.append(entry)
            continue


        placeholder = entry.split('/')[0]

        if placeholder == "$SHARED":
            entry = entry.replace(placeholder, '$RP_PILOT_SANDBOX')

        elif placeholder.startswith('$Pipeline'):
            elems = placeholder.split('_')

            if len(elems) != 6:

                expected = '$Pipeline_{pipeline.uid}_' \
                           'Stage_{stage.uid}_' \
                           'Task_{task.uid} or $SHARED'
                raise EnTKValueError(obj='placeholder', attribute='length',
                                    expected_value=expected, actual_value=elems)

            pname = elems[1]
            sname = elems[3]
            tname = elems[5]

            try:
                entry = entry.replace(placeholder,
                                      placeholders[pname][sname][tname]['path'])

            except Exception:
                logger.warning('Argument parsing failed. Task %s of Stage %s '
                               'in Pipeline %s does not exist',
                               tname, sname, pname)

        resolved_args.append(entry)

    return resolved_args


# ------------------------------------------------------------------------------
#
def resolve_tags(task, parent_pipeline_name, placeholders):

    tags = dict(task.tags or {'colocate': task.uid})

    pnames = list(placeholders)
    pnames.remove(parent_pipeline_name)

    colo_tag = tags['colocate']
    for pname in [parent_pipeline_name] + pnames:
        for sname in placeholders[pname]:
            if colo_tag in placeholders[pname][sname]:
                tags['colocate'] = placeholders[pname][sname][colo_tag]['uid']
                return tags

    return tags


# ------------------------------------------------------------------------------
#
def get_input_list_from_task(task, placeholders, logger):
    """
    Purpose: Parse Task object to extract the files to be staged as input.

    Details: The extracted data is then converted into the appropriate RP
             directive depending on whether the data is to be copied/downloaded.

    :arguments:
        :task:         EnTK Task object
        :placeholders: dictionary holding the values for placeholders

    :return: list of RP directives for the files that need to be staged in
    """

    try:

        if not isinstance(task, Task):
            raise EnTKTypeError(expected_type=Task, actual_type=type(task))

        input_data = list()

        if task.link_input_data:

            for path in task.link_input_data:

                path = resolve_placeholders(path, placeholders, logger)

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

                path = resolve_placeholders(path, placeholders, logger)

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

                path = resolve_placeholders(path, placeholders, logger)

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

                path = resolve_placeholders(path, placeholders, logger)

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


    except Exception:

        logger.exception('Failed to get input list of files from task')
        raise


# ------------------------------------------------------------------------------
#
def get_output_list_from_task(task, placeholders, logger):
    """
    Purpose: Parse Task object to extract the files to be staged as the output.

    Details: The extracted data is then converted into the appropriate RP
             directive depending on whether the data is to be copied/downloaded.

    :arguments:
        :task:         EnTK Task object
        :placeholders: dictionary holding the values for placeholders

    :return: list of RP directives for the files that need to be staged out

    """

    try:

        if not isinstance(task, Task):
            raise EnTKTypeError(expected_type=Task, actual_type=type(task))


        output_data = list()

        if task.link_output_data:

            for path in task.link_output_data:

                path = resolve_placeholders(path, placeholders, logger)

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
                output_data.append(temp)

        if task.download_output_data:

            for path in task.download_output_data:

                path = resolve_placeholders(path, placeholders, logger)

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

        if task.copy_output_data:

            for path in task.copy_output_data:

                path = resolve_placeholders(path, placeholders, logger)

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

        if task.move_output_data:

            for path in task.move_output_data:

                path = resolve_placeholders(path, placeholders, logger)

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


    except Exception:
        logger.exception('Failed to get output list of files from task')
        raise


# ------------------------------------------------------------------------------
#
def create_td_from_task(task, placeholders, task_hash_table, pkl_path, sid,
                        logger, prof=None):
    """
    Purpose: Create an RP Task description based on the defined Task.

    :arguments:
        :task:         EnTK Task object
        :placeholders: dictionary holding the values for placeholders

    :return: rp.TaskDescription
    """

    try:

        logger.debug('Creating Task from Task %s: %s' % (task.uid, task.sandbox))
        logger.debug('Hash table state: %s' % task_hash_table)

        if prof:
            prof.prof('td_create', uid=task.uid)

        td = rp.TaskDescription()

        task_pre_uid = task_hash_table.get(task.uid, None)
        if task_pre_uid is None:
            td.uid = task.uid
        else:
            tmp_uid = ru.generate_id(prefix=task.uid, ns=sid)
            td.uid = tmp_uid
        task_hash_table[task.uid] = td.uid
        with open(pkl_path, 'wb') as pickle_file:
            pickle.dump(task_hash_table, pickle_file, pickle.HIGHEST_PROTOCOL)

        logger.debug('Hash table state: %s' % task_hash_table)

        td.name = '%s,%s,%s,%s,%s,%s' % (task.uid, task.name,
                                         task.parent_stage['uid'],
                                         task.parent_stage['name'],
                                         task.parent_pipeline['uid'],
                                         task.parent_pipeline['name'])

        td.pre_launch     = task.pre_launch
        td.pre_exec       = task.pre_exec
        td.executable     = task.executable
        td.arguments      = resolve_arguments(task.arguments, placeholders, logger)
        td.sandbox        = task.sandbox
        td.post_exec      = task.post_exec
        td.environment    = task.environment
        td.post_launch    = task.post_launch
        td.stage_on_error = task.stage_on_error

        if task.annotations:
            td.metadata   = {'data': task.annotations.as_dict()}

        if task.parent_pipeline['uid']:
            td.tags = resolve_tags(
                task=task,
                parent_pipeline_name=task.parent_pipeline['uid'],
                placeholders=placeholders)

        td.ranks          = task.cpu_reqs['cpu_processes']
        td.cores_per_rank = task.cpu_reqs['cpu_threads']
        td.gpus_per_rank  = task.gpu_reqs['gpu_processes']

        if task.cpu_reqs['cpu_thread_type']:
            td.threading_type = task.cpu_reqs['cpu_thread_type']
        else:
            td.threading_type = rp.POSIX

        if task.gpu_reqs['gpu_process_type']:
            td.gpu_type = task.gpu_reqs['gpu_process_type']
        else:
            td.gpu_type = ''

        if task.lfs_per_process:
            td.lfs_per_process = task.lfs_per_process

        if task.mem_per_process:
            td.mem_per_process = task.mem_per_process

        if task.stdout: td.stdout = task.stdout
        if task.stderr: td.stderr = task.stderr

        td.input_staging  = get_input_list_from_task(task, placeholders, logger)
        td.output_staging = get_output_list_from_task(task, placeholders, logger)

        if prof:
            prof.prof('td from task - done', uid=task.uid)

        logger.debug('Task %s created from Task %s' % (td.name, task.uid))

        return td

    except Exception:
        logger.exception('Task creation failed')
        raise


# ------------------------------------------------------------------------------
#
def create_task_from_rp(rp_task, logger, prof=None):
    """
    Purpose: Create a Task based on the RP Task.

    Details: Currently, only the uid, parent_stage and parent_pipeline are
             retrieved. The exact initial Task (that was converted to a TD)
             cannot be recovered as the RP API does not provide the same
             attributes for a Task as for a TD.  Also, this is not required for
             the most part.

    :arguments:
        :task: RP Task

    :return: Task
    """

    try:
        logger.debug('Create Task from RP Task %s' % rp_task.name)

        if prof:
            prof.prof('task_create', uid=rp_task.name.split(',')[0].strip())

        task_info_tuple = rp_task.name.split(',')
        task = Task({
            'uid'                     : task_info_tuple[0].strip(),
            'name'                    : task_info_tuple[1].strip(),
            'parent_stage'   : {'uid' : task_info_tuple[2].strip(),
                                'name': task_info_tuple[3].strip()},
            'parent_pipeline': {'uid' : task_info_tuple[4].strip(),
                                'name': task_info_tuple[5].strip()},
            'rts_uid'                 : rp_task.uid
        })

        if   rp_task.state == rp.DONE                  : task.exit_code = 0
        elif rp_task.state in [rp.FAILED, rp.CANCELED] : task.exit_code = 1

        if rp_task.state == rp.FAILED:
            task.exception        = rp_task.exception
            task.exception_detail = rp_task.exception_detail

        task.path = ru.Url(rp_task.sandbox).path

        if prof:
            prof.prof('task_created', uid=task.uid)

        logger.debug('Task %s created from RP Task %s' % (task.uid, rp_task.name))

        return task


    except Exception:
        logger.exception('Task creation from RP Task failed, error')
        raise


# ------------------------------------------------------------------------------

