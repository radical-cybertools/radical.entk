
import os
import radical.pilot as rp
import radical.utils as ru

from radical.entk import Task
from radical.entk import exceptions as ree


# FIXME: this ignores the log output location used in other entk loggers
logger = ru.Logger('radical.entk.task_processor')


# ------------------------------------------------------------------------------
#
def resolve_placeholders(path, placeholders):
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
            raise ree.TypeError(expected_type=str,
                                actual_type=type(path))

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

        elems = placeholder.split('/')[0].split('_')

        if not len(elems) == 6:

            expected = '$Pipeline_(pipeline_name)_' \
                       'Stage_(stage_name)_' \
                       'Task_(task_name) or $SHARED',
            raise ree.ValueError(obj='placeholder', attribute='task',
                                 expected_value=expected, actual_value=elems)

        pname    = elems[1]
        sname    = elems[3]
        tname    = elems[5]
        resolved = None

        if pname in placeholders:
            if sname in placeholders[pname]:
                if tname in placeholders[pname][sname]:
                    resolved = path.replace(placeholder,
                               placeholders[pname][sname][tname]['path'])
                else:
                    logger.warning('%s not assigned to any task in Stage %s Pipeline %s' %
                                   (tname, sname, pname))
            else:
                logger.warning('%s not assigned to any Stage in Pipeline %s' % (
                    sname, pname))
        else:
            logger.warning('%s not assigned to any Pipeline' % (pname))

        if not resolved:
            logger.warning('No placeholder could be found for task name %s \
                        stage name %s and pipeline name %s. Please be sure to \
                        use object names and not uids in your references,i.e, \
                        $Pipeline_(pipeline_name)_Stage_(stage_name)_Task_(task_name)')
            expected = '$Pipeline_(pipeline_name)_' \
                       'Stage_(stage_name)_' \
                       'Task_(task_name) or $SHARED'
            raise ree.ValueError(obj='placeholder', attribute='task',
                                 expected_value=expected, actual_value=elems)

        return resolved

    except Exception as ex:

        logger.exception('Failed to resolve placeholder %s, error: %s' %
                         (path, ex))
        raise


def resolve_arguments(args, placeholders):

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
            entry = entry.replace(placeholder, '$RP_PILOT_STAGING')

        elif placeholder.startswith('$Pipeline'):
            elems = placeholder.split('_')

            if len(elems) != 6:

                expected = '$Pipeline_{pipeline.uid}_' \
                           'Stage_{stage.uid}_' \
                           'Task_{task.uid} or $SHARED'
                raise ree.ValueError(obj='placeholder', attribute='length',
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
def resolve_tags(tag, parent_pipeline_name, placeholders):

    # Check self pipeline first
    for sname in placeholders[parent_pipeline_name]:
        for tname in placeholders[parent_pipeline_name][sname]:
            if tag != tname:
                continue
            return placeholders[parent_pipeline_name][sname][tname]['rts_uid']

    for pname in placeholders:

        # skip self pipeline this time
        if pname == parent_pipeline_name:
            continue

        for sname in placeholders[pname]:
            for tname in placeholders[pname][sname]:
                if tag != tname:
                    continue
                return placeholders[pname][sname][tname]['rts_uid']

    raise ree.EnTKError(msg='Tag %s cannot be used as no previous task with '
                            'that name is found' % tag)


# ------------------------------------------------------------------------------
#
def get_input_list_from_task(task, placeholders):
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
            raise ree.TypeError(expected_type=Task, actual_type=type(task))

        input_data = list()

        if task.link_input_data:

            for path in task.link_input_data:

                path = resolve_placeholders(path, placeholders)

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

                path = resolve_placeholders(path, placeholders)

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

                path = resolve_placeholders(path, placeholders)

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

                path = resolve_placeholders(path, placeholders)

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
def get_output_list_from_task(task, placeholders):
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
            raise ree.TypeError(expected_type=Task, actual_type=type(task))


        output_data = list()

        if task.link_output_data:

            for path in task.link_output_data:

                path = resolve_placeholders(path, placeholders)

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

                path = resolve_placeholders(path, placeholders)

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

                path = resolve_placeholders(path, placeholders)

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

                path = resolve_placeholders(path, placeholders)

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
def create_cud_from_task(task, placeholders, prof=None):
    """
    Purpose: Create a Compute Unit description based on the defined Task.

    :arguments:
        :task:         EnTK Task object
        :placeholders: dictionary holding the values for placeholders

    :return: ComputeUnitDescription
    """

    try:

        logger.debug('Creating CU from Task %s' % (task.uid))

        if prof:
            prof.prof('cud_create', uid=task.uid)

        cud = rp.ComputeUnitDescription()
        cud.name = '%s,%s,%s,%s,%s,%s' % (task.uid, task.name,
                                          task.parent_stage['uid'],
                                          task.parent_stage['name'],
                                          task.parent_pipeline['uid'],
                                          task.parent_pipeline['name'])
        cud.pre_exec   = task.pre_exec
        cud.executable = task.executable
        cud.arguments  = resolve_arguments(task.arguments, placeholders)
        cud.sandbox    = task.sandbox
        cud.post_exec  = task.post_exec

        if task.tag:
            if task.parent_pipeline['name']:
                cud.tag = resolve_tags(
                        tag=task.tag,
                        parent_pipeline_name=task.parent_pipeline['name'],
                        placeholders=placeholders)

        cud.cpu_processes    = task.cpu_reqs['cpu_processes']
        cud.cpu_threads      = task.cpu_reqs['cpu_threads']
        cud.cpu_process_type = task.cpu_reqs['cpu_process_type']
        cud.cpu_thread_type  = task.cpu_reqs['cpu_thread_type']
        cud.gpu_processes    = task.gpu_reqs['gpu_processes']
        cud.gpu_threads      = task.gpu_reqs['gpu_threads']
        cud.gpu_process_type = task.gpu_reqs['gpu_process_type']
        cud.gpu_thread_type  = task.gpu_reqs['gpu_thread_type']

        if task.lfs_per_process:
            cud.lfs_per_process = task.lfs_per_process

        if task.stdout: cud.stdout = task.stdout
        if task.stderr: cud.stderr = task.stderr

        cud.input_staging  = get_input_list_from_task(task, placeholders)
        cud.output_staging = get_output_list_from_task(task, placeholders)

        if prof:
            prof.prof('cud from task - done', uid=task.uid)

        logger.debug('CU %s created from Task %s' % (cud.name, task.uid))

        return cud


    except Exception:
        logger.exception('CU creation failed')
        raise


# ------------------------------------------------------------------------------
#
def create_task_from_cu(cu, prof=None):
    """
    Purpose: Create a Task based on the Compute Unit.

    Details: Currently, only the uid, parent_stage and parent_pipeline are
             retrieved. The exact initial Task (that was converted to a CUD)
             cannot be recovered as the RP API does not provide the same
             attributes for a CU as for a CUD.  Also, this is not required for
             the most part.

    TODO:    Add exit code, stdout, stderr and path attributes to a Task.
             These can be extracted from a CU

    :arguments:
        :cu: RP Compute Unit

    :return: Task
    """

    try:
        logger.debug('Create Task from CU %s' % cu.name)

        if prof:
            prof.prof('task_create', uid=cu.name.split(',')[0].strip())

        task = Task()

        task.uid                     = cu.name.split(',')[0].strip()
        task.name                    = cu.name.split(',')[1].strip()
        task.parent_stage['uid']     = cu.name.split(',')[2].strip()
        task.parent_stage['name']    = cu.name.split(',')[3].strip()
        task.parent_pipeline['uid']  = cu.name.split(',')[4].strip()
        task.parent_pipeline['name'] = cu.name.split(',')[5].strip()
        task.rts_uid                 = cu.uid

        if cu.state == rp.DONE: task.exit_code = 0
        else                  : task.exit_code = 1

        task.path = ru.Url(cu.sandbox).path

        if prof:
            prof.prof('task_created', uid=cu.name.split(',')[0].strip())

        logger.debug('Task %s created from CU %s' % (task.uid, cu.name))

        return task


    except Exception:
        logger.exception('Task creation from CU failed, error')
        raise


# ------------------------------------------------------------------------------

