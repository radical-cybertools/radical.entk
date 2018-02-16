import radical.pilot as rp
from radical.entk import Task
import radical.utils as ru
import traceback
from radical.entk.exceptions import *
import os

logger = ru.get_logger('radical.entk.task_processor')

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

        if not isinstance(path,str):
            raise TypeError(expected_type=str, actual_type=type(path))

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
            return path.replace(placeholder, 'pilot://')

        # Expected placeholder format:
        # $Pipeline_{pipeline.uid}_Stage_{stage.uid}_Task_{task.uid}

        broken_placeholder = placeholder.split('/')[0].split('_')

        if not len(broken_placeholder) ==  6:
            raise ValueError(   
                                obj='placeholder',
                                attribute='length',
                                expected_value = '$Pipeline_{pipeline.uid}_Stage_{stage.uid}_Task_{task.uid} or $SHARED',
                                actual_value = broken_placeholder)

        pipeline_uid    = broken_placeholder[1]
        stage_uid       = broken_placeholder[3]
        task_uid        = broken_placeholder[5]

        resolved_placeholder = path.replace(placeholder,placeholder_dict[pipeline_uid][stage_uid][task_uid])

        return resolved_placeholder

    except Exception, ex:

        logger.error('Failed to resolve placeholder %s, error: %s'%(path, ex))
        raise 
    

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

        return input_data

    except Exception, ex:

        logger.error('Failed to get input list of files from task, error: %s'%ex)
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

        return output_data

    except Exception, ex:
        logger.error('Failed to get output list of files from task, error: %s'%ex)
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
        
        logger.debug('Creating CU from Task %s'%(task.uid))

        if prof:
            prof.prof('cud from task - create', uid=task.uid)

        cud = rp.ComputeUnitDescription()
        cud.name        = '%s,%s,%s'%(task.uid, task.parent_stage, task.parent_pipeline)
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

        logger.debug('Create Task from CU %s'%cu.name)

        if prof:
            prof.prof('task from cu - create', uid=cu.name.split(',')[0].strip())

        task = Task(duplicate=True)
        task.uid                = cu.name.split(',')[0].strip()
        task.parent_stage      = cu.name.split(',')[1].strip()
        task.parent_pipeline   = cu.name.split(',')[2].strip()

        if cu.exit_code is not None:
            task.exit_code = cu.exit_code
        else:

            if cu.state == rp.DONE:
                task.exit_code = 0
            else:
                task.exit_code = 1

        task.path               =  ru.Url(cu.sandbox).path

        if prof:
            prof.prof('task from cu - done', uid=cu.name.split(',')[0].strip())

        logger.debug('Task %s created from CU %s'%(task.uid, cu.name))

        return task

    except Exception, ex:
        logger.error('Task creation from CU failed, error: %s'%ex)
        raise