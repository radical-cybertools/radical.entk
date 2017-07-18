import radical.pilot as rp
from radical.entk import Task
import radical.utils as ru
import traceback
from radical.entk.exceptions import *
import os

logger = ru.get_logger('radical.entk.task_processor')

def get_input_list_from_task(task):

    """
    Purpose: Parse a Task object to extract the files to be staged as the input. 

    Details: The extracted data is then converted into the appropriate RP directive depending on whether the data
    is to be uploaded/copied/linked.

    :arguments: Task 
    :return: list of RP directives for the files that need to be staged in
    """

    if not isinstance(task, Task):
        raise TypeError(expected_type=Task, actual_type=type(task))


    input_data = []

    if task.link_input_data:

        for data in task.link_input_data:

            if len(data.split('>')) > 1:

                temp = {
                            'source': data.split('>')[0].strip(),                            
                            'target': data.split('>')[1].strip(),
                            'action': rp.LINK
                        }
            else:
                temp = {
                            'source': data.split('>')[0].strip(),                            
                            'target': os.path.basename(data.split('>')[0].strip()),
                            'action': rp.LINK
                        }
            input_data.append(temp)

    if task.upload_input_data:

        for data in task.upload_input_data:

            if len(data.split('>')) > 1:

                temp = {
                            'source': data.split('>')[0].strip(),
                            'target': data.split('>')[1].strip()
                        }
            else:
                temp = {
                            'source': data.split('>')[0].strip(),
                            'target': os.path.basename(data.split('>')[0].strip())
                        }
            input_data.append(temp)


    if task.copy_input_data:

        for data in task.copy_input_data:

            if len(data.split('>')) > 1:

                temp = {
                            'source': data.split('>')[0].strip(),
                            'target': data.split('>')[1].strip(),
                            'action': rp.COPY
                        }
            else:
                temp = {
                            'source': data.split('>')[0].strip(),
                            'target': os.path.basename(data.split('>')[0].strip()),
                            'action': rp.COPY
                        }
            input_data.append(temp)

    return input_data


def get_output_list_from_task(task):

    """
    Purpose: Parse a Task object to extract the files to be staged as the output. 

    Details: The extracted data is then converted into the appropriate RP directive depending on whether the data
    is to be copied/downloaded.

    :arguments: Task 
    :return: list of RP directives for the files that need to be staged out
    """

    if not isinstance(task, Task):
        raise TypeError(expected_type=Task, actual_type=type(task))

    output_data = []

    if task.copy_output_data:

        for data in task.copy_output_data:

            if len(data.split('>')) > 1:

                temp = {
                            'source': data.split('>')[0].strip(),
                            'target': data.split('>')[1].strip(),
                            'action': rp.COPY
                        }
            else:
                temp = {
                            'source': data.split('>')[0].strip(),
                            'target': os.path.basename(data.split('>')[0].strip()),
                            'action': rp.COPY
                        }
            output_data.append(temp)


    if task.download_output_data:

        for data in task.download_output_data:

            if len(data.split('>')) > 1:

                temp = {
                            'source': data.split('>')[0].strip(),
                            'target': data.split('>')[1].strip()
                        }
            else:
                temp = {
                            'source': data.split('>')[0].strip(),
                            'target': os.path.basename(data.split('>')[0].strip())
                        }
            output_data.append(temp)


    return output_data

def create_cud_from_task(task, prof=None):

    """
    Purpose: Create a Compute Unit description based on the defined Task.

    :arguments: Task
    :return: ComputeUnitDescription
    """

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

        cud.input_staging   = get_input_list_from_task(task)
        cud.output_staging  = get_output_list_from_task(task)

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

    :arguments: ComputeUnit
    :return: Task
    """

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