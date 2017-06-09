import radical.pilot as rp
from radical.entk import Task
import radical.utils as ru
import traceback


logger = ru.get_logger('radical.entk.task_processor')

def get_input_list_from_task(task):

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

def create_cud_from_task(task):

    try:

        logger.debug('Creating CU from Task %s'%(task.uid))
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

        logger.debug('CU %s created from Task %s'%(cud.name, task.uid))

        return cud

    except Exception, ex:
        logger.error('CU creation failed, error: %s'%ex)
        raise


def create_task_from_cu(cu):

    try:

        logger.debug('Create Task from CU %s'%cu.name)

        task = Task()
        task.uid                = cu.name.split(',')[0].strip()
        task._parent_stage       = cu.name.split(',')[1].strip()
        task._parent_pipeline    = cu.name.split(',')[2].strip()
        #task.pre_exec   = cu.pre_exec
        #task.executable = cu.executable
        #task.arguments  = cu.arguments
        #task.post_exec  = cu.post_exec
        #task.cores      = cu.cores
        #task.mpi        = cu.mpi

        #task.link_input_data, task.copy_input_data, task.upload_input_data  = get_input_data_from_cu(cu)
        #task.copy_output_data, task.download_output_data                    = get_output_data_from_cu(cu)

        logger.debug('Task %s created from CU %s'%(task.uid, cu.name))

        return task

    except Exception, ex:
        logger.error('Task creation from CU failed, error: %s'%ex)
        print traceback.format_exc()
        raise