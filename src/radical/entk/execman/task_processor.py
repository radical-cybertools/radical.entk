import radical.pilot as rp
from radical.entk import Task

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

def create_compute_unit(task):

    cud = rp.ComputeUnitDescription()
    cud.name        = task.uid
    cud.pre_exec    = task.pre_exec
    cud.executable  = task.executable
    cud.arguments   = task.arguments
    cud.post_exec   = task.post_exec
    cud.cores       = task.cores
    cud.mpi         = task.mpi

    cud.input_staging   = get_input_list_from_task(task)
    cud.output_staging  = get_output_list_from_task(task)

    return cud


def get_input_data_from_cu(cu):

    link_input_data = []
    copy_input_data = []
    link_input_data = []

    for input_data in cu.input_staging:

        if 'action' in input_data:

            if input_data['action'] == rp.LINK:
                link_input_data += '%s > %s'%(input_data['source'], input_data['target'])

            else:
                copy_input_data += '%s > %s'%(input_data['source'], input_data['target'])

        else:
            upload_input_data += '%s > %s'%(input_data['source'], input_data['target'])

    return link_input_data, copy_input_data, upload_input_data


def get_output_data_from_cu(cu):

    copy_output_data = []
    download_output_data = []

    for output_data in cu.output_staging:

        if 'action' in output_data:
            copy_output_data += '%s > %s'%(output_data['source'], output_data['target'])

        else:
            download_output_data += '%s > %s'%(output_data['source'], output_data['target'])

    return copy_output_data, download_output_data

def create_task(cu):

    task = Task()
    task.uid        = cu.name
    task.pre_exec   = cu.pre_exec
    task.executable = cu.executable
    task.arguments  = cu.arguments
    task.post_exec  = cu.post_exec
    task.cores      = cu.cores
    task.mpi        = cu.mpi

    task.link_input_data, task.copy_input_data, task.upload_input_data  = get_input_data_from_cu(cu)
    task.copy_output_data, task.download_output_data                    = get_output_data_from_cu(cu)

    return task