from radical.entk import Pipeline, Stage, Task, AppManager
import os
from random import randint
# ------------------------------------------------------------------------------
# Set default verbosity
# if os.environ.get('RADICAL_ENTK_VERBOSE') == None:
    # os.environ['RADICAL_ENTK_REPORT'] = 'True'


# Description of how the RabbitMQ process is accessible
# No need to change/set any variables if you installed RabbitMQ has a system
# process. If you are running RabbitMQ under a docker container or another
# VM, set "RMQ_HOSTNAME" and "RMQ_PORT" in the session where you are running
# this script.
hostname = os.environ.get('RMQ_HOSTNAME', 'localhost')
port = int(os.environ.get('RMQ_PORT', 5672))


num_replicas = 8
pipelines = []
# Replica state
# 0 - Ready for MD
# 1 - MD done, waiting for exchange selection
# 2 - Selected for exchange
# 3 - Not selected for exchange
# 4 - Done
replica_state = [0 for _ in range(num_replicas)]
finish_threshold = 4

replica_cycles = [0 for _ in range(num_replicas)]
cycle_threshold = 4

replica_neighbors = dict()
for ind in range(num_replicas):
    replica_neighbors['R_%s'%ind] = list()


def generate_pipeline(index):

    def md_condition():

        print 'Replica %s | Cycle %s | Event: Evaluating condition after MD'%(index, replica_cycles[index])
        
        global replica_state
        replica_state[index] = 1

        if replica_state.count(1) == finish_threshold:            
            return True
            
        return False


    def md_on_true():

        print 'Replica %s | Cycle %s | Event: Adding exchange'%(index, replica_cycles[index])

        global replica_state
        
        replica_state[index] = 2
        s_exch = create_exch_stage()        
        p.add_stages(s_exch)
        exch_r_ids = list()
        for ind, state in enumerate(replica_state):
            if state==1:
                replica_state[ind] = 3
                replica_neighbors['R_%s'%index].append(ind)
                exch_r_ids.append(str(ind))

        print 'Replica %s | Cycle %s | Event: Initiating exchanges between replicas %s'%(index, replica_cycles[index], ' '.join(exch_r_ids))

    def md_on_false():

        print 'Replica %s | Cycle %s | Event: No exchange added'%(index, replica_cycles[index])


    def create_md_stage():

        print 'Replica %s | Cycle %s | Event: Creating MD task'%(index, replica_cycles[index])

        # Create a Stage object
        s_md = Stage()

        # Create a Task object which creates a file named 'output.txt' of size 1 MB
        t_md = Task()
        t_md.executable = ['/bin/sleep']
        t_md.arguments = ['%s'%randint(2,7)]

        # Add the Task to the Stage
        s_md.add_tasks(t_md)

        s_md.post_exec = {  'condition': md_condition,
                            'on_true': md_on_true,
                            'on_false': md_on_false
                        }

        return s_md


    def exch_condition():    

        print 'Replica %s | Cycle %s | Event: Evaluation condition after exchange'%(index, replica_cycles[index])

        global replica_state, replica_cycles

        # Always returns True
        for ind, state in enumerate(replica_state):
            if ind in replica_neighbors['R_%s'%index]:
                if state in [2,3]:
                    replica_cycles[ind] += 1
                    if replica_cycles[ind] == cycle_threshold:
                        print 'Replica %s completed'%ind
                        replica_state[ind] = 4

        return True

    def exch_on_true():

        print 'Replica %s | Cycle %s | Event: Adding MD tasks to replicas after exchange'%(index, replica_cycles[index])

        global replica_state, pipelines
        for ind, state in enumerate(replica_state):
            if ind in replica_neighbors['R_%s'%index]:
                if state in [2,3]:
                    s_md = create_md_stage()
                    pipelines[ind].add_stages(s_md)
                    print 'Replica %s | Cycle %s | Event: MD task added to replica %s'%(index, replica_cycles[index], ind)
                    pipelines[ind].rerun()
                    replica_state[ind] = 0

    def exch_on_false():

        pass

    def create_exch_stage():

        print 'Replica %s | Cycle %s | Event: Creating exchange task'%(index, replica_cycles[index])

        s_exch = Stage()
        t_exch = Task()
        t_exch.executable = ['/bin/sleep']
        t_exch.arguments =  ['5']

        s_exch.add_tasks(t_exch)

        s_exch.post_exec = {
                            'condition': exch_condition,
                            'on_true': exch_on_true,
                            'on_false': exch_on_false
                        }

        return s_exch

    # Create a Pipeline object
    p = Pipeline()
    s_md = create_md_stage()    
    p.add_stages(s_md)

    return p


if __name__ == '__main__':

    for cnt in range(num_replicas):
        pipelines.append(generate_pipeline(index=cnt))

    # Create Application Manager
    appman = AppManager(hostname=hostname, port=port)

    # Create a dictionary describe four mandatory keys:
    # resource, walltime, and cpus
    # resource is 'local.localhost' to execute locally
    res_dict = {

        'resource': 'local.localhost_anaconda',
        'walltime': 10,
        'cpus': num_replicas
    }

    # Assign resource request description to the Application Manager
    appman.resource_desc = res_dict

    # Assign the workflow as a set or list of Pipelines to the Application Manager
    # Note: The list order is not guaranteed to be preserved
    appman.workflow = set(pipelines)

    # Run the Application Manager
    appman.run()
