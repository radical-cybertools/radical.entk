__author__    = "Vivek Balasubramanian <vivek.balasubramanian@rutgers.edu>"
__copyright__ = "Copyright 2016, http://radical.rutgers.edu"
__license__   = "MIT"


from plugin_base import PluginBase
import radical.pilot as rp
import radical.utils as ru

import saga

from staging.input_data import get_input_data
from staging.output_data import get_output_data


_plugin_info = {
            'name': 'eop',
            'type': 'static'
        }

class PluginEoP(object):

    def __init__(self):

        self._executable_workload = None
        self._resource = None
        self._manager = None

        self._logger = ru.get_logger("radical.entk.plugin.eop")
        self._reporter = self._logger.report

        self._tot_fin_tasks=[0]
        self._tot_sub_tasks=[0]

        self._logger.info("Plugin EoP created")


    def register_resource(self, resource):
        self._resource = resource
        self._logger.info("Registered resource %s with execution plugin"%(resource))

    def get_resource(self):
        return self._resource

    @property
    def tot_fin_tasks(self):
        return self._tot_fin_tasks
    
    @tot_fin_tasks.setter
    def tot_fin_tasks(self, val):
        self._tot_fin_tasks = val


    def set_workload(self, kernels, monitor=None, cur_task = None):

        if type(kernels) != list:
            self._executable_workload = [kernels]
        else:
            self._executable_workload = kernels

        temp=[]
        for item in self._executable_workload:
            if item != None:
                temp.append(item)

        self._executable_workload = temp

        self._logger.info("New workload assigned to plugin for execution")


    def add_manager(self, manager):
        self._manager = manager
        self._logger.debug("Task execution manager (RP-Unit Manager) assigned to execution plugin")


    def create_tasks(self, record, pattern_name, iteration, stage, instance=None):

        try:

            if len(self._executable_workload) == 0:
                return None

            from staging.input_data import get_input_data
            from staging.output_data import get_output_data

            if len(self._executable_workload) > 1:

                cuds = []

                inst=1

                for kernel in self._executable_workload:

                    kernel._bind_to_resource(self._resource)
                    rbound_kernel = kernel
                    cud = rp.ComputeUnitDescription()
                    cud.name = "stage-{0}-task-{1}".format(stage, inst)
                    self._logger.debug('Creating stage %s of pipeline %s'%(stage, inst))

                    cud.pre_exec           = rbound_kernel.pre_exec
                    cud.post_exec          = rbound_kernel.post_exec
                    cud.executable         = rbound_kernel.executable
                    cud.arguments          = rbound_kernel.arguments
                    cud.mpi                = rbound_kernel.uses_mpi
                    cud.cores              = rbound_kernel.cores
                    cud.input_staging      = get_input_data(rbound_kernel, 
                                                            record, 
                                                            cur_pat = pattern_name, 
                                                            cur_iter= iteration, 
                                                            cur_stage = stage, 
                                                            cur_task=inst
                                                        )

                    cud.output_staging     = get_output_data(rbound_kernel, 
                                                            record, 
                                                            cur_pat = pattern_name, 
                                                            cur_iter= iteration, 
                                                            cur_stage = stage, 
                                                            cur_task=inst
                                                        )

                    inst+=1

                    cuds.append(cud)
                    self._logger.debug("Kernel %s converted into RP Compute Unit"%(kernel.name))

                return cuds

            else:

                kernel = self._executable_workload[0]

                cur_stage = stage
                cur_task = instance

                if len(self._tot_fin_tasks) < cur_stage:
                    self._tot_fin_tasks.append(0)
                    self._logger.info('\nStarting submission of stage %s of all pipelines'%(cur_stage))                
                    
                self._logger.debug('Creating task %s of stage %s'%(cur_task,cur_stage))

                kernel._bind_to_resource(self._resource)
                rbound_kernel = kernel

                cud = rp.ComputeUnitDescription()
                cud.name = "stage-{0}-task-{1}".format(cur_stage,cur_task)

                cud.pre_exec           = rbound_kernel.pre_exec
                cud.post_exec          = rbound_kernel.post_exec
                cud.executable         = rbound_kernel.executable
                cud.arguments          = rbound_kernel.arguments
                cud.mpi                = rbound_kernel.uses_mpi
                cud.cores              = rbound_kernel.cores
                cud.input_staging      = get_input_data(rbound_kernel, 
                                                        record, 
                                                        cur_pat = pattern_name, 
                                                        cur_iter= iteration, 
                                                        cur_stage = cur_stage, 
                                                        cur_task=cur_task
                                                    )
                cud.output_staging     = get_output_data(rbound_kernel, 
                                                        record, 
                                                        cur_pat = pattern_name, 
                                                        cur_iter= iteration, 
                                                        cur_stage = cur_stage, 
                                                        cur_task=cur_task
                                                    )

                return cud

        except Exception, ex:
            self._logger.error("Task creation failed, error: %s"%(ex))
            raise


    def execute_tasks(self, tasks):

        try:

            if tasks == None:
                return None

            if type(tasks) == list:
                cur_stage = int(tasks[0].name.split('-')[1])
                self._logger.info('Submitting stage %s of all pipelines'%(cur_stage))
                exec_cus = self._manager.submit_units(tasks)
                return exec_cus

            else:
                cur_stage = int(tasks.name.split('-')[1])
                cur_task = int(tasks.name.split('-')[3])
                self._logger.info('Submitting stage %s of pipeline %s'%(cur_stage,cur_task))
                task = self._manager.submit_units(tasks)
                return task

        except Exception, ex:
            self._logger.error("Could not execute tasks, error : %s"%(ex))
            raise
