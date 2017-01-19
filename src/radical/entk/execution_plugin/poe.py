__author__    = "Vivek Balasubramanian <vivek.balasubramanian@rutgers.edu>"
__copyright__ = "Copyright 2016, http://radical.rutgers.edu"
__license__   = "MIT"


from plugin_base import PluginBase
import radical.pilot as rp
import radical.utils as ru

from staging.input_data import get_input_data
from staging.output_data import get_output_data

import saga
import sys

_plugin_info = {
            'name': 'poe',
            'type': 'static'
        }

class PluginPoE(object):

    def __init__(self):

        self._executable_workload = list()
        self._resource = None
        self._manager = None

        self._logger = ru.get_logger("radical.entk.plugin.poe")
        self._reporter = self._logger.report

        self._logger.info("Plugin PoE created")


    def register_resource(self, resource):
        self._resource = resource
        self._logger.info("Registered resource %s with execution plugin"%(resource))

    def get_resources(self):
        return self._resource


    def set_workload(self, kernels):

        if type(kernels) != list:
            self._executable_workload = [kernels]
        else:
            self._executable_workload = kernels

        self._logger.info("New workload assigned to plugin for execution")


    def add_workload(self, kernels):

        if type(kernels) != list:
            self._executable_workload.append(kernels)
        else:
            self._executable_workload.extend(kernels)

        self._logger.info("New workload added to plugin for execution")

    def add_manager(self, manager):
        self._manager = manager
        self._logger.debug("Task execution manager (RP-Unit Manager) assigned to execution plugin")


    def execute_tasks(self, record, pattern_name, iteration, stage):
        
        try:
            self._manager.register_callback(unit_state_cb)

            cus = []

            inst=1

            for kernel in self._executable_workload:

                kernel._bind_to_resource(self._resource)
                rbound_kernel = kernel
                cud = rp.ComputeUnitDescription()
                cud.name = "stage-{0}-task-{1}".format(stage, inst)

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
                cud.output_staging     = get_output_data(
                                                        rbound_kernel, 
                                                        record, 
                                                        cur_pat = pattern_name, 
                                                        cur_iter= iteration, 
                                                        cur_stage = stage, 
                                                        cur_task=inst
                                                    )

                inst+=1

                cus.append(cud)
                self._logger.debug("Kernel %s converted into RP Compute Unit"%(kernel.name))

            exec_cus = self._manager.submit_units(cus)

            copy_exec_cus_A = exec_cus
            copy_exec_cus_B = exec_cus

            if pattern_name == "None":
                self._logger.info("Submitted %s tasks with kernel:%s of iteration:%s, stage:%s"\
                                                            %(  inst-1, 
                                                                rbound_kernel.name, 
                                                                iteration, 
                                                                stage
                                                            ))
            else:
                self._logger.info("Pattern %s: Submitted %s tasks with kernel:%s of "\
                                                        %( pattern_name,
                                                            inst-1, 
                                                            rbound_kernel.name
                                                        )+
                                    "iteration:%s, stage:%s"\
                                                        %(
                                                            iteration, 
                                                            stage 
                                                        ))


            exec_uids = [cu.uid for cu in exec_cus]

            self._logger.info("Waiting for completion of workload")
            self._manager.wait_units(exec_uids)
            self._logger.info("Workload execution successful")
            self._logger.debug("Stage %s execution completed"%(stage))

            return exec_cus

        except Exception, ex:

            self._logger.error('Task execution failed, error: %s'%(ex))
