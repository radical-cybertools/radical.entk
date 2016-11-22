__author__    = "Vivek Balasubramanian <vivek.balasubramanian@rutgers.edu>"
__copyright__ = "Copyright 2016, http://radical.rutgers.edu"
__license__   = "MIT"

from radical.entk.exceptions import *
from radical.entk.kernel_plugins.kernel_base import KernelBase
from radical.entk.kernel_plugins.kernel import Kernel
from radical.entk.execution_pattern import ExecutionPattern
from radical.entk.unit_patterns.poe.poe import PoE
from radical.entk.unit_patterns.eop.eop import EoP

import radical.utils as ru
import radical.pilot as rp
import sys

import Queue
import threading

# ------------------------------------------------------------------------------
#
class AppManager():

    # --------------------------------------------------------------------------
    #
    def __init__(self, name=None, on_error=None):

        self._name = name

        self._pattern = None
        self._loaded_kernels = list()
        self._loaded_plugins = list() 

        self._logger = ru.get_logger("radical.entk.appman")
        self._logger.info("Application Manager created")
        self._reporter = self._logger.report

        self._kernel_dict = dict()

        self._on_error = on_error   # 'exit' / 'terminate' / 'resubmit'
        if self._on_error==None:
            self._on_error = 'exit'


        self._callback_flag = False
        self._task_queue = Queue.Queue()
        self._fail_queue = Queue.Queue()
        self._task_event = threading.Event()
        self._fail_event = threading.Event()


    # --------------------------------------------------------------------------
    #
    def sanity_pattern_check(self):
        # AM: shouldn't this be:
        # if not isinstance(self.__pattern, ExecutionPattern)
        # ?  Otherwise this will break on non-trivial inheritance, because the
        # inheritance tree is hardcoded here...
        if self._pattern.__class__.__base__.__base__ != ExecutionPattern:
            raise TypeError(expected_type="(derived from) ExecutionPattern", 
                            actual_type=type(self._pattern))

    # --------------------------------------------------------------------------
    #
    @property
    def name(self):
        return self._name


    # --------------------------------------------------------------------------
    #
    def register_kernels(self, kernel_class):

        # AM: For code which is supposed to handle lists and single items,
        # please use the following:
        #
        # def method(things):
        #   if not (isinstance(things, list):
        #     things = [things]
        #   for thing in things:
        #     print thing
        #
        # that way you avoid code duplication for the two cases, and it is
        # easier to (a) maintain the code, and (b) see what the method is
        # supposed to do.

        # print type(kernel_class)
        try:
            # AM: using isinstance again
          # if type(kernel_class) == list:
            if isinstance(kernel_class, list):
                for item in kernel_class:
                    # AM: use isinstance ! :)  I am stopping those comments now.
                    if not hasattr(item, '__base__'):
                        raise TypeError(expected_type="KernelBase", actual_type = type(item))                   
                    elif item.__base__ != Kernel:
                        raise TypeError(expected_type="KernelBase", actual_type = type(item()))     

                    if item in self._loaded_kernels:
                        raise ExistsError(item='{0}'.format(item().name), parent = 'loaded_kernels')

                    self._loaded_kernels.append(item)
                    self._logger.info("Kernel {0} registered with application manager".format(item().name))

            elif not hasattr(kernel_class,'__base__'):
                raise TypeError(expected_type="KernelBase", actual_type = type(kernel_class))

            elif kernel_class.__base__ != KernelBase:
                raise TypeError(expected_type="KernelBase", actual_type = type(kernel_class()))

            else:
                self._loaded_kernels.append(kernel_class)
                self._logger.info("Kernel {0} registered with application manager".format(kernel_class().name))
        
        except Exception, ex:

                # AM: please use 'log.exception('error seen') in except clauses:
                # this will also log the error message *and* the stacktrace.
                self._logger.error("Kernel registration failed: {0}".format(ex))
                raise


    # --------------------------------------------------------------------------
    #
    def list_kernels(self):

        try:
            # AM: consider this version:
            #
            # return [item().name for item in self._loaded_kernels]
            #
            registered_kernels = list()
            for item in self._loaded_kernels:
                registered_kernels.append(item().name)

            return registered_kernels

        except Exception, ex:

            self._logger.error("Could not list kernels: {0}".format(ex))
            raise


    # --------------------------------------------------------------------------
    #
    def save(self, pattern):

        self._pattern = pattern
        self.sanity_pattern_check()

        # Convert pattern to JSON
        self.pattern_to_json(pattern)


    # --------------------------------------------------------------------------
    #
    def pattern_to_json(self, pattern):
        pass


    # --------------------------------------------------------------------------
    #
    def validate_kernel(self, user_kernel):

        try:

            # AM: please use
            #
            # if not user_kernel:
            #   return None
            #
            if user_kernel == None:
                return None

            found=False
            for kernel in self._loaded_kernels:

                if kernel().name == user_kernel.name:

                    found=True

                    new_kernel = kernel()

                    # AM: why can't that just be
                    #
                    # new_kernel.pre_exec = user_kernel.pre_exec
                    #
                    # In the worst case that will set it to None, so changes
                    # nothing?
                    if user_kernel.pre_exec != None:
                        new_kernel.pre_exec = user_kernel.pre_exec

                    if user_kernel.executable != None:
                        new_kernel.executable = user_kernel.executable

                    if user_kernel.arguments != None:
                        new_kernel.arguments = user_kernel.arguments

                    if user_kernel.uses_mpi != None:    
                        new_kernel.uses_mpi = user_kernel.uses_mpi

                    if user_kernel.cores != None:
                        new_kernel.cores = user_kernel.cores

                    if user_kernel.upload_input_data != None:
                        new_kernel.upload_input_data = user_kernel.upload_input_data

                    if user_kernel.copy_input_data != None:
                        new_kernel.copy_input_data = user_kernel.copy_input_data

                    if user_kernel.link_input_data != None:
                        new_kernel.link_input_data = user_kernel.link_input_data

                    if user_kernel.copy_output_data != None:
                        new_kernel.copy_output_data = user_kernel.copy_output_data

                    if user_kernel.download_output_data != None:
                        new_kernel.download_output_data = user_kernel.download_output_data

                    if user_kernel.timeout != None:
                        new_kernel.timeout = user_kernel.timeout

                    new_kernel.cancel_tasks = user_kernel.cancel_tasks

                    new_kernel.validate_arguments()

                    self._logger.debug("Kernel {0} validated".format(new_kernel.name))

                    return new_kernel

            if found==False:
                # your lines are too long.  For strings, you can consider to
                # switch to the % notation, which is shorter and easier to break
                # across lines:
                #
                # self._logger.error("Kernel %s does not exist" % user_kernel.name)
                # or
                # self._logger.error("Kernel %s does not exist" \
                #                   % user_kernel.name)
                self._logger.error("Kernel {0} does not exist".format(user_kernel.name))
                raise Exception()

        except Exception, ex:

            self._logger.error('Kernel validation failed: {0}'.format(ex))
            raise


    # --------------------------------------------------------------------------
    #
    def add_workload(self, pattern):

        self._pattern = pattern

        try:
            self.create_record(pattern.name, pattern.total_iterations, 
                               pattern.pipeline_size, pattern.ensemble_size)
        except Exception, ex:
            self._logger.error("Create new record function call for added pattern failed, error : {0}".format(ex))
            raise


    # --------------------------------------------------------------------------
    #
    def add_to_record(self, pattern_name, record, cus, iteration, stage, 
                      instance=None, monitor=False, status=None):
            
        try:

            # Differences between EoP and PoE
            if instance==None:
                #PoE
                inst=1
            else:
                #EoP
                inst=instance

            if type(cus) != list:
                cus = [cus]

            if status==None:
                status='Pending'

            pat_key = "pat_{0}".format(pattern_name)

            self._logger.debug('Adding Tasks to record')

            for cu in cus:

                itername     = "iter_%s"     % iteration
                stagename    = "stage_%s"    % stage
                instancename = "instance_%s" % inst

                if itername not in record[pat_key]:
                    record[pat_key][itername] = dict()

                if stagename not in record[pat_key][itername]:
                    record[pat_key][itername][stagename] = dict()

                if instancename not in record[pat_key][itername][stagename]:
                    record[pat_key][itername][stagename]['branch'] = \
                            record[pat_key]["iter_1"][stagename]['branch']

                record[pat_key][itername][stagename][instancename] = {
                        "output" : cu.stdout,
                        "uid"    : cu.uid,
                        "path"   : cu.working_directory,
                        'status' : status 
                        }

              # if "iter_{0}".format(iteration) not in record[pat_key]:
              #     record[pat_key]["iter_{0}".format(iteration)] = dict()
              #
              # if "stage_{0}".format(stage) not in record[pat_key]["iter_{0}".format(iteration)]:
              #     record[pat_key]["iter_{0}".format(iteration)]["stage_{0}".format(stage)] = dict()
              #
              # if "instance_{0}".format(inst) not in record[pat_key]["iter_{0}".format(iteration)]["stage_{0}".format(stage)]:
              #     record[pat_key]["iter_{0}".format(iteration)]["stage_{0}".format(stage)]["instance_{0}".format(inst)] = dict()
              #     record[pat_key]["iter_{0}".format(iteration)]["stage_{0}".format(stage)]['branch'] = record[pat_key]["iter_1"]["stage_{0}".format(stage)]['branch']
              #
              # record[pat_key]["iter_{0}".format(iteration)]["stage_{0}".format(stage)]["instance_{0}".format(inst)]["output"] = cu.stdout
              # record[pat_key]["iter_{0}".format(iteration)]["stage_{0}".format(stage)]["instance_{0}".format(inst)]["uid"] = cu.uid
              # record[pat_key]["iter_{0}".format(iteration)]["stage_{0}".format(stage)]["instance_{0}".format(inst)]["path"] = cu.working_directory
              # record[pat_key]["iter_{0}".format(iteration)]["stage_{0}".format(stage)]["instance_{0}".format(inst)]['status'] = status

                inst+=1

            #stage_done=True

            #if instance==None:
            #    inst=1
            #else:
            #    inst=instance

            #for cu in cus:
            #    val = record[pat_key]["iter_{0}".format(iteration)]["stage_{0}".format(stage)]["instance_{0}".format(inst)]['path']
            #    inst+=1
            #    if (val==None):
            #        stage_done=False
            #        break

            #if stage_done:
            #    record[pat_key]["iter_{0}".format(iteration)]["stage_{0}".format(stage)]['status'] = 'Running'
                
            return record

        except Exception, ex:
            self._logger.error("Could not add new CU data to record, error: {0}".format(ex))
            raise


    # --------------------------------------------------------------------------
    #
    def get_record(self):
        return self._kernel_dict


    # --------------------------------------------------------------------------
    #
    def create_record(self, pattern_name, total_iterations, pipeline_size, 
                      ensemble_size):


        try:

            pat_key = "pat_{0}".format(pattern_name)
            self._kernel_dict[pat_key] = dict()

            # AM: define itername, stagename, instancename!
            # The code below is very hard to read, and repeated string expansion
            # is slow!  And lines are long :P

            for iter in range(1, total_iterations+1):
                self._kernel_dict[pat_key]["iter_{0}".format(iter)] = dict()

                for stage in range(1, pipeline_size+1):
                    self._kernel_dict[pat_key]["iter_{0}".format(iter)]["stage_{0}".format(stage)]  = dict()                    

                    # Set available branches
                    if getattr(self._pattern,'branch_{0}'.format(stage), False):
                        self._kernel_dict[pat_key]["iter_{0}".format(iter)]["stage_{0}".format(stage)]['branch'] = True
                    else:
                        self._kernel_dict[pat_key]["iter_{0}".format(iter)]["stage_{0}".format(stage)]['branch'] = False
        
                    # Create instance key/vals for each stage
                    if type(ensemble_size) == int:
                        instances = ensemble_size
                    elif type( ensemble_size) == list:
                        instances = ensemble_size[stage-1]

                    for inst in range(1, instances+1):
                        self._kernel_dict[pat_key]["iter_{0}".format(iter)]["stage_{0}".format(stage)]["instance_{0}".format(inst)] = dict()
                        self._kernel_dict[pat_key]["iter_{0}".format(iter)]["stage_{0}".format(stage)]["instance_{0}".format(inst)]["output"] = None
                        self._kernel_dict[pat_key]["iter_{0}".format(iter)]["stage_{0}".format(stage)]["instance_{0}".format(inst)]["uid"] = None
                        self._kernel_dict[pat_key]["iter_{0}".format(iter)]["stage_{0}".format(stage)]["instance_{0}".format(inst)]["path"] = None
                        # Set kernel default status
                        self._kernel_dict[pat_key]["iter_{0}".format(iter)]["stage_{0}".format(stage)]["instance_{0}".format(inst)]['status'] = 'Pending'

        except Exception, ex:

            self._logger.error("New record creation failed, error: {0}".format(ex))
            raise


    # --------------------------------------------------------------------------
    #
    def get_record(self):
        return self._kernel_dict


    # --------------------------------------------------------------------------
    #
    def run(self, resource, task_manager, rp_session):

        try:
            # Create dictionary for logging
            record = self.get_record()

            # For data transfer, inform pattern of the resource
            self._pattern.session_id = rp_session

            if self._pattern.__class__.__base__ == PoE:
    
                # Based on the execution pattern, the app manager should choose the execution plugin
                try:
                    from radical.entk.execution_plugin.poe import PluginPoE

                    plugin = PluginPoE()                
                    plugin.register_resource(resource = resource)
                    plugin.add_manager(task_manager)

                except Exception, ex:
                    self._logger.error("PoE Plugin setup failed, error: {0}".format(ex))


                try:
                    # Submit kernels stage by stage to execution plugin
                    while(self._pattern.cur_iteration <= self._pattern.total_iterations):
            
                        #for self._pattern.next_stage in range(1, self._pattern.pipeline_size+1):
                        while ((self._pattern.next_stage<=self._pattern.pipeline_size)and(self._pattern.next_stage!=0)):

                            # Get kernel from execution pattern
                            stage =  self._pattern.get_stage(stage=self._pattern.next_stage)

                            validated_kernels = list()
                            validated_monitors = list()

                            # Validate user specified Kernel with KernelBase and return fully defined but resource-unbound kernel
                            # Create instance key/vals for each stage
                            if type(self._pattern.ensemble_size) == int:
                                instances = self._pattern.ensemble_size
                            elif type(self._pattern.ensemble_size) == list:
                                instances = self._pattern.ensemble_size[self._pattern.next_stage-1]

                            # Initialization
                            stage_monitor = None

                            for inst in range(1, instances+1):

                                stage_instance_return = stage(inst)

                                if type(stage_instance_return) == list:
                                    if len(stage_instance_return) == 2:
                                        stage_kernel = stage_instance_return[0]
                                        stage_monitor = stage_instance_return[1]
                                    else:
                                        stage_kernel = stage_instance_return[0]
                                        stage_monitor = None
                                else:
                                    stage_kernel = stage_instance_return
                                    stage_monitor = None
                                    
                                validated_kernels.append(self.validate_kernel(stage_kernel))
                            validated_monitor = self.validate_kernel(stage_monitor)


                            # Pass resource-unbound kernels to execution plugin
                            #print len(list_kernels_stage)
                            plugin.set_workload(kernels=validated_kernels, monitor=validated_monitor)
                            cus = plugin.execute(record=record, pattern_name=self._pattern.name, iteration=self._pattern.cur_iteration, stage=1)

                            # Update record
                            record = self.add_to_record(record=record, cus=cus, pattern_name = self._pattern.name, iteration=self._pattern.cur_iteration, stage=self._pattern.next_stage)

                            # Check if montior exists
                            if plugin.monitor != None:
                                cu = plugin.execute_monitor(record=record, tasks=cus, cur_pat=self._pattern.name, cur_iter=self._pattern.cur_iteration, cur_stage=self._pattern.next_stage)
                                
                                # Update record
                                record = self.add_to_record(record=record, cus=cu, pattern_name = self._pattern.name, iteration=self._pattern.cur_iteration, stage=self._pattern.next_stage, monitor=True)

                            self._pattern.pattern_dict = record["pat_{0}".format(self._pattern.name)] 

                            #print record
                            branch_function = None

                            # Execute branch if it exists
                            if (record["pat_{0}".format(self._pattern.name)]["iter_{0}".format(self._pattern.cur_iteration)]["stage_{0}".format(self._pattern.next_stage)]["branch"]):
                                self._logger.info('Executing branch function branch_{0}'.format(self._pattern.next_stage))
                                branch_function = self._pattern.get_branch(stage=self._pattern.next_stage)
                                branch_function()

                            #print self._pattern.stage_change
                            if (self._pattern.stage_change==True):
                                pass
                            else:
                                self._pattern.next_stage+=1

                            self._pattern.stage_change = False

                            # Terminate execution
                            if self._pattern.next_stage == 0:
                                self._logger.info("Branching function has set termination condition -- terminating")
                                break
                    
                        # Terminate execution
                        if self._pattern.next_stage == 0:
                            break

                        self._pattern.cur_iteration+=1

                except Exception, ex:
                    self._logger.error("PoE Workload submission failed, error: {0}".format(ex))
                    raise


            # App Manager actions for EoP pattern
            if self._pattern.__class__.__base__ == EoP:
    
                # Based on the execution pattern, the app manager should choose the execution plugin
                try:

                    from radical.entk.execution_plugin.eop import PluginEoP

                    plugin = PluginEoP()                
                    plugin.register_resource(resource = resource)
                    plugin.add_manager(task_manager)
                    num_stages = self._pattern.pipeline_size
                    num_tasks = self._pattern.ensemble_size

                    # List of all CUs
                    all_cus = []
                    lock_all_cus = threading.Lock()

                except Exception, ex:
                    self._logger.error("Plugin setup failed, error: {0}".format(ex))
                    raise


                try:

                    def execute_thread():

                        #while self._task_event.isSet()==False:
                        while True:

                            try:

                                record=self.get_record()
                                unit = self._task_queue.get()

                                if unit == 'quit':
                                    return

                                cur_stage = int(unit.name.split('-')[1])
                                cur_task = int(unit.name.split('-')[3])

                                with lock_all_cus:
                                    plugin.tot_fin_tasks[cur_stage-1]+=1
                                    self._logger.info('Tot_fin_tasks from thread: {0}'.format(plugin.tot_fin_tasks))

                                self._logger.info('Stage {1} of pipeline {0} has finished'.format(cur_task,cur_stage))
                                                                
                                # Execute branch if it exists
                                if (record["pat_{0}".format(self._pattern.name)]["iter_{0}".format(self._pattern.cur_iteration[cur_task-1])]["stage_{0}".format(cur_stage)]["branch"]):
                                    self._logger.info('Executing branch function branch_{0}'.format(cur_stage))
                                    branch_function = self._pattern.get_branch(stage=cur_stage)
                                    branch_function(instance=cur_task) 


                                with lock_all_cus:

                                    if (self._pattern.stage_change==True):
                                        if self._pattern.new_stage !=0:
                                            if cur_stage < self._pattern.new_stage:
                                                self._pattern._incremented_tasks[cur_task-1] -= self._pattern.new_stage - cur_stage - 1
                                            elif cur_stage >= self._pattern.new_stage:
                                                self._pattern._incremented_tasks[cur_task-1] -= abs(cur_stage - self._pattern.pipeline_size)
                                                self._pattern._incremented_tasks[cur_task-1] += abs(self._pattern.pipeline_size - self._pattern.new_stage) + 1
                                            
                                        else:
                                            self._pattern._incremented_tasks[cur_task-1] -= abs(cur_stage - self._pattern.pipeline_size)

                                        if self._pattern.next_stage[cur_task-1] >= self._pattern.new_stage:
                                            self._pattern.cur_iteration[cur_task-1] += 1

                                        self._pattern.next_stage[cur_task-1] = self._pattern.new_stage
                                    else:
                                        self._pattern.next_stage[cur_task-1] +=1


                                self._pattern.stage_change = False
                                self._pattern.new_stage = None

                                # Terminate execution
                                if self._pattern.next_stage[cur_task-1] == 0:
                                    self._logger.info("Branching function has set termination condition -- terminating pipeline {0}".format(cur_task))


                                # Check if this is the last task of the stage
                                with lock_all_cus:
                                    if plugin.tot_fin_tasks[cur_stage-1] == self._pattern.ensemble_size:
                                        self._logger.info('Stage {0} of all pipelines has finished'.format(cur_stage))


                                if ((self._pattern.next_stage[cur_task-1]<= self._pattern.pipeline_size)and(self._pattern.next_stage[cur_task-1] !=0)):
                                
                                    stage =     self._pattern.get_stage(stage=self._pattern.next_stage[cur_task-1])
                                    stage_kernel = stage(cur_task)
                                    
                                    validated_kernel = self.validate_kernel(stage_kernel)                                                                       


                                    plugin.set_workload(kernels=validated_kernel, cur_task=cur_task)
                                    cud = plugin.create_tasks(record=record, pattern_name=self._pattern.name, iteration=self._pattern.cur_iteration[cur_task-1], stage=self._pattern.next_stage[cur_task-1], instance=cur_task)                
                                    cu = plugin.execute_tasks(tasks=cud)

                                    with lock_all_cus:

                                        if cu!= None:
                                            all_cus.append(cu)

                                    if cur_stage==2:
                                        print 'stage 3 submitted'

                                with lock_all_cus:
                                    self._logger.info('All cus from thread: {0}'.format(len(all_cus)))


                                #self._task_queue.task_done()

                            except Exception, ex:
                                self._logger.error('Failed to run next stage, error: {0}'.format(ex))
                                raise


                    def fail_thread():

                        #while self._fail_event.isSet() == False:
                        while True:

                            try:

                                record=self.get_record()
                                unit = self._fail_queue.get()

                                if unit == 'quit':
                                    return

                                cur_stage = int(unit.name.split('-')[1])
                                cur_task = int(unit.name.split('-')[3])

                                if self._on_error == 'resubmit':                                        
                                    
                                    new_unit = plugin.execute_tasks(unit.description)

                                    with lock_all_cus:
                                        all_cus.append(new_unit)
                                        all_cus.remove(unit)

                                elif self._on_error == 'terminate':

                                    record=self.add_to_record(record=record, cus=unit, pattern_name = self._pattern.name, iteration=self._pattern.cur_iteration[cur_task-1], stage=cur_stage, instance=cur_task, status='Failed')
                                    self._pattern.pattern_dict = record["pat_{0}".format(self._pattern.name)]
                                    plugin.tot_fin_tasks[cur_stage-1]+=1

                                    with lock_all_cus:
                                        all_cus.remove(unit)


                                elif self._on_error == 'recreate':

                                    stage =  self._pattern.get_stage(stage=self._pattern.next_stage[cur_task-1])
                                    stage_kernel = stage(cur_task)

                                    validated_kernel = self.validate_kernel(stage_kernel)

                                    plugin.set_workload(kernels=validated_kernel, cur_task=cur_task)
                                    cud = plugin.create_tasks(record=record, pattern_name=self._pattern.name, iteration=self._pattern.cur_iteration[cur_task-1], stage=self._pattern.next_stage[cur_task-1], instance=cur_task)                
                                    cu = plugin.execute_tasks(tasks=cud)

                                    with lock_all_cus:

                                        if cu!= None:
                                            all_cus.append(cu)

                                        all_cus.remove(unit)


                                elif self._on_error == 'continue':
                                        
                                    record = self.add_to_record(record=record, cus=unit, pattern_name = self._pattern.name, iteration=self._pattern.cur_iteration[cur_task-1], stage=cur_stage, instance=cur_task, status='Failed')
                                    self._pattern.pattern_dict = record["pat_{0}".format(self._pattern.name)]
                                    self._task_queue.put(unit)

                                    with lock_all_cus:
                                        all_cus.remove(unit)

                                else:

                                    pass


                                #self._fail_queue.task_done()

                            except Exception, ex:
                                self._logger.error('Failed to handle failed task, error: {0}'.format(ex))
                                raise



                    def unit_state_cb (unit, state) :

                        record = self.get_record()

                        self._logger.debug('Callback initiated for {0}, state: {1}'.format(unit.name, state))

                        # Perform these operations only for tasks and not monitors
                        if unit.name.startswith('stage'):                            

                            cur_stage = int(unit.name.split('-')[1])
                            cur_task = int(unit.name.split('-')[3])

                            if state == rp.FAILED:

                                if self._on_error == 'resubmit':

                                    self._logger.error("Stage {0} of pipeline {1} failed: UID: {2}, STDERR: {3}, STDOUT: {4} LAST LOG: {5}".format(cur_stage, cur_task, unit.uid, unit.stderr, unit.stdout, unit.log[-1]))
                                    self._logger.info("Resubmitting stage {0} of pipeline {1}...".format(cur_stage, cur_task))
                                    self._fail_queue.put(unit)

                                elif self._on_error == 'exit':
                                    self._logger.error("Stage {0} of pipeline {1} failed: UID: {2}, STDERR: {3}, STDOUT: {4} LAST LOG: {5}".format(cur_stage, cur_task, unit.uid, unit.stderr, unit.stdout, unit.log[-1]))
                                    self._logger.info("Exiting ...")

                                    sys.exit(1)

                                elif self._on_error == 'terminate':
                                    self._logger.error("Stage {0} of pipeline {1} failed: UID: {2}, STDERR: {3}, STDOUT: {4} LAST LOG: {5}".format(cur_stage, cur_task, unit.uid, unit.stderr, unit.stdout, unit.log[-1]))
                                    self._logger.info("Terminating pipeline ...")

                                    self._fail_queue.put(unit)

                                    return


                                elif self._on_error == 'recreate':

                                    self._logger.error("Stage {0} of pipeline {1} failed: UID: {2}, STDERR: {3}, STDOUT: {4} LAST LOG: {5}".format(cur_stage, cur_task, unit.uid, unit.stderr, unit.stdout, unit.log[-1]))
                                    self._logger.info("Recreating stage {0} of pipeline {1}...".format(cur_stage, cur_task))

                                    self._fail_queue.put(unit)

                                    return

                                elif self._on_error == 'continue':

                                    self._logger.error("Stage {0} of pipeline {1} failed: UID: {2}, STDERR: {3}, STDOUT: {4} LAST LOG: {5}".format(cur_stage, cur_task, unit.uid, unit.stderr, unit.stdout, unit.log[-1]))
                                    self._logger.info("Continuing ahead...".format(cur_stage, cur_task))

                                    #self._fail_queue.put(unit)

                                    record = self.add_to_record(record=record, cus=unit, pattern_name = self._pattern.name, iteration=self._pattern.cur_iteration[cur_task-1], stage=cur_stage, instance=cur_task, status='Failed')
                                    self._pattern.pattern_dict = record["pat_{0}".format(self._pattern.name)]
                                    self._task_queue.put(unit)

                                    with lock_all_cus:
                                        all_cus.remove(unit)

                                    return

                            elif ((state == rp.DONE)or(state==rp.CANCELED)):

                                try:
                                    cur_stage = int(unit.name.split('-')[1])
                                    cur_task = int(unit.name.split('-')[3])                                    

                                    with lock_all_cus:
                                        all_cus.remove(unit)

                                    record=self.get_record()                                    
                                    record = self.add_to_record(record=record, cus=unit, pattern_name = self._pattern.name, iteration=self._pattern.cur_iteration[cur_task-1], stage=cur_stage, instance=cur_task, status='Done')
                                    self._pattern.pattern_dict = record["pat_{0}".format(self._pattern.name)] 

                                    self._task_queue.put(unit)
                                    
                                except Exception, ex:
                                    self._logger.error('Failed to push to task queue, error: {0}'.format(ex))
                                    raise

                    #register callbacks if not done already

                    if self._callback_flag != True:
                        task_manager.register_callback(unit_state_cb)
                        self._callback_flag = True

                    # Get kernel from execution pattern
                    stage = self._pattern.get_stage(stage=1)

                    validated_kernels = list()

                    # Validate user specified Kernel with KernelBase and return fully defined but resource-unbound kernel
                    # Create instance key/vals for each stage
                    
                    instances = self._pattern.ensemble_size
                    
                    for inst in range(1, instances+1):

                        stage_kernel = stage(inst)
                        validated_kernels.append(self.validate_kernel(stage_kernel))


                    # Pass resource-unbound kernels to execution plugin
                    plugin.set_workload(kernels=validated_kernels)
                    cus = plugin.create_tasks(record=record, pattern_name=self._pattern.name, iteration=1, stage=1)
                    cus = plugin.execute_tasks(tasks=cus)

                    # Start execute_thread
                    t1 = threading.Thread(target=execute_thread, args=())
                    t1.start()


                    # Start thread to handle failure
                    #t2 = threading.Thread(target=fail_thread, args=())
                    #t2.start()


                    with lock_all_cus:
                        if cus!=None:
                            all_cus.extend(cus)

                    quit=False


                    while not quit:
                    
                        pending_cus_1 = []
                        pending_cus_2 = []
                        done_cus = []

                        print 'all cus beg: {0}'.format(len(all_cus))

                        with lock_all_cus:

                            for unit in all_cus:
                                if (unit.state == rp.DONE)or(unit.state == rp.CANCELED)or(unit.state==rp.FAILED):
                                    done_cus.append(unit)
                                else:
                                    pending_cus_1.append(unit.uid)


                            all_cu_uids = task_manager.list_units()
                            all_cus_2 = task_manager.get_units(all_cu_uids)

                            for unit in all_cus_2:
                                if (unit.state!=rp.DONE)and(unit.state!=rp.CANCELED)and(unit.state!=rp.FAILED):
                                    pending_cus_2.append(unit.uid)

                            for unit in done_cus:
                                all_cus.remove(unit)

                        print 'tot_fin_tasks: {0}'.format(plugin.tot_fin_tasks)
                        print 'all cus end: {0}'.format(len(all_cus))
                        print 'pending cus 1: {0}, {1}'.format(len(pending_cus_1), pending_cus_1)
                        print 'pending cus 2: {0}, {1}'.format(len(pending_cus_2), pending_cus_2)
                        print 'done cus: {0}'.format(len(done_cus))

                        #task_manager.wait_units(pending_cus_2, timeout=60) 

                        import time
                        time.sleep(30)

                        print sum(plugin.tot_fin_tasks)
                        print (self._pattern.pipeline_size*self._pattern.ensemble_size + sum(self._pattern._incremented_tasks) )

                        with lock_all_cus:
                            if (sum(plugin.tot_fin_tasks)==(self._pattern.pipeline_size*self._pattern.ensemble_size + sum(self._pattern._incremented_tasks) )):
                                quit=True



                    #if t2.isAlive() == True:
                        #self._fail_event.set()                        
                    #    self._fail_queue.put('quit')
                    #t2.join()

                    if t1.isAlive() == True:
                        #self._task_event.set()
                        self._task_queue.put('quit')
                    t1.join()
                    



                except Exception, ex:
                    self._logger.error("EoP Pattern execution failed, error: {0}".format(ex))
                    raise

        except Exception, ex:
            self._logger.error("App manager failed at workload execution, error: {0}".format(ex))
            raise
