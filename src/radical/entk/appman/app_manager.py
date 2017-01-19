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

        self._dh = ru.DebugHelper()

        self._name = name

        self._pattern = None
        self._loaded_kernels = list()
        self._loaded_plugins = list() 

        self._logger = ru.get_logger("radical.entk.appman")
        self._logger.info("Application Manager created")
        self._reporter = self._logger.report

        self._kernel_dict = dict()

        self._on_error = on_error   # 'exit' / 'terminate' / 'resubmit'
                                    # 'continue' / 'recreate'

        if not self._on_error:
            self._on_error = 'exit'


        self._callback_flag = False
        self._task_queue = Queue.Queue()
        self._fail_queue = Queue.Queue()

        # List of all CUs
        self.all_cus = list()
        self.all_cus_lock = threading.Lock()

        self._uid               = ru.generate_id('entk.appmanager')
        self._prof              = ru.Profiler('%s' % self._uid)

        self._prof.prof('Instantiated', uid=self._uid)

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

            self._prof.prof('registering kernels', uid=self._uid)

            if isinstance(kernel_class, list):
                for item in kernel_class:
                    if not hasattr(item, '__base__'):
                        raise TypeError(expected_type="KernelBase", actual_type = type(item))                   
                    elif item.__base__ != Kernel:
                        raise TypeError(expected_type="KernelBase", actual_type = type(item()))     

                    if item in self._loaded_kernels:
                        raise ExistsError(item=item().name, parent = 'loaded_kernels')

                    self._loaded_kernels.append(item)
                    self._logger.info("Kernel {0} registered with application manager" \
                                                                            %(item().name))

            elif not hasattr(kernel_class,'__base__'):
                raise TypeError(expected_type="KernelBase", actual_type = type(kernel_class))

            elif kernel_class.__base__ != KernelBase:
                raise TypeError(expected_type="KernelBase", actual_type = type(kernel_class()))

            else:
                self._loaded_kernels.append(kernel_class)
                self._logger.info("Kernel %s registered with application manager" \
                                                                        %(kernel_class().name))

            
            self._prof.prof('kernels registered', uid=self._uid)            

            
        
        except Exception, ex:

                self._logger.exception("Kernel registration failed: %s"%(ex))
                raise


    # --------------------------------------------------------------------------
    #
    def list_kernels(self):

        try:

            return [item().name for item in self._loaded_kernels]

        except Exception, ex:

            self._logger.exception("Could not list kernels: %s"%(ex))
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

            if not user_kernel:
                return None

            found=False
            for kernel in self._loaded_kernels:

                if kernel().name == user_kernel.name:

                    found=True

                    new_kernel = kernel()
                    
                    new_kernel.pre_exec     = user_kernel.pre_exec
                    new_kernel.post_exec    = user_kernel.post_exec
                    new_kernel.executable   = user_kernel.executable
                    new_kernel.arguments    = user_kernel.arguments
                    new_kernel.uses_mpi     = user_kernel.uses_mpi          
                    new_kernel.cores        = user_kernel.cores

                    new_kernel.upload_input_data    = user_kernel.upload_input_data
                    new_kernel.copy_input_data      = user_kernel.copy_input_data
                    new_kernel.link_input_data      = user_kernel.link_input_data
                    new_kernel.copy_output_data     = user_kernel.copy_output_data
                    new_kernel.download_output_data = user_kernel.download_output_data    

                    new_kernel.validate_arguments()

                    self._logger.debug("Kernel %s validated"%(new_kernel.name))

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
                self._logger.error("Kernel %s does not exist" % user_kernel.name)
                raise Exception()

        except Exception, ex:

            self._logger.exception('Kernel validation failed: %s'%(ex))
            raise


    # --------------------------------------------------------------------------
    #
    def add_workload(self, pattern):

        self._pattern = pattern

        try:

            self._prof.prof('creating record', uid=self._uid)

            self.create_record(pattern.name, pattern.total_iterations, 
                               pattern.pipeline_size, pattern.ensemble_size)

            self._prof.prof('record created', uid=self._uid)

        except Exception, ex:
            self._logger.exception("Create new record function call for added"\
                            " pattern failed, error : %s"%ex)
            raise


    # --------------------------------------------------------------------------
    #
    def add_to_record(self, pattern_name, record, cus, iteration, stage, 
                      instance=None, monitor=False, status=None):
            
        try:

            # Differences between EoP and PoE
            if not instance:
                #PoE
                inst=1
            else:
                #EoP
                inst=instance

            if not isinstance(cus, list):
                cus = [cus]

            if not status:
                status='Pending'

            pat_key = "pat_%s"%(pattern_name)

            self._logger.debug('Adding Tasks to record')

            for cu in cus:

                iter_name     = "iter_%s"     % iteration
                stage_name    = "stage_%s"    % stage
                instance_name = "instance_%s" % inst

                if iter_name not in record[pat_key]:
                    record[pat_key][iter_name] = dict()

                if stage_name not in record[pat_key][iter_name]:
                    record[pat_key][iter_name][stage_name] = dict()

                if instance_name not in record[pat_key][iter_name][stage_name]:
                    record[pat_key][iter_name][stage_name]['branch'] = \
                            record[pat_key]["iter_1"][stage_name]['branch']

                record[pat_key][iter_name][stage_name][instance_name] = {
                        "output" : cu.stdout,
                        "uid"    : cu.uid,
                        "path"   : cu.working_directory,
                        'status' : status 
                        }              

                inst+=1
                
            return record

        except Exception, ex:
            self._logger.exception("Could not add new CU data to record, error: %s" \
                %ex)
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

            pat_key = "pat_%s"%(pattern_name)
            self._kernel_dict[pat_key] = dict()         

            for iter in range(1, total_iterations+1):

                iter_name     = "iter_%s"     % iter
                self._kernel_dict[pat_key][iter_name] = dict()

                for stage in range(1, pipeline_size+1):

                    stage_name    = "stage_%s"    % stage
                    self._kernel_dict[pat_key][iter_name][stage_name]  = dict()                    

                    # Set available branches
                    branch_name = "branch_%s" % stage
                    if getattr(self._pattern,branch_name, False):
                        self._kernel_dict[pat_key][iter_name][stage_name]['branch'] = True
                    else:
                        self._kernel_dict[pat_key][iter_name][stage_name]['branch'] = False
        
                    # Create instance key/vals for each stage
                    if isinstance(ensemble_size, int):
                        instances = ensemble_size
                    else:
                        instances = ensemble_size[stage-1]

                    for inst in range(1, instances+1):

                        instance_name = "instance_%s" % inst 
                        self._kernel_dict[pat_key][iter_name][stage_name][instance_name] = {
                            "output": None,
                            "uid": None,
                            "path": None,
                            "status": "Pending"
                        }

        except Exception, ex:

            self._logger.exception("New record creation failed, error: %s" % ex)
            raise


    # --------------------------------------------------------------------------
    #
    def get_record(self):
        return self._kernel_dict


    # --------------------------------------------------------------------------
    #
    def exec_poe(self, resource, task_manager, rp_session):

        # Based on the execution pattern, the app manager should choose the execution plugin
        try:
            from radical.entk.execution_plugin.poe import PluginPoE


            self._prof.prof('poe setup started', uid=self._uid)

            plugin = PluginPoE()                
            plugin.register_resource(resource = resource)
            plugin.add_manager(task_manager)
            record = self.get_record()

            self._prof.prof('poe setup done', uid=self._uid)

        except Exception, ex:
            self._logger.exception("PoE Plugin setup failed, error: %s" % ex)


        try:


            self._prof.prof('poe task launch started', uid=self._uid)

            # register callbacks if not done already
            if not self._callback_flag:
                task_manager.register_callback(self._unit_state_cb_poe)
                self._callback_flag = True

            # Submit kernels stage by stage to execution plugin
            while ((self._pattern.cur_iteration <= self._pattern.total_iterations) \
                and(self._pattern.next_stage!=0)):
        
                #for self._pattern.next_stage in range(1, self._pattern.pipeline_size+1):
                while ((self._pattern.next_stage<=self._pattern.pipeline_size) \
                    and(self._pattern.next_stage!=0)):

                    # Get kernel from execution pattern
                    self._prof.prof('iteration: %s, stage:%s creation started' \
                                                %(self._pattern.cur_iteration, 
                                                    self._pattern.next_stage), uid=self._uid)


                    stage =  self._pattern.get_stage(stage=self._pattern.next_stage)

                    validated_kernels = list()

                    # Validate user specified Kernel with KernelBase and return fully 
                    # defined but resource-unbound kernel.

                    if isinstance(self._pattern.ensemble_size, int):
                        instances = self._pattern.ensemble_size
                    else:
                        instances = self._pattern.ensemble_size[self._pattern.next_stage-1]

                    for inst in range(1, instances+1):

                        stage_kernel = stage(inst)                                               
                        validated_kernels.append(self.validate_kernel(stage_kernel))

                    # Pass resource-unbound kernels to execution plugin
                    #print len(list_kernels_stage)
                    plugin.set_workload(kernels=validated_kernels)

                    self._prof.prof('iteration: %s, stage:%s submission started' \
                                                %(self._pattern.cur_iteration, 
                                                    self._pattern.next_stage), uid=self._uid)

                    cus = plugin.execute_tasks(   record=record, 
                                            pattern_name=self._pattern.name, 
                                            iteration=self._pattern.cur_iteration, 
                                            stage=self._pattern.next_stage
                                        )

                    self._prof.prof('iteration: %s, stage:%s submission done' \
                                                %(self._pattern.cur_iteration, 
                                                    self._pattern.next_stage), uid=self._uid)

                    # Update record
                    record = self.add_to_record(    record=record, 
                                                    cus=cus, 
                                                    pattern_name = self._pattern.name, 
                                                    iteration=self._pattern.cur_iteration, 
                                                    stage=self._pattern.next_stage
                                                )                 

                    self._pattern.pattern_dict = record["pat_%s"%(self._pattern.name)] 

                    #print record
                    branch_function = None

                    # Execute branch if it exists
                    pattern_name    = "pat_%s" % self._pattern.name
                    iter_name       = "iter_%s" % self._pattern.cur_iteration
                    stage_name      = "stage_%s" % self._pattern.next_stage   

                    if (record[pattern_name][iter_name][stage_name]["branch"]):
                        self._logger.info('Executing branch function branch_%s'\
                                                %self._pattern.next_stage)

                        branch_function = self._pattern.get_branch(stage=self._pattern.next_stage)
                        branch_function()

                    #print self._pattern.stage_change
                    if self._pattern.stage_change:
                        pass
                    else:
                        self._pattern.next_stage+=1

                    self._pattern.stage_change = False

                    # Terminate execution
                    if self._pattern.next_stage == 0:
                        self._logger.info("Branching function has set termination "+
                                            "condition -- terminating")
                        break

                    self._prof.prof('iteration: %s, stage:%s creation done' \
                                                %(self._pattern.cur_iteration, 
                                                    self._pattern.next_stage), uid=self._uid)
            
                # Terminate execution
                if self._pattern.next_stage == 0:
                    break

                self._pattern.cur_iteration+=1

            self._prof.prof('poe task launch done', uid=self._uid)

        except Exception, ex:
            self._logger.exception("PoE Workload submission failed, error: %s"%(ex))
            raise


    def _unit_state_cb_poe (self, unit, state) :

        cur_stage = int(unit.name.split('-')[1])
        cur_task = int(unit.name.split('-')[3])
        cur_iter = self._pattern.cur_iteration


        if state == rp.FAILED:
            self._logger.error("Stage %s of pipeline %s failed: "%(cur_stage, 
                                                                    cur_task)+

                                "UID: %s, STDERR: %s, STDOUT: %s "%(unit.uid, 
                                                                    unit.stderr, 
                                                                    unit.stdout)+

                                "LAST LOG: %s"%unit.log[-1])

            self._logger.info("Exiting ...")
            sys.exit(1)

    # --------------------------------------------------------------------------
    #
    def exec_eop(self, resource, task_manager, rp_session):

        record = self.get_record()

        # Add threading event objects here so that upon another run() call event is not already
        # over.
        
        self._task_event = threading.Event()
        self._fail_event = threading.Event()

        # Based on the execution pattern, the app manager should choose the execution plugin
        try:

            from radical.entk.execution_plugin.eop import PluginEoP

            self._prof.prof('eop setup started', uid=self._uid)

            plugin = PluginEoP()                
            plugin.register_resource(resource = resource)
            plugin.add_manager(task_manager)
            num_stages = self._pattern.pipeline_size
            num_tasks = self._pattern.ensemble_size

            self._prof.prof('eop setup done', uid=self._uid)

        except Exception, ex:
            self._logger.exception("Plugin setup failed, error: %s"%(ex))
            raise


        try:

            # register callbacks if not done already
            if not self._callback_flag:
                task_manager.register_callback(self._unit_state_cb_eop)
                self._callback_flag = True

            self._prof.prof('eop, iteration: 1, stage: 1, task: 1, starting', uid=self._uid)

            # Get kernel from execution pattern
            stage = self._pattern.get_stage(stage=1)

            validated_kernels = list()

            # Validate user specified Kernel with KernelBase and return fully 
            # defined but resource-unbound kernel
            
            instances = self._pattern.ensemble_size
            
            for inst in range(1, instances+1):

                stage_kernel = stage(inst)
                validated_kernels.append(self.validate_kernel(stage_kernel))


            # Pass resource-unbound kernels to execution plugin
            plugin.set_workload(kernels=validated_kernels)

            self._prof.prof('eop, iteration: 1, stage: 1, task: 1, creating', uid=self._uid)

            cus = plugin.create_tasks(record=record, 
                                      pattern_name=self._pattern.name,
                                      iteration=1, 
                                      stage=1)

            self._prof.prof('eop, iteration: 1, stage: 1, task: 1, created', uid=self._uid)

            cus = plugin.execute_tasks(tasks=cus)

            self._prof.prof('eop, iteration: 1, stage: 1, task: 1, submitted', uid=self._uid)



            # Start _execute_thread
            t1 = threading.Thread(  target=self._execute_thread, 
                                    args=[plugin])
            t1.start()


            # Start thread to handle failure
            if self._on_error != 'exit':
                t2 = threading.Thread(target=self._fail_thread, args=[plugin])
                t2.start()

            with self.all_cus_lock:
                if cus!=None:
                    self.all_cus.extend(cus)

            while True: 
                # this loop will break when all tasks are done
                # AM: correct?
                # VB: yes, that's correct.
            
                pending_cus_1 = []
                pending_cus_2 = []
                done_cus = []

                with self.all_cus_lock:

                    '''
                    for unit in self.all_cus:
                        # AM: instead of multiple checks, you can use 'in'
                        #
                        # if (unit.state == rp.DONE)or(unit.state == rp.CANCELED)
                        # or(unit.state==rp.FAILED):
                        if unit.state in [rp.DONE, rp.CANCELED, rp.FAILED]:
                            done_cus.append(unit)

                        else:
                            pending_cus_1.append(unit.uid)
                    '''

                    all_cu_uids = task_manager.list_units()
                    all_cus_2   = task_manager.get_units(all_cu_uids)

                    for unit in all_cus_2:
                        # AM: same here with 'not in'
                        if unit.state not in [rp.DONE, rp.CANCELED, rp.FAILED]:
                            pending_cus_2.append(unit.uid)

                    for unit in done_cus:
                        self.all_cus.remove(unit)

                task_manager.wait_units(pending_cus_2, timeout=60)

                # AM: Uhm, what does that do here?  Are you sure you collected
                #     all units in your callbacks at this point?
                # VB: This was me testing out if there is any difference if I don't use 
                # wait_units() to wait, but just sleep for a while.
                #import time
                #time.sleep(30)

                with self.all_cus_lock:
                    sum1 = sum(plugin.tot_fin_tasks)
                    sum2 = self._pattern.pipeline_size*self._pattern.ensemble_size \
                         + sum(self._pattern._incremented_tasks)

                    if sum1 == sum2:
                        break

            if self._on_error != 'exit':
                self._fail_event.set()                        
                t2.join()

            self._task_event.set()
            t1.join()
            

        except Exception, ex:
            self._logger.exception("EoP Pattern execution failed, error: %s"%ex)
            raise


    # --------------------------------------------------------------------------
    #
    def _execute_thread(self, plugin):

        while not self._task_event.is_set():

            try:

                record = self.get_record()
                unit   = self._task_queue.get(timeout=10)

                cur_stage = int(unit.name.split('-')[1])
                cur_task  = int(unit.name.split('-')[3])

                with self.all_cus_lock:
                    plugin.tot_fin_tasks[cur_stage-1]+=1
                    self._logger.info('Tot_fin_tasks from thread: %s'%plugin.tot_fin_tasks)

                self._logger.info('Stage %s of pipeline %s has finished'%(cur_stage,
                                    cur_task))
                                                
                # Execute branch if it exists
                # AM: *please* define the names - this line is impossible to
                #     parse in under a minute...

                pattern_name    = "pat_%s" % self._pattern.name
                iter_name       = "iter_%s" % self._pattern.cur_iteration[cur_task-1]
                stage_name      = "stage_%s" % cur_stage

                cur_iter = self._pattern.cur_iteration[cur_task-1]

                if (record[pattern_name][iter_name][stage_name]["branch"]):                    

                    self._prof.prof('eop, iteration: %s, stage: %s, task: %s, executing branch' \
                                                                        %(  cur_iter,
                                                                            cur_stage,
                                                                            cur_task), 
                                                                        uid=self._uid)

                    self._logger.info('Executing branch function branch_%s' % cur_stage)
                    branch_function = self._pattern.get_branch(stage=cur_stage)
                    branch_function(instance=cur_task) 

                    self._prof.prof('eop, iteration: %s, stage: %s, task: %s, executed branch' \
                                                                        %(  cur_iter,
                                                                            cur_stage,
                                                                            cur_task), 
                                                                        uid=self._uid)


                self._prof.prof('eop, iteration: %s, stage: %s, task: %s, determining next stage' \
                                                                        %(  cur_iter,
                                                                            cur_stage,
                                                                            cur_task), 
                                                                        uid=self._uid)

                with self.all_cus_lock:


                    if self._pattern.stage_change:

                        if self._pattern.new_stage:

                            # Skip ahead to a new stage
                            if cur_stage < self._pattern.new_stage:                                

                                tasks_skipped = self._pattern.new_stage - cur_stage - 1
                                self._pattern._incremented_tasks[cur_task-1] -= tasks_skipped

                            # Roll back to a new stage
                            elif cur_stage >= self._pattern.new_stage:

                                tasks_cur_to_last_stage = abs(cur_stage - \
                                                                self._pattern.pipeline_size)

                                tasks_beg_to_new_stage = abs(self._pattern.pipeline_size - \
                                                                self._pattern.new_stage) + 1

                                self._pattern._incremented_tasks[cur_task-1] -= \
                                                                tasks_cur_to_last_stage

                                self._pattern._incremented_tasks[cur_task-1] += \
                                                                tasks_beg_to_new_stage
                            
                        else:
                            self._pattern._incremented_tasks[cur_task-1] -= \
                                                abs(cur_stage - self._pattern.pipeline_size)

                        if self._pattern.next_stage[cur_task-1] >= self._pattern.new_stage:
                            self._pattern.cur_iteration[cur_task-1] += 1

                        self._pattern.next_stage[cur_task-1] = self._pattern.new_stage
                    else:
                        self._pattern.next_stage[cur_task-1] +=1


                self._pattern.stage_change = False
                self._pattern.new_stage = None

                # Terminate execution
                if self._pattern.next_stage[cur_task-1] == 0:
                    self._logger.info("Branching function has set termination condition %s"\
                                                                        % cur_task)

                self._prof.prof('eop, iteration: %s, stage: %s, task: %s, determined next stage' \
                                                        %(  cur_iter,
                                                            cur_stage,
                                                            cur_task), 
                                                            uid=self._uid)


                # Check if this is the last task of the stage
                with self.all_cus_lock:
                    if plugin.tot_fin_tasks[cur_stage-1] == self._pattern.ensemble_size:
                        self._logger.info('Stage %s of all pipelines has finished'\
                                            %cur_stage)


                # AM: make conditions parseable!
                cond1 = self._pattern.next_stage[cur_task-1] <= \
                        self._pattern.pipeline_size
                cond2 = self._pattern.next_stage[cur_task-1] != 0

                if cond1 and cond2:

                    self._prof.prof('eop, iteration: %s, stage: %s, task: %s, starting' \
                                                        %(  cur_iter,
                                                            self._pattern.next_stage[cur_task-1],
                                                            cur_task), 
                                                            uid=self._uid)
                


                    next_stage = self._pattern.next_stage[cur_task-1]
                    stage = self._pattern.get_stage(stage=next_stage)
                    stage_kernel = stage(cur_task)
                    
                    validated_kernel = self.validate_kernel(stage_kernel)                                                                       


                    plugin.set_workload(kernels=validated_kernel, cur_task=cur_task)


                    self._prof.prof('eop, iteration: %s, stage: %s, task: %s, creating' \
                                                        %(  cur_iter,
                                                            self._pattern.next_stage[cur_task-1],
                                                            cur_task), 
                                                            uid=self._uid)

                    cud = plugin.create_tasks(record=record, 
                                              pattern_name=self._pattern.name, 
                                              iteration=self._pattern.cur_iteration[cur_task-1], 
                                              stage=self._pattern.next_stage[cur_task-1], 
                                              instance=cur_task)                

                    self._prof.prof('eop, iteration: %s, stage: %s, task: %s, created' \
                                                        %(  cur_iter,
                                                            self._pattern.next_stage[cur_task-1],
                                                            cur_task), 
                                                            uid=self._uid)

                    cu = plugin.execute_tasks(tasks=cud)


                    self._prof.prof('eop, iteration: %s, stage: %s, task: %s, submitted' \
                                                        %(  cur_iter,
                                                            self._pattern.next_stage[cur_task-1],
                                                            cur_task), 
                                                            uid=self._uid)

                    if cu:
                        with self.all_cus_lock:
                            self.all_cus.append(cu)

                with self.all_cus_lock:
                    self._logger.info('All cus from thread: %s' %len(self.all_cus))

                # AM: why is this disabled?
                # VB: Residual of some testing.
                self._task_queue.task_done()


            except Queue.Empty:
                self._logger.debug('Task queue empty -- ending current iteration')

            except Exception, ex:
                self._logger.exception('Failed to run next stage, error: %s'%(ex))
                raise


    # --------------------------------------------------------------------------
    #
    def _fail_thread(self, plugin):

        while not self._fail_event.is_set():

            try:

                record = self.get_record()
                unit   = self._fail_queue.get(timeout=10)

                cur_stage = int(unit.name.split('-')[1])
                cur_task = int(unit.name.split('-')[3])



                if self._on_error == 'resubmit':                                        
                    
                    new_unit = plugin.execute_tasks(unit.description)

                    with self.all_cus_lock:
                        self.all_cus.append(new_unit)
                        self.all_cus.remove(unit)

                elif self._on_error == 'terminate':

                    # AM: line length: 210 chars, *after* unindent from run().
                    #     In my editor, this wraps 3 times, impossible to
                    #     understand...
                    _iter  = self._pattern.cur_iteration[cur_task-1]
                    record = self.add_to_record(record       = record, 
                                                cus          = unit, 
                                                pattern_name = self._pattern.name, 
                                                iteration    = _iter,
                                                stage        = cur_stage,
                                                instance     = cur_task, 
                                                status       = 'Failed')

                    # AM: define names!
                    pattern_name = "pat_%s"%(self._pattern.name)
                    
                    self._pattern.pattern_dict = record[pattern_name]
                    plugin.tot_fin_tasks[cur_stage-1]+=1

                    with self.all_cus_lock:
                        self.all_cus.remove(unit)


                elif self._on_error == 'recreate':

                    stage =  self._pattern.get_stage(stage=self._pattern.next_stage[cur_task-1])
                    stage_kernel = stage(cur_task)

                    validated_kernel = self.validate_kernel(stage_kernel)

                    plugin.set_workload(kernels=validated_kernel, cur_task=cur_task)

                    # AM: too long
                    cud = plugin.create_tasks(  record=record, 
                                                pattern_name=self._pattern.name, 
                                                iteration=self._pattern.cur_iteration[cur_task-1], 
                                                stage=self._pattern.next_stage[cur_task-1], 
                                                instance=cur_task

                                            )                
                    cu = plugin.execute_tasks(tasks=cud)

                    with self.all_cus_lock:

                        if not cu:
                            self.all_cus.append(cu)

                        self.all_cus.remove(unit)


                elif self._on_error == 'continue':
                        
                    # AM: too long
                    record = self.add_to_record(record=record, 
                                                cus=unit, 
                                                pattern_name = self._pattern.name, 
                                                iteration=self._pattern.cur_iteration[cur_task-1], 
                                                stage=cur_stage, 
                                                instance=cur_task, 
                                                status='Failed'
                                            )

                    pattern_name = "pat_%s"%(self._pattern.name)

                    self._pattern.pattern_dict = record[pattern_name]
                    self._task_queue.put(unit)

                    with self.all_cus_lock:
                        self.all_cus.remove(unit)

                else:

                    pass


                # AM: why is this disabled?
                self._fail_queue.task_done()


            except Queue.Empty:
                self._logger.debug('Fail queue empty -- ending current iteration')

            except Exception, ex:
                self._logger.exception('Failed to handle failed task, error: %s'%(ex))
                raise


    # --------------------------------------------------------------------------
    #
    def _unit_state_cb_eop (self, unit, state) :

        record = self.get_record()

        self._logger.debug('Callback initiated for %s, state: %s'%(unit.name, state))

        # Perform these operations only for tasks and not monitors
        if unit.name.startswith('stage'):                            

            cur_stage = int(unit.name.split('-')[1])
            cur_task = int(unit.name.split('-')[3])

            if state == rp.FAILED:

                cur_iter = self._pattern.cur_iteration[cur_task-1]

                self._prof.prof('eop, iteration: %s, stage: %s, task: %s, failed' \
                                                                        %(  cur_iter,
                                                                            cur_stage,
                                                                            cur_task), 
                                                                        uid=self._uid)

                if self._on_error == 'resubmit':

                    # AM: too long
                    self._logger.error("Stage %s of pipeline %s failed: "%(cur_stage, 
                                                                                cur_task) +

                                        "UID: %s, STDERR: %s, STDOUT: %s "%(unit.uid, 
                                                                            unit.stderr, 
                                                                            unit.stdout) +

                                        "LAST LOG: %s"%unit.log[-1])

                    self._logger.info("Resubmitting stage %s of pipeline %s..." \
                                            %(cur_stage, cur_task))

                    self._fail_queue.put(unit)

                elif self._on_error == 'exit':

                    # AM: too long
                    self._logger.error("Stage %s of pipeline %s failed: "%(cur_stage, 
                                                                                cur_task)+

                                        "UID: %s, STDERR: %s, STDOUT: %s "%(unit.uid, 
                                                                            unit.stderr, 
                                                                            unit.stdout)+

                                        "LAST LOG: %s"%unit.log[-1])

                    self._logger.info("Exiting ...")

                    sys.exit(1)

                elif self._on_error == 'terminate':

                    # AM: too long
                    self._logger.error("Stage %s of pipeline %s failed: "%(cur_stage, 
                                                                                cur_task)+

                                        "UID: %s, STDERR: %s, STDOUT: %s "%(unit.uid, 
                                                                            unit.stderr, 
                                                                            unit.stdout)+

                                        "LAST LOG: %s"%unit.log[-1])

                    self._logger.info("Terminating pipeline ...")

                    self._fail_queue.put(unit)

                    return


                elif self._on_error == 'recreate':

                    # AM: too long
                    self._logger.error("Stage %s of pipeline %s failed: "%(cur_stage, 
                                                                                cur_task)+

                                        "UID: %s, STDERR: %s, STDOUT: %s "%(unit.uid, 
                                                                            unit.stderr, 
                                                                            unit.stdout)+

                                        "LAST LOG: %s"%unit.log[-1])


                    self._logger.info("Recreating stage %s of pipeline %s..." \
                                            %(cur_stage, cur_task))

                    self._fail_queue.put(unit)

                    return

                elif self._on_error == 'continue':

                    # AM: too long
                    self._logger.error("Stage %s of pipeline %s failed: "%(cur_stage, 
                                                                                cur_task)+

                                        "UID: %s, STDERR: %s, STDOUT: %s "%(unit.uid, 
                                                                            unit.stderr, 
                                                                            unit.stdout)+

                                        "LAST LOG: %s"%unit.log[-1])

                    self._logger.info("Continuing ahead to stage %s pipeline %s..."\
                                        %(cur_stage, cur_task))

                    # AM: too long
                    record = self.add_to_record(record=record, 
                                                cus=unit, 
                                                pattern_name = self._pattern.name, 
                                                iteration=self._pattern.cur_iteration[cur_task-1], 
                                                stage=cur_stage, 
                                                instance=cur_task, 
                                                status='Failed'
                                            )

                    pattern_name = "pat_%s"%(self._pattern.name)
                    self._pattern.pattern_dict = record[pattern_name]
                    self._task_queue.put(unit)

                    with self.all_cus_lock:
                        if unit in self.all_cus:
                            # AM: I had to add the check above - do you see why
                            #     that would be needed?  Should that be an
                            #     assert?
                            # VB: ack, makes sense.
                            self.all_cus.remove(unit)

                    return

            elif ((state == rp.DONE)or(state==rp.CANCELED)):

                try:
                    cur_stage = int(unit.name.split('-')[1])
                    cur_task = int(unit.name.split('-')[3])       
                    cur_iter = self._pattern.cur_iteration[cur_task-1]                             

                    self._prof.prof('eop, iteration: %s, stage: %s, task: %s, done' \
                                                                        %(  cur_iter,
                                                                            cur_stage,
                                                                            cur_task), 
                                                                        uid=self._uid)

                    with self.all_cus_lock:
                        self.all_cus.remove(unit)

                    self._prof.prof('eop, iteration: %s, stage: %s, task: %s, recording' \
                                                                        %(  cur_iter,
                                                                            cur_stage,
                                                                            cur_task), 
                                                                        uid=self._uid)

                    record = self.get_record()                                    
                    # AM: too long
                    record = self.add_to_record(record=record, 
                                                cus=unit, 
                                                pattern_name = self._pattern.name, 
                                                iteration=self._pattern.cur_iteration[cur_task-1], 
                                                stage=cur_stage, 
                                                instance=cur_task, 
                                                status='Done'
                                            )

                    pattern_name = "pat_%s"%(self._pattern.name)
                    self._pattern.pattern_dict = record[pattern_name] 

                    self._prof.prof('eop, iteration: %s, stage: %s, task: %s, recorded' \
                                                                        %(  cur_iter,
                                                                            cur_stage,
                                                                            cur_task), 
                                                                        uid=self._uid)

                    self._task_queue.put(unit)
                    
                except Exception, ex:
                    self._logger.exception('Failed to push to task queue, error: %s'%(ex))
                    raise


    # --------------------------------------------------------------------------
    #
    def run(self, resource, task_manager, rp_session):

        try:
            # Create dictionary for logging
            # AM: what does that comment mean??
            record = self.get_record()

            # For data transfer, inform pattern of the resource
            self._pattern.session_id = rp_session

            self.sanity_pattern_check()

            if isinstance(self._pattern, PoE):
                self.exec_poe(resource, task_manager, rp_session)

            elif isinstance(self._pattern, EoP):
                self.exec_eop(resource, task_manager, rp_session)
   

        except Exception, ex:
            self._logger.exception("App manager failed at workload execution, error: %s"\
                                                %(ex))
            raise


    # ------------------------------------------------------------------------------
