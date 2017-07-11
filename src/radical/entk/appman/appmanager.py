__copyright__   = "Copyright 2017-2018, http://radical.rutgers.edu"
__author__      = "Vivek Balasubramanian <vivek.balasubramaniana@rutgers.edu>"
__license__     = "MIT"

import radical.utils as ru
from radical.entk.exceptions import *
from radical.entk.pipeline.pipeline import Pipeline
from radical.entk.task.task import Task
from radical.entk.execman.resource_manager import ResourceManager
from radical.entk.execman.task_manager import TaskManager
from wfprocessor import WFprocessor
import sys, time, os
import Queue
import pika
import json
from threading import Thread, Event
import traceback
from radical.entk import states

slow_run = os.environ.get('RADICAL_ENTK_SLOW',False)


class AppManager(object):

    """

    An application manager takes the responsibility of dispatching tasks from the various pipelines
    according to their relative order to an underlying runtime system for execution.

    :hostname: host rabbitmq server is running
    :push_threads: number of threads to push tasks on the pending_qs
    :pull_threads: number of threads to pull tasks from the completed_qs
    :sync_threads: number of threads to pull task from the synchronizer_q
    :pending_qs: number of queues to hold pending tasks to be pulled by the helper/execution manager
    :completed_qs: number of queues to hold completed tasks pushed by the helper/execution manager
    """


    def __init__(self, hostname = 'localhost', push_threads=1, pull_threads=1, 
                sync_threads=1, pending_qs=1, completed_qs=1):

        self._uid       = ru.generate_id('radical.entk.appmanager')
        self._logger = ru.get_logger('radical.entk.appmanager')
        self._prof = ru.Profiler(name = self._uid)

        self._prof.prof('create amgr obj', uid=self._uid)

        self._name      = str()

        self._workflow  = None
        self._resubmit_failed = False

        # RabbitMQ Queues
        self._num_pending_qs = pending_qs
        self._num_completed_qs = completed_qs
        self._pending_queue = list()
        self._completed_queue = list()
        # RabbitMQ inits
        self._mq_connection = None
        self._mq_channel = None
        self._mq_hostname = hostname


        # Threads and procs counts
        self._num_push_threads = push_threads
        self._num_pull_threads = pull_threads
        self._num_sync_threads = sync_threads
        self._end_sync = Event()

        # None objects for error handling
        self._wfp = None
        self._task_manager = None
        self._sync_thread = None

        # Resource Manager object
        self._resource_manager = None
        
        # Logger        
        self._logger.info('Application Manager initialized')

        self._prof.prof('amgr obj created', uid=self._uid)

    # -----------------------------------------------
    # Getter functions
    # -----------------------------------------------

    @property
    def name(self):

        """
        Name for the application manager 

        :getter: Returns the name of the application manager
        :setter: Assigns the name of the application manager
        :type: String
        """

        return self._name

    @property
    def resubmit_failed(self):

        """
        Enable resubmission of failed tasks

        :getter: Returns the value of the resubmission flag
        :setter: Assigns a boolean value for the resubmission flag
        """
        return self._resubmit_failed
    

    @property
    def resource_manager(self):

        """
        :getter: Returns the resource manager object being used
        :setter: Assigns a resource manager
        """

        return self._resource_manager
    # -----------------------------------------------
    # Setter functions
    # -----------------------------------------------

    @name.setter
    def name(self, value):
        self._name = value

    @resubmit_failed.setter
    def resubmit_failed(self, value):
        self._resubmit_failed = value

    @resource_manager.setter
    def resource_manager(self, value):
        self._resource_manager = value

    # Function to add workflow to the application manager
    # ------------------------------------------------------

    def assign_workflow(self, workflow):

        """
        Assign workflow to the application manager to be executed

        :arguments: set of Pipelines
        """


        try:
            
            self._prof.prof('assigning workflow', uid=self._uid)
            self._workflow = self._validate_workflow(workflow)
            self._logger.info('Workflow assigned to Application Manager')

        except KeyboardInterrupt:

            self._logger.error('Execution interrupted by user ' + 
                        '(you probably hit Ctrl+C), tring to exit gracefully...')
            
            raise KeyboardInterrupt

        except Exception, ex:

            self._logger.error('Fatal error while adding workflow to appmanager')
            raise Error(text=ex)

        

    def _validate_workflow(self, workflow):

        """
        Validate whether the workflow consists of a set of pipelines
        """

        try:

            self._prof.prof('validating workflow', uid=self._uid)

            if not isinstance(workflow, set):

                if not isinstance(workflow, list):
                    workflow = set([workflow])
                else:
                    workflow = set(workflow)

            for item in workflow:
                if not isinstance(item, Pipeline):
                    self._logger.info('workflow type incorrect')
                    raise TypeError(expected_type=['Pipeline', 'set of Pipeline'], 
                                    actual_type=type(item))

            self._prof.prof('workflow validated', uid=self._uid)

            return workflow

        except KeyboardInterrupt:

            self._logger.error('Execution interrupted by user ' + 
                        '(you probably hit Ctrl+C), tring to exit gracefully...')
            
            raise KeyboardInterrupt

        except Exception, ex:

            self._logger.error('Fatal error while adding workflow to appmanager')
            raise Error(text=ex)


    def _setup_mqs(self):

        """
        Setup RabbitMQ system
        """

        try:

            self._prof.prof('init mqs setup', uid=self._uid)

            self._logger.debug('Setting up mq connection and channel')

            self._mq_connection = pika.BlockingConnection(
                                    pika.ConnectionParameters(host=self._mq_hostname)
                                    )
            self._mq_channel = self._mq_connection.channel()

            self._logger.debug('Connection and channel setup successful')

            self._logger.debug('Setting up all exchanges and queues')

            #self._mq_channel.exchange_delete(exchange='fork')
            #self._mq_channel.exchange_declare(exchange='fork')

            for i in range(1,self._num_pending_qs+1):
                queue_name = 'pendingq-%s'%i
                self._pending_queue.append(queue_name)
                self._mq_channel.queue_delete(queue=queue_name)
                self._mq_channel.queue_declare(queue=queue_name) 
                                                # Durable Qs will not be lost if rabbitmq server crashes

            for i in range(1,self._num_completed_qs+1):
                queue_name = 'completedq-%s'%i
                self._completed_queue.append(queue_name)
                self._mq_channel.queue_delete(queue=queue_name)
                self._mq_channel.queue_declare(queue=queue_name)
                #self._mq_channel.queue_bind(exchange='fork', queue=queue_name)
                                                # Durable Qs will not be lost if rabbitmq server crashes

            self._mq_channel.queue_delete(queue='synchronizerq')
            self._mq_channel.queue_declare(queue='synchronizerq')
                                                    # Durable Qs will not be lost if rabbitmq server crashes


            self._logger.debug('All exchanges and queues are setup')

            self._prof.prof('mqs setup done', uid=self._uid)

            return True


        except KeyboardInterrupt:

            self._logger.error('Execution interrupted by user (you probably hit Ctrl+C), '+
                                'trying to exit mqs setup gracefully...')

            raise KeyboardInterrupt

        except Exception, ex:

            self._logger.error('Error setting RabbitMQ system')
            raise Error(text=ex)


    def _synchronizer(self):

        """
        Thread to keep the workflow data structure in appmanager up to date
        """

        try:

            self._prof.prof('synchronizer started', uid=self._uid)

            self._logger.info('synchronizer thread started')


            mq_connection = pika.BlockingConnection(
                                    pika.ConnectionParameters(host=self._mq_hostname)
                                    )
            mq_channel = mq_connection.channel()

            while not self._end_sync.is_set():

                method_frame, header_frame, body = mq_channel.basic_get(
                                                            queue='synchronizerq'
                                                            )                

                if body:

                    completed_task = Task()
                    completed_task.from_dict(json.loads(body))
                    self._logger.info('Got finished task %s from synchronizer queue'%(completed_task.uid))

                    completed_task.state = states.SYNCHRONIZING

                    self._prof.prof('transition', 
                                    uid=completed_task.uid, 
                                    state=completed_task.state)
                    
                    self._logger.info('Task: %s, State: %s'%(  completed_task.uid, 
                                                                    completed_task.state)
                                                                )

                    for pipe in self._workflow:

                        if not pipe._completed:

                            if completed_task._parent_pipeline == pipe.uid:

                                self._logger.debug('Found parent pipeline: %s'%pipe.uid)
                            
                                for stage in pipe.stages:

                                    if completed_task._parent_stage == stage.uid:
                                        self._logger.debug('Found parent stage: %s'%(stage.uid))

                                        completed_task.state = states.SYNCHRONIZED

                                        self._prof.prof('transition', 
                                                        uid=completed_task.uid, 
                                                        state=completed_task.state)
                                        
                                        self._logger.info('Task: %s, State: %s'%(  completed_task.uid, 
                                                                                    completed_task.state)
                                                                                )
                                        if completed_task.exit_code:
                                            completed_task.state = states.FAILED
                                        else:
                                            completed_task.state = states.DONE

                                        self._prof.prof('transition', 
                                                        uid=completed_task.uid, 
                                                        state=completed_task.state)

                                        self._logger.info('Task: %s, State: %s'%(  completed_task.uid, 
                                                            completed_task.state)
                                                        )

                                        for task in stage.tasks:

                                            if task.uid == completed_task.uid:

                                                task.state = str(completed_task.state)

                                                if task.state == states.DONE:

                                                    if stage._check_stage_complete():

                                                        try:

                                                            stage.state = states.DONE
                                                            
                                                            self._prof.prof('transition', 
                                                                            uid=stage.uid, 
                                                                            state=stage.state)
                                                            
                                                            self._logger.info('Stage: %s, State: %s'%
                                                                                            (stage.uid, 
                                                                                            stage.state))

                                                        except Exception, ex:
                                                            # Rolling back stage status
                                                            self._logger.error('Error while updating stage '+
                                                                            'state, rolling back. Error: %s'%ex)

                                                            stage.state = states.SCHEDULED
                                                            
                                                            self._prof.prof('transition', 
                                                                            uid=stage.uid, 
                                                                            state=stage.state)
                                                            
                                                            self._logger.info('Stage: %s, State: %s'%
                                                                                            (stage.uid, 
                                                                                            stage.state))

                                                        try:
                                                            pipe._increment_stage()
                                                        except:
                                                            pass

                                                        if pipe._completed:
                                                            #self._workload.remove(pipe)                                                                    
                                                            
                                                            pipe.state = states.SCHEDULED
                                                            
                                                            self._prof.prof('transition', 
                                                                            uid=pipe.uid, 
                                                                            state=pipe.state)
                                                            
                                                            self._logger.info('Pipe: %s, State: %s'%
                                                                                            (pipe.uid, 
                                                                                            pipe.state))

                                                            pipe.state = states.DONE
                                                            
                                                            self._prof.prof('transition', 
                                                                            uid=pipe.uid, 
                                                                            state=pipe.state)
                                                            
                                                            self._logger.info('Pipe: %s, State: %s'%
                                                                                            (pipe.uid, 
                                                                                            pipe.state))
                                                                    
                                                elif task.state == states.FAILED:

                                                    if self._resubmit_failed:

                                                        try:

                                                            new_task = Task()
                                                            new_task._replicate(completed_task)

                                                            pipe.stages[pipe._current_stage-1].add_tasks(new_task)

                                                        except Exception, ex:
                                                            self._logger.error("Resubmission of task %s failed, error: %s"%
                                                                                                (completed_task.uid,ex))
                                                            raise

                                                    else:

                                                        if stage._check_stage_complete():

                                                            try:
                                                                    
                                                                stage.state = states.DONE
                                                                
                                                                self._prof.prof('transition', 
                                                                            uid=stage.uid, 
                                                                            state=stage.state)
                                                                
                                                                self._logger.info('Stage: %s, State: %s'%
                                                                                                (stage.uid, 
                                                                                                stage.state))

                                                                pipe._increment_stage()

                                                            except Exception, ex:
                                                    
                                                                # Rolling back stage status
                                                                self._logger.error('Error while updating stage '+
                                                                                    'state, rolling back. Error: %s'%ex)

                                                                stage.state = states.SCHEDULED
                                                                
                                                                self._prof.prof('transition', 
                                                                            uid=stage.uid, 
                                                                            state=stage.state)
                                                                
                                                                self._logger.info('Stage: %s, State: %s'%
                                                                                                    (stage.uid, 
                                                                                                    stage.state))
                                                                pipe._decrement_stage()                                    

                                                            if pipe.completed:

                                                                #self._workload.remove(pipe)
                                                                pipe.state = states.SCHEDULED
                                                                
                                                                self._prof.prof('transition', 
                                                                            uid=pipe.uid, 
                                                                            state=pipe.state)
                                                                
                                                                self._logger.info('Pipe: %s, State: %s'%
                                                                                                    (pipe.uid, 
                                                                                                    pipe.state))
                                                                pipe.state = states.DONE
                                                                
                                                                self._prof.prof('transition', 
                                                                            uid=pipe.uid, 
                                                                            state=pipe.state)
                                                                
                                                                self._logger.info('Pipe: %s, State: %s'%
                                                                                                    (pipe.uid, 
                                                                                                    pipe.state))

                                                else:

                                                    # Task is canceled
                                                    pass

                                                # Found the task and processed it -- no more iterations needed
                                                break

                                        # Found the stage and processed it -- no more iterations neeeded
                                        break

                                # Found the pipeline and processed it -- no more iterations neeeded
                                break

                    mq_channel.basic_ack(delivery_tag=method_frame.delivery_tag)

            self._prof.prof('terminating synchronizer', uid=self._uid)

        except KeyboardInterrupt:

            self._logger.error('Execution interrupted by user (you probably hit Ctrl+C), '+
                                'trying to terminate synchronizer thread gracefully...')

            raise KeyboardInterrupt



        except Exception, ex:

            self._logger.error('Unknown error in synchronizer: %s. \n Terminating thread'%ex)
            print traceback.format_exc()
            raise Error(text=ex)


    def run(self):

        """
        Run the application manager
        """

        try:

            if not self._workflow:
                print 'Assign workflow before invoking run method - cannot proceed'
                self._logger.error('No workflow assigned currently, please check your script')
                raise ValueError(expected_value='set of pipelines', actual_value=None)


            if not self._resource_manager:
                self._logger.error('No resource manager assigned currently, please create and add a valid resource manager')
                raise ValueError(expected_value=ResourceManager, actual_value=None)

            else:

                self._prof.prof('amgr run started', uid=self._uid)

                # Setup rabbitmq stuff
                self._logger.info('Setting up RabbitMQ system')
                setup = self._setup_mqs()

                if not setup:
                    raise


                # Submit resource request
                self._logger.info('Starting resource request submission')
                self._prof.prof('init rreq submission', uid=self._uid)
                self._resource_manager._submit_resource_request()


                # Start synchronizer thread
                self._logger.info('Starting synchronizer thread')
                self._sync_thread = Thread(target=self._synchronizer, name='synchronizer-thread')
                self._prof.prof('starting synchronizer thread', uid=self._uid)
                self._sync_thread.start()

                # Create WFProcessor object
                self._prof.prof('creating wfp obj', uid=self._uid)
                self._wfp = WFprocessor(  workflow = self._workflow, 
                                    pending_queue = self._pending_queue, 
                                    completed_queue=self._completed_queue,
                                    mq_hostname=self._mq_hostname)

                self._logger.info('Starting WFProcessor process from AppManager')                
                self._wfp.start_processor()                

                # Create tmgr object
                self._prof.prof('creating tmgr obj', uid=self._uid)
                self._task_manager = TaskManager(   pending_queue = self._pending_queue,
                                                    completed_queue = self._completed_queue,
                                                    mq_hostname = self._mq_hostname,
                                                    rmgr = self._resource_manager
                                                )
                self._logger.info('Starting task manager process from AppManager')
                self._task_manager.start_manager()
                self._task_manager.start_monitor()

                
                active_pipe_count = len(self._workflow)   
                finished_pipe_uids = []             

                while (active_pipe_count > 0)or(self._wfp.workflow_incomplete()):

                    if slow_run:
                        time.sleep(1)

                    if active_pipe_count > 0:

                        for pipe in self._workflow:

                            with pipe._stage_lock:

                                if (pipe._completed) and (pipe.uid not in finished_pipe_uids) :
                                    self._logger.info('Pipe %s completed'%pipe.uid)
                                    finished_pipe_uids.append(pipe.uid)
                                    active_pipe_count -= 1
                                    self._logger.info('Active pipes: %s'%active_pipe_count)


                    if not self._sync_thread.is_alive():
                        self._sync_thread = Thread(   target=self._synchronizer, 
                                                name='synchronizer-thread')
                        self._logger.info('Restarting synchronizer thread')
                        self._prof.prof('restarting synchronizer', uid=self._uid)
                        self._sync_thread.start()

                    
                    if not self._wfp.check_alive():

                        self._prof.prof('recreating wfp obj', uid=self._uid)
                        self._wfp = WFProcessor(  workflow = self._workflow, 
                                            pending_queue = self._pending_queue, 
                                            completed_queue=self._completed_queue,
                                            mq_hostname=self._mq_hostname)

                        self._logger.info('Restarting WFProcessor process from AppManager')                        
                        self._wfp.start_processor()

                    if not self._task_manager.check_alive():

                        self._prof.prof('recreating tmgr obj', uid=self._uid)
                        self._task_manager = TaskManager(   pending_queue = self._pending_queue,
                                                            completed_queue = self._completed_queue,
                                                            mq_hostname = self._mq_hostname,
                                                            rmgr = self._resource_manager
                                                        )
                        self._logger.info('Restarting task manager process from AppManager')
                        self._task_manager.start_manager()

                    
                self._prof.prof('start termination', uid=self._uid)
                    
                # Terminate threads in following order: wfp, helper, synchronizer
                self._logger.info('Terminating WFprocessor')
                self._wfp.end_processor()       
                
                self._logger.info('Terminating task manager process')
                self._task_manager.end_manager()
                self._task_manager.end_monitor()
                

                self._logger.info('Terminating synchronizer thread')
                self._end_sync.set()
                self._sync_thread.join()
                self._logger.info('Synchronizer thread terminated')

                self._resource_manager._cancel_resource_request()

                self._prof.prof('termination done', uid=self._uid)


        except KeyboardInterrupt:

            self._prof.prof('start termination', uid=self._uid)

            self._logger.error('Execution interrupted by user (you probably hit Ctrl+C), '+
                                'trying to cancel enqueuer thread gracefully...')

            # Terminate threads in following order: wfp, helper, synchronizer
            if self._wfp:
                self._logger.info('Terminating WFprocessor')
                self._wfp.end_processor()

            if self._task_manager:
                self._logger.info('Terminating task manager process')
                self._task_manager.end_manager()
                
            if self._sync_thread:
                self._logger.info('Terminating synchronizer thread')
                self._end_sync.set()
                self._sync_thread.join()
                self._logger.info('Synchronizer thread terminated')

            self._resource_manager._cancel_resource_request()

            self._prof.prof('termination done', uid=self._uid)

            raise KeyboardInterrupt

        except Exception, ex:

            self._prof.prof('start termination', uid=self._uid)

            self._logger.error('Unknown error in AppManager: %s'%ex)
            print traceback.format_exc()

            ## Terminate threads in following order: wfp, helper, synchronizer
            if self._wfp:
                self._logger.info('Terminating WFprocessor')
                self._wfp.end_processor()

            if self._task_manager:
                self._logger.info('Terminating task manager process')
                self._task_manager.end_manager()

            if self._sync_thread:
                self._logger.info('Terminating synchronizer thread')
                self._end_sync.set()
                self._sync_thread.join()
                self._logger.info('Synchronizer thread terminated')

            self._resource_manager._cancel_resource_request()

            self._prof.prof('termination done', uid=self._uid)
            
            sys.exit(1)
