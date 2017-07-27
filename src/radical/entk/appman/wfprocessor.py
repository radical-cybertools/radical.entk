__copyright__   = "Copyright 2017-2018, http://radical.rutgers.edu"
__author__      = "Vivek Balasubramanian <vivek.balasubramaniana@rutgers.edu>"
__license__     = "MIT"

import radical.utils as ru
from radical.entk.exceptions import *
from multiprocessing import Process, Event
from radical.entk import states, Pipeline, Task
from radical.entk.utils.sync_initiator import sync_with_master
import time
from time import sleep
import json
import threading
import pika
import traceback
import os
import uuid

class WFprocessor(object):

    """
    An WFProcessor (workflow processor) takes the responsibility of dispatching tasks from the various pipelines of the
    workflow according to their relative order to the TaskManager. All state updates are communicated to the AppManager.

    :Arguments:
        :workflow: COPY of the entire workflow existing in the AppManager 
        :pending_qs: number of queues to hold pending tasks
        :completed_qs: number of queues to hold completed tasks
        :mq_hostname: hostname where the RabbitMQ is live
    """


    def __init__(self, workflow, pending_queue, completed_queue, mq_hostname):

        self._uid           = ru.generate_id('radical.entk.wfprocessor')        
        self._logger        = ru.get_logger('radical.entk.wfprocessor')
        self._prof = ru.Profiler(name = self._uid + '-obj')

        self._prof.prof('create wfp obj', uid=self._uid)

        self._workflow      = workflow

        if not isinstance(pending_queue,list):
            raise TypeError(expected_type=list, actual_type=type(pending_queue))

        if not isinstance(completed_queue,list):
            raise TypeError(expected_type=list, actual_type=type(completed_queue))

        if not isinstance(mq_hostname,str):
            raise TypeError(expected_type=str, actual_type=type(mq_hostname))

        # Mqs queue names and channel
        self._pending_queue = pending_queue
        self._completed_queue = completed_queue
        self._mq_hostname = mq_hostname

        self._wfp_process = None       
        self._resubmit_failed = False       

        self._logger.info('Created WFProcessor object: %s'%self._uid)

        self._prof.prof('wfp obj created', uid=self._uid)
    

    # ------------------------------------------------------------------------------------------------------------------
    # Private Methods
    # ------------------------------------------------------------------------------------------------------------------

    def _wfp(self):

        """
        **Purpose**: This is the function executed in the wfp process. The function is used to simply create
        and spawn two threads: enqueue, dequeue. The enqueue thread pushes ready tasks to the queues in the pending_q 
        list whereas the dequeue thread pulls completed tasks from the queues in the completed_q. This function is also
        responsible for the termination of these threads and hence blocking.
        """

        try:

            local_prof = ru.Profiler(name = self._uid + '-proc')

            local_prof.prof('wfp process started', uid=self._uid)

            self._logger.info('WFprocessor started')

            # Process should run till terminate condtion is encountered
            while (not self._wfp_terminate.is_set()):

                try:

                    # Start dequeue thread
                    if (not self._dequeue_thread) or (not self._dequeue_thread.is_alive()):

                        local_prof.prof('creating dequeue-thread', uid=self._uid)
                        self._dequeue_thread = threading.Thread(target=self._dequeue, args=(local_prof,), name='dequeue-thread')

                        self._logger.info('Starting dequeue-thread')
                        local_prof.prof('starting dequeue-thread', uid=self._uid)
                        self._dequeue_thread.start()

                    # Start enqueue thread
                    if (not self._enqueue_thread) or (not self._enqueue_thread.is_alive()):

                        local_prof.prof('creating enqueue-thread', uid=self._uid)
                        self._enqueue_thread = threading.Thread(target=self._enqueue, args=(local_prof,), name='enqueue-thread')

                        self._logger.info('Starting enqueue-thread')
                        local_prof.prof('starting enqueue-thread', uid=self._uid)
                        self._enqueue_thread.start()

                except KeyboardInterrupt:
                    raise KeyboardInterrupt

                except Exception, ex:
                    self._logger.error('WFProcessor interrupted')
                    raise

            local_prof.prof('start termination', uid=self._uid)

            self._logger.info('Terminating enqueue-thread')
            self._enqueue_thread_terminate.set()
            self._enqueue_thread.join()
            self._logger.info('Terminating dequeue-thread')
            self._dequeue_thread_terminate.set()
            self._dequeue_thread.join()

            local_prof.prof('termination done', uid=self._uid)

            local_prof.prof('terminating wfp process', uid=self._uid)

            local_prof.close()

        except KeyboardInterrupt:

            self._logger.error('Execution interrupted by user (you probably hit Ctrl+C), '+
                                'trying to cancel wfprocessor process gracefully...')

            if not self._enqueue_thread_terminate.is_set():
                self._logger.info('Terminating enqueue-thread')
                self._enqueue_thread_terminate.set()
                self._enqueue_thread.join()

            if not self._dequeue_thread_terminate.is_set():
                self._logger.info('Terminating dequeue-thread')
                self._dequeue_thread_terminate.set()
                self._dequeue_thread.join()

            self._logger.info('WFprocessor process terminated')

            raise KeyboardInterrupt


        except Exception, ex:
            self._logger.error('Unknown error in wfp process: %s. \n Closing all threads'%ex)
            print traceback.format_exc()

            if not self._enqueue_thread_terminate.is_set():
                self._logger.info('Terminating enqueue-thread')
                self._enqueue_thread_terminate.set()
                self._enqueue_thread.join()

            if not self._dequeue_thread_terminate.is_set():
                self._logger.info('Terminating dequeue-thread')
                self._dequeue_thread_terminate.set()
                self._dequeue_thread.join()

            self._logger.info('WFprocessor process terminated')

            raise Error(text=ex)           


    def _enqueue(self, local_prof):

        """
        **Purpose**: This is the function that is run in the enqueue thread. This function extracts Tasks from the 
        copy of workflow that exists in the WFprocessor object and pushes them to the queues in the pending_q list.
        Since this thread works on the copy of the workflow, every state update to the Task, Stage and Pipeline is
        communicated back to the AppManager (master process) via the 'sync_with_master' function that has dedicated
        queues to communicate with the master.

        Details: Termination condition of this thread is set by the wfp process.
        """

        try:


            local_prof.prof('enqueue-thread started', uid=self._uid)
            self._logger.info('enqueue-thread started')

            mq_connection = pika.BlockingConnection(pika.ConnectionParameters(host=self._mq_hostname))
            mq_channel = mq_connection.channel()

            while not self._enqueue_thread_terminate.is_set():

                for pipe in self._workflow:

                    with pipe._stage_lock:

                        if not pipe.completed:
      
                            # Test if the pipeline is already in the final state
                            if pipe.state in states.FINAL:
                                continue

                            elif pipe.state == states.INITIAL:
                                
                                pipe.state = states.SCHEDULING
                                
                                local_prof.prof('transition', 
                                                uid=pipe.uid, 
                                                state=pipe.state)

                                sync_with_master(   obj=pipe, 
                                                    obj_type='Pipeline', 
                                                    channel = mq_channel)

                                self._logger.info('Pipe: %s, State: %s'%(pipe.uid, pipe.state))
    
                            if pipe.stages[pipe.current_stage-1].state in [states.INITIAL]:
    
                                try:

                                    # Starting scheduling of tasks of current stage
                                    pipe.stages[pipe.current_stage-1].state = states.SCHEDULING
                                    
                                    local_prof.prof('transition', 
                                                    uid=pipe.stages[pipe._current_stage-1].uid, 
                                                    state=pipe.stages[pipe._current_stage-1].state,
                                                    msg = pipe.uid)

                                    sync_with_master(   obj=pipe.stages[pipe.current_stage-1], 
                                                        obj_type='Stage', 
                                                        channel = mq_channel)

                                    self._logger.info('Stage: %s, State: %s'%(pipe.stages[pipe.current_stage-1].uid, 
                                        pipe.stages[pipe.current_stage-1].state))

                                    executable_stage = pipe.stages[pipe.current_stage-1]
                                    executable_tasks = executable_stage.tasks
    
                                    tasks_submitted=False
    
                                    for executable_task in executable_tasks:
    
                                        if executable_task.state == states.INITIAL:
                                            
                                            self._logger.info('Task: %s, State: %s'%(  executable_task.uid, 
                                                                                        executable_task.state)
                                                                )
    
                                            # Try-exception block for tasks
                                            try:
    
                                                executable_task.state = states.SCHEDULING

                                                local_prof.prof('transition', 
                                                                uid=executable_task.uid, 
                                                                state=executable_task.state,
                                                                msg = executable_stage.uid)

                                                sync_with_master(   obj=executable_task, 
                                                                    obj_type='Task', 
                                                                    channel = mq_channel)
                                                
                                                self._logger.info('Task: %s, State: %s'%(  executable_task.uid, 
                                                                                        executable_task.state))
    
                                                task_as_dict = json.dumps(executable_task.to_dict())
    
                                                self._logger.debug('Publishing task %s to %s'
                                                                            %(executable_task.uid,
                                                                                self._pending_queue[0])
                                                                        )


                                                # Put the task on one of the pending_queues

                                                mq_channel.basic_publish(   exchange='',
                                                                            routing_key=self._pending_queue[0],
                                                                            body=task_as_dict
                                                                            #properties=pika.BasicProperties(
                                                                                # make message persistent
                                                                                #delivery_mode = 2, 
                                                                                #)
                                                                        )

                                                executable_task.state = states.SCHEDULED

                                                local_prof.prof('transition', 
                                                                uid=executable_task.uid, 
                                                                state=executable_task.state,
                                                                msg = executable_stage.uid)

                                                sync_with_master(   obj=executable_task, 
                                                                    obj_type='Task', 
                                                                    channel = mq_channel)

                                                self._logger.info('Task: %s, State: %s'%(  executable_task.uid, 
                                                                                        executable_task.state))

                                                tasks_submitted = True
                                                self._logger.debug('Task %s published to queue'% executable_task.uid)                                                                                                
    
                                            except KeyboardInterrupt:
                                                raise KeyboardInterrupt

                                            except Exception, ex:
    
                                                # Rolling back queue status
                                                self._logger.error('Error while updating task '+
                                                                    'state, rolling back. Error: %s'%ex)
                                               
                                                # Revert task status
                                                executable_task.state = states.INITIAL

                                                local_prof.prof('transition', 
                                                                uid=executable_task.uid, 
                                                                state=executable_task.state,
                                                                msg = executable_stage.uid)

                                                sync_with_master(   obj=executable_task, 
                                                                    obj_type='Task', 
                                                                    channel = mq_channel)

                                                self._logger.info('Task: %s, State: %s'%(  executable_task.uid, 
                                                                                        executable_task.state))

                                                raise
                                    
                                    # All tasks of current stage scheduled
                                    pipe.stages[pipe.current_stage-1].state = states.SCHEDULED                                    

                                    local_prof.prof('transition', 
                                                    uid=pipe.stages[pipe._current_stage-1].uid, 
                                                    state=pipe.stages[pipe._current_stage-1].state,
                                                    msg=pipe.uid)


                                    sync_with_master(   obj=pipe.stages[pipe.current_stage-1], 
                                                        obj_type='Stage', 
                                                        channel = mq_channel)

                                    self._logger.info('Stage: %s, State: %s'%(  pipe.stages[pipe.current_stage-1].uid, 
                                                                                pipe.stages[pipe.current_stage-1].state))

                                    if tasks_submitted:
                                        tasks_submitted = False
                                
                                except KeyboardInterrupt:
                                    raise KeyboardInterrupt

                                                                                
                                except Exception, ex:
    
                                    # Rolling back queue status
                                    self._logger.error('Error while updating stage '+
                                                        'state, rolling back. Error: %s'%ex)
    
                                    # Revert stage state
                                    pipe.stages[pipe.current_stage-1].state = states.INITIAL

                                    local_prof.prof('transition', 
                                                    uid=pipe.stages[pipe._current_stage-1].uid, 
                                                    state=pipe.stages[pipe._current_stage-1].state,
                                                    msg=pipe.uid)

                                    sync_with_master(   obj=pipe.stages[pipe.current_stage-1], 
                                                        obj_type='Stage', 
                                                        channel = mq_channel)

                                    self._logger.info('Stage: %s, State: %s'%(  pipe.stages[pipe.current_stage-1].uid, 
                                                                                pipe.stages[pipe.current_stage-1].state))

                                    raise

            self._logger.info('Enqueue thread terminated')                                  
            mq_connection.close()

            local_prof.prof('terminating enqueue-thread', uid=self._uid)
                                    
        except KeyboardInterrupt:

            self._logger.error('Execution interrupted by user (you probably hit Ctrl+C), '+
                                'trying to cancel enqueuer thread gracefully...')

            mq_connection.close()

            raise KeyboardInterrupt

        except Exception, ex:

            self._logger.error('Unknown error in wfp process: %s. \n Terminating all threads'%ex)
            print traceback.format_exc()
            mq_connection.close()

            raise Error(text=ex) 



    def _dequeue(self, local_prof):

        """
        **Purpose**: This is the function that is run in the dequeue thread. This function extracts Tasks from the 
        completed queus and updates the copy of workflow that exists in the WFprocessor object.
        Since this thread works on the copy of the workflow, every state update to the Task, Stage and Pipeline is
        communicated back to the AppManager (master process) via the 'sync_with_master' function that has dedicated
        queues to communicate with the master.

        Details: Termination condition of this thread is set by the wfp process.
        """

        try:

            local_prof.prof('dequeue-thread started', uid=self._uid)
            self._logger.info('Dequeue thread started')

            mq_connection = pika.BlockingConnection(pika.ConnectionParameters(host=self._mq_hostname))
            mq_channel = mq_connection.channel()

            while not self._dequeue_thread_terminate.is_set():

                try:

                    method_frame, header_frame, body = mq_channel.basic_get(queue=self._completed_queue[0])

                    if body:

                        # Get task from the message
                        completed_task = Task()
                        completed_task.from_dict(json.loads(body))
                        self._logger.info('Got finished task %s from queue'%(completed_task.uid))

                        completed_task.state = states.DEQUEUEING                        

                        local_prof.prof('transition', 
                                        uid=completed_task.uid, 
                                        state=completed_task.state,
                                        msg=completed_task._parent_stage)

                        sync_with_master(   obj=completed_task, 
                                            obj_type='Task', 
                                            channel = mq_channel)

                        self._logger.info('Task: %s, State: %s'%(  completed_task.uid, 
                                                                    completed_task.state)
                                                                )


                        # Traverse the entire workflow to find out the correct Task
                        for pipe in self._workflow:

                            with pipe._stage_lock:

                                if not pipe.completed:

                                    if completed_task._parent_pipeline == pipe.uid:

                                        self._logger.debug('Found parent pipeline: %s'%pipe.uid)
                                
                                        for stage in pipe.stages:

                                            if completed_task._parent_stage == stage.uid:
                                                self._logger.debug('Found parent stage: %s'%(stage.uid))

                                                completed_task.state = states.DEQUEUED

                                                local_prof.prof('transition', 
                                                                uid=completed_task.uid, 
                                                                state=completed_task.state,
                                                                msg=completed_task._parent_stage)

                                                sync_with_master(   obj=completed_task, 
                                                                    obj_type='Task', 
                                                                    channel = mq_channel)

                                                self._logger.info('Task: %s, State: %s'%(  completed_task.uid, 
                                                                                            completed_task.state))

                                                if completed_task.exit_code:
                                                    completed_task.state = states.FAILED
                                                else:
                                                    completed_task.state = states.DONE

                                                local_prof.prof('transition', 
                                                                uid=completed_task.uid, 
                                                                state=completed_task.state,
                                                                msg=completed_task._parent_stage)

                                                sync_with_master(   obj=completed_task, 
                                                                    obj_type='Task', 
                                                                    channel = mq_channel)

                                                self._logger.info('Task: %s, State: %s'%(  completed_task.uid, 
                                                                                            completed_task.state))

                                                for task in stage.tasks:

                                                    if task.uid == completed_task.uid:
                                                        task.state = str(completed_task.state)

                                                        if task.state == states.DONE:

                                                            if stage._check_stage_complete():

                                                                try:

                                                                    stage.state = states.DONE

                                                                    local_prof.prof('transition', 
                                                                                    uid=stage.uid, 
                                                                                    state=stage.state,
                                                                                    msg=stage._parent_pipeline)

                                                                    sync_with_master(   obj=stage, 
                                                                                        obj_type='Stage', 
                                                                                        channel = mq_channel)

                                                                    self._logger.info('Stage: %s, State: %s'%
                                                                                                (stage.uid, 
                                                                                                stage.state))

                                                                except Exception, ex:
                                                                    # Rolling back stage status
                                                                    self._logger.error('Error while updating stage '+
                                                                            'state, rolling back. Error: %s'%ex)

                                                                    stage.state = states.SCHEDULED

                                                                    local_prof.prof('transition', 
                                                                                    uid=stage.uid, 
                                                                                    state=stage.state,
                                                                                    msg=stage._parent_pipeline)

                                                                    sync_with_master(   obj=stage, 
                                                                                        obj_type='Stage', 
                                                                                        channel = mq_channel)



                                                                    self._logger.info('Stage: %s, State: %s'%
                                                                                                    (stage.uid, 
                                                                                                    stage.state))

                                                                try:
                                                                    pipe._increment_stage()
                                                                except:
                                                                    pass

                                                                if pipe.completed:
                                                                    #self._workload.remove(pipe) 

                                                                    pipe.state = states.DONE

                                                                    local_prof.prof('transition', 
                                                                                    uid=pipe.uid, 
                                                                                    state=pipe.state)

                                                                    sync_with_master(   obj=pipe, 
                                                                                        obj_type='Pipeline', 
                                                                                        channel = mq_channel)

                                                                    self._logger.info('Pipe: %s, State: %s'%
                                                                                                    (pipe.uid, 
                                                                                                    pipe.state))
                                                                    
                                                        elif task.state == states.FAILED:

                                                            if self._resubmit_failed:

                                                                try:
                                                                    new_task = Task()
                                                                    new_task._replicate(completed_task)

                                                                    pipe.stages[pipe.current_stage-1].add_tasks(new_task)

                                                                except Exception, ex:
                                                                    self._logger.error("Resubmission of task %s failed, error: %s"%
                                                                                                    (completed_task.uid,ex))
                                                                    raise

                                                            else:

                                                                if stage._check_stage_complete():

                                                                    try:
                                                                    
                                                                        stage.state = states.DONE

                                                                        local_prof.prof('transition', 
                                                                                        uid=stage.uid, 
                                                                                        state=stage.state,
                                                                                        msg=stage._parent_pipeline)

                                                                        sync_with_master(   obj=stage, 
                                                                                            obj_type='Stage', 
                                                                                            channel = mq_channel)

                                                                        self._logger.info('Stage: %s, State: %s'%
                                                                                                    (stage.uid, 
                                                                                                    stage.state))

                                                                        pipe._increment_stage()

                                                                    except Exception, ex:
                                                                        # Rolling back stage status
                                                                        self._logger.error('Error while updating stage '+
                                                                                        'state, rolling back. Error: %s'%ex)

                                                                        stage.state = states.SCHEDULED

                                                                        local_prof.prof('transition', 
                                                                                        uid=stage.uid, 
                                                                                        state=stage.state,
                                                                                        msg=stage._parent_pipeline)

                                                                        sync_with_master(   obj=stage, 
                                                                                            obj_type='Stage', 
                                                                                            channel = mq_channel)

                                                                        self._logger.info('Stage: %s, State: %s'%
                                                                                                    (stage.uid, 
                                                                                                    stage.state))
                                                                        pipe._decrement_stage()                                    

                                                                    if pipe.completed:

                                                                        pipe.state = states.DONE

                                                                        local_prof.prof('transition', 
                                                                                        uid=pipe.uid, 
                                                                                        state=pipe.state)

                                                                        sync_with_master(   obj=pipe, 
                                                                                            obj_type='Pipeline', 
                                                                                            channel = mq_channel)

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

                except KeyboardInterrupt:
                    raise KeyboardInterrupt

                except Exception, ex:
                    self._logger.error('Unable to receive message from completed queue: %s'%ex)
                    raise


            self._logger.info('Terminated dequeue thread')
            mq_connection.close()

            local_prof.prof('terminating dequeue-thread', uid=self._uid)

        except KeyboardInterrupt:

            self._logger.error('Execution interrupted by user (you probably hit Ctrl+C), '+
                            'trying to exit gracefully...')

            mq_connection.close()

            raise KeyboardInterrupt

        except Exception, ex:
            self._logger.error('Unknown error in thread: %s'%ex)
            print traceback.format_exc()

            mq_connection.close()

            raise Error(text=ex)



    # ------------------------------------------------------------------------------------------------------------------
    # Public Methods
    # ------------------------------------------------------------------------------------------------------------------

    def start_processor(self):

        """
        **Purpose**: Method to start the wfp process. The wfp function
        is not to be accessed directly. The function is started in a separate
        process using this method.
        """

        if not self._wfp_process:

            try:

                self._prof.prof('creating wfp process', uid=self._uid)
                self._wfp_process = Process(target=self._wfp, name='wfprocessor')

                self._enqueue_thread = None
                self._dequeue_thread = None
                self._enqueue_thread_terminate = threading.Event()
                self._dequeue_thread_terminate = threading.Event()

                self._wfp_terminate = Event()
                self._logger.info('Starting WFprocessor process')
                self._prof.prof('starting wfp process', uid=self._uid)
                self._wfp_process.start()                

                return True

            except Exception, ex:

                self.end_processor()
                self._logger.error('WFprocessor not started')
                raise Error(text=ex)      

        else:
            self._logger.info('WFprocessor already running')


    def end_processor(self):

        """
        **Purpose**: Method to terminate the wfp process. This method is 
        blocking as it waits for the wfp process to terminate (aka join).
        """

        try:
            #if not self._wfp_terminate.is_set():
            
            self._wfp_terminate.set()
            self._logger.debug('Attempting to end WFprocessor... event: %s'%self._wfp_terminate.is_set())

            if self.check_alive():
                self._wfp_process.join()
                self._logger.debug('WFprocessor process terminated')
            else:
                self._logger.debug('WFprocessor process already terminated')

            self._prof.prof('wfp process terminated', uid=self._uid)

            self._prof.close()

        except Exception, ex:
            self._logger.error('Could not terminate wfprocessor process')
            raise Error(text=ex)

    def workflow_incomplete(self):

        """
        **Purpose**: Method to check if the workflow execution is incomplete.
        """
        
        try:
            for pipe in self._workflow:
                with pipe._stage_lock:
                    if pipe.completed:                    
                        pass
                    else:
                        return True
            return False

        except KeyboardInterrupt:
            raise KeyboardInterrupt

        except Exception, ex:
            self._logger.error('Could not check if workflow is incomplete, error:%s'%ex)
            raise


    def check_alive(self):

        """
        **Purpose**: Method to check if the wfp process is alive
        """

        return self._wfp_process.is_alive()