__copyright__   = "Copyright 2017-2018, http://radical.rutgers.edu"
__author__      = "Vivek Balasubramanian <vivek.balasubramaniana@rutgers.edu>"
__license__     = "MIT"

import radical.utils as ru
from radical.entk.exceptions import *
from multiprocessing import Process, Event
from radical.entk import states, Pipeline, Task
import time
from time import sleep
import json
import threading
import pika
import traceback
import os
import uuid

slow_run = os.environ.get('RADICAL_ENTK_SLOW',False)

class WFprocessor(object):

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
    

    def start_processor(self):

        # This method starts the extractor function in a separate thread
        if not self._wfp_process:

            try:

                self._prof.prof('creating wfp process', uid=self._uid)
                self._wfp_process = Process(target=self.wfp_process, name='wfprocessor')

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
                raise            

        else:
            self._logger.info('WFprocessor already running')

            raise



    def end_processor(self):

        # Set termination flag
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

            raise

    def workflow_incomplete(self):
        
        try:
            for pipe in self._workflow:
                with pipe._stage_lock:
                    if pipe._completed:                    
                        pass
                    else:
                        return True
            return False

        except KeyboardInterrupt:
            raise KeyboardInterrupt

        except Exception, ex:
            self._logger.error('Could not check if workflow is incomplete, error:%s'%ex)
            raise


    def wfp_process(self):

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
                        self._dequeue_thread = threading.Thread(target=self.dequeue, args=(local_prof,), name='dequeue-thread')

                        self._logger.info('Starting dequeue-thread')
                        local_prof.prof('starting dequeue-thread', uid=self._uid)
                        self._dequeue_thread.start()

                    # Start enqueue thread
                    if (not self._enqueue_thread) or (not self._enqueue_thread.is_alive()):

                        local_prof.prof('creating enqueue-thread', uid=self._uid)
                        self._enqueue_thread = threading.Thread(target=self.enqueue, args=(local_prof,), name='enqueue-thread')

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


    def enqueue(self, local_prof):

        try:

            def sync_with_master(obj, obj_type, channel):

                object_as_dict = {'object': obj.to_dict()}
                if obj_type == 'Task': 
                    object_as_dict['type'] = 'Task'

                elif obj_type == 'Stage':
                    object_as_dict['type'] = 'Stage'

                elif obj_type == 'Pipeline':
                    object_as_dict['type'] = 'Pipeline'

                corr_id = str(uuid.uuid4())

                channel.basic_publish(
                                        exchange='',
                                        routing_key='sync-to-master',
                                        body=json.dumps(object_as_dict),
                                        properties=pika.BasicProperties(
                                                        reply_to = 'sync-ack',
                                                        correlation_id = corr_id
                                                        )
                                    )

                local_prof.prof('publishing obj with state %s for sync'%obj.state, uid=obj.uid)
            
                while True:
                    #self._logger.info('waiting for ack')
                    method_frame, props, body = channel.basic_get(queue='sync-ack')

                    if body:
                        if corr_id == props.correlation_id:

                            local_prof.prof('obj with state %s synchronized'%obj.state, uid=obj.uid)

                            self._logger.info('%s synchronized'%obj.uid)

                            channel.basic_ack(delivery_tag = method_frame.delivery_tag)

                            break

            local_prof.prof('enqueue-thread started', uid=self._uid)
            self._logger.info('enqueue-thread started')

            mq_connection = pika.BlockingConnection(pika.ConnectionParameters(host=self._mq_hostname))
            mq_channel = mq_connection.channel()

            while not self._enqueue_thread_terminate.is_set():

                for pipe in self._workflow:

                    with pipe._stage_lock:

                        if not pipe._completed:
    
                            #self._logger.debug('Pipe %s lock acquired'%(pipe.uid))
    
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
    
                            if pipe.stages[pipe._current_stage-1].state in [states.INITIAL]:
    
                                try:

                                    # Starting scheduling of tasks of current stage
                                    pipe.stages[pipe._current_stage-1].state = states.SCHEDULING
                                    
                                    local_prof.prof('transition', 
                                                    uid=pipe.stages[pipe._current_stage-1].uid, 
                                                    state=pipe.stages[pipe._current_stage-1].state)

                                    sync_with_master(   obj=pipe.stages[pipe._current_stage-1], 
                                                        obj_type='Stage', 
                                                        channel = mq_channel)

                                    self._logger.info('Stage: %s, State: %s'%(pipe.stages[pipe._current_stage-1].uid, 
                                        pipe.stages[pipe._current_stage-1].state))

                                    executable_stage = pipe.stages[pipe._current_stage-1]
                                    executable_tasks = executable_stage.tasks
    
                                    tasks_submitted=False
    
                                    for executable_task in executable_tasks:
    
                                        if executable_task.state == states.INITIAL:
                                            
                                            self._logger.info('Task: %s, State: %s'%(  executable_task.uid, 
                                                                                        executable_task.state)
                                                                )
    
                                            # Try-exception block for tasks
                                            try:
    
                                                # Update specific task's state to scheduling
                                                executable_task.state = states.SCHEDULING

                                                local_prof.prof('transition', 
                                                                uid=executable_task.uid, 
                                                                state=executable_task.state)

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
                                                                state=executable_task.state)

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
                                                                state=executable_task.state)

                                                sync_with_master(   obj=executable_task, 
                                                                    obj_type='Task', 
                                                                    channel = mq_channel)

                                                self._logger.info('Task: %s, State: %s'%(  executable_task.uid, 
                                                                                        executable_task.state))

                                                raise
                                    
                                    # All tasks of current stage scheduled
                                    pipe.stages[pipe._current_stage-1].state = states.SCHEDULED                                    

                                    local_prof.prof('transition', 
                                                    uid=pipe.stages[pipe._current_stage-1].uid, 
                                                    state=pipe.stages[pipe._current_stage-1].state)

                                    sync_with_master(   obj=pipe.stages[pipe._current_stage-1], 
                                                        obj_type='Stage', 
                                                        channel = mq_channel)

                                    self._logger.info('Stage: %s, State: %s'%(  pipe.stages[pipe._current_stage-1].uid, 
                                                                                pipe.stages[pipe._current_stage-1].state))

                                    if tasks_submitted:
                                        tasks_submitted = False

                                    if slow_run:
                                        sleep(1)
                                
                                except KeyboardInterrupt:
                                    raise KeyboardInterrupt

                                                                                
                                except Exception, ex:
    
                                    # Rolling back queue status
                                    self._logger.error('Error while updating stage '+
                                                        'state, rolling back. Error: %s'%ex)
    
                                    # Revert stage state
                                    pipe.stages[pipe._current_stage-1].state = states.INITIAL

                                    local_prof.prof('transition', 
                                                    uid=pipe.stages[pipe._current_stage-1].uid, 
                                                    state=pipe.stages[pipe._current_stage-1].state)

                                    sync_with_master(   obj=pipe.stages[pipe._current_stage-1], 
                                                        obj_type='Stage', 
                                                        channel = mq_channel)

                                    self._logger.info('Stage: %s, State: %s'%(  pipe.stages[pipe._current_stage-1].uid, 
                                                                                pipe.stages[pipe._current_stage-1].state))

                                    raise

                            if slow_run:
                                sleep(1)

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



    def dequeue(self, local_prof):

        try:

            def sync_with_master(obj, obj_type, channel):

                object_as_dict = {'object': obj.to_dict()}
                if obj_type == 'Task': 
                    object_as_dict['type'] = 'Task'

                elif obj_type == 'Stage':
                    object_as_dict['type'] = 'Stage'

                elif obj_type == 'Pipeline':
                    object_as_dict['type'] = 'Pipeline'

                corr_id = str(uuid.uuid4())

                channel.basic_publish(
                                        exchange='',
                                        routing_key='sync-to-master',
                                        body=json.dumps(object_as_dict),
                                        properties=pika.BasicProperties(
                                                        reply_to = 'sync-ack',
                                                        correlation_id = corr_id
                                                        )
                                    )

                local_prof.prof('publishing obj with state %s for sync'%obj.state, uid=obj.uid)
            
                while True:
                    #self._logger.info('waiting for ack')
                    method_frame, props, body = channel.basic_get(queue='sync-ack')

                    if body:
                        if corr_id == props.correlation_id:

                            local_prof.prof('obj with state %s synchronized'%obj.state, uid=obj.uid)
                            
                            self._logger.info('%s synchronized'%obj.uid)

                            channel.basic_ack(delivery_tag = method_frame.delivery_tag)

                            break

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
                                        state=completed_task.state)

                        sync_with_master(   obj=completed_task, 
                                            obj_type='Task', 
                                            channel = mq_channel)

                        self._logger.info('Task: %s, State: %s'%(  completed_task.uid, 
                                                                    completed_task.state)
                                                                )

                        for pipe in self._workflow:

                            with pipe._stage_lock:

                                if not pipe._completed:

                                    if completed_task._parent_pipeline == pipe.uid:

                                        self._logger.debug('Found parent pipeline: %s'%pipe.uid)
                                
                                        for stage in pipe.stages:

                                            if completed_task._parent_stage == stage.uid:
                                                self._logger.debug('Found parent stage: %s'%(stage.uid))

                                                completed_task.state = states.DEQUEUED

                                                local_prof.prof('transition', 
                                                                uid=completed_task.uid, 
                                                                state=completed_task.state)

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
                                                                state=completed_task.state)

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
                                                                                    state=stage.state)

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

                                                                    sync_with_master(   obj=stage, 
                                                                                        obj_type='Stage', 
                                                                                        channel = mq_channel)

                                                                    local_prof.prof('transition', 
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

                                                                    pipe.stages[pipe._current_stage-1].add_tasks(new_task)

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
                                                                                        state=stage.state)

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
                                                                                        state=stage.state)

                                                                        sync_with_master(   obj=stage, 
                                                                                            obj_type='Stage', 
                                                                                            channel = mq_channel)

                                                                        self._logger.info('Stage: %s, State: %s'%
                                                                                                    (stage.uid, 
                                                                                                    stage.state))
                                                                        pipe._decrement_stage()                                    

                                                                    if pipe._completed:

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

                        if slow_run:
                            sleep(1)

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


    def check_alive(self):

        return self._wfp_process.is_alive()