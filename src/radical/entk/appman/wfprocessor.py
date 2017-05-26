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

slow_run = os.environ.get('RADICAL_ENTK_SLOW',False)

class WFprocessor(object):

    def __init__(self, workload, pending_queue, completed_queue, mq_hostname):

        self._uid           = ru.generate_id('radical.entk.wfprocessor')        
        self._logger        = ru.get_logger('radical.entk.wfprocessor')
        self._workload      = workload

        if not isinstance(pending_queue,list):
            raise TypeError(expected_type=list, actual_type=type(pending_queue))

        # Mqs queue names and channel
        self._pending_queue = pending_queue
        self._completed_queue = completed_queue
        self._mq_hostname = mq_hostname

        self._wfp_process = None       
        self._resubmit_failed = False       

        self._logger.info('Created WFProcessor object: %s'%self._uid)
    

    def start_processor(self):

        # This method starts the extractor function in a separate thread
        if not self._wfp_process:

            try:
                self._wfp_process = Process(target=self.wfp_process, name='wfprocessor')

                self._enqueue_thread = None
                self._dequeue_thread = None
                self._enqueue_thread_terminate = threading.Event()
                self._dequeue_thread_terminate = threading.Event()

                self._wfp_terminate = Event()
                self._logger.info('Starting WFprocessor process')
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
                self._logger.debug('WFprocessor process closed')
            else:
                self._logger.debug('WFprocessor process already closed')

        except Exception, ex:
            self._logger.error('Could not terminate wfprocessor process')

            raise

    def workload_incomplete(self):
        
        try:
            for pipe in self._workload:
                with pipe.stage_lock:
                    if pipe.completed:                    
                        pass
                    else:
                        return True
            return False

        except KeyboardInterrupt:
            raise KeyboardInterrupt

        except Exception, ex:
            self._logger.error('Could not check if workload is incomplete, error:%s'%ex)
            raise


    def wfp_process(self):

        try:

            # Process should run till terminate condtion is encountered
            self._logger.info('WFprocessor started')

            while (not self._wfp_terminate.is_set())or(self.workload_incomplete()):

                try:

                    # Start dequeue thread
                    if (not self._dequeue_thread) or (not self._dequeue_thread.is_alive()):
                        self._dequeue_thread = threading.Thread(target=self.dequeue, name='dequeue-thread')

                        self._logger.info('Starting dequeue thread')
                        self._dequeue_thread.start()

                    # Start enqueue thread
                    if (not self._enqueue_thread) or (not self._enqueue_thread.is_alive()):
                        self._enqueue_thread = threading.Thread(target=self.enqueue, name='enqueue-thread')

                        self._logger.info('Starting enqueue thread')
                        self._enqueue_thread.start()

                except KeyboardInterrupt:
                    raise KeyboardInterrupt

                except Exception, ex:
                    self._logger.error('WFProcessor interrupted')
                    raise

            self._logger.info('Terminating enqueue thread')
            self._enqueue_thread_terminate.set()
            self._enqueue_thread.join()
            self._logger.info('Terminating dequeue thread')
            self._dequeue_thread_terminate.set()
            self._dequeue_thread.join()              

        except KeyboardInterrupt:

            self._logger.error('Execution interrupted by user (you probably hit Ctrl+C), '+
                                'trying to cancel wfprocessor process gracefully...')

            if not self._enqueue_thread_terminate.is_set():
                self._logger.info('Terminating enqueue thread')
                self._enqueue_thread_terminate.set()
                self._enqueue_thread.join()

            if not self._dequeue_thread_terminate.is_set():
                self._logger.info('Terminating dequeue thread')
                self._dequeue_thread_terminate.set()
                self._dequeue_thread.join()

            self._logger.info('WFprocessor process terminated')

            raise


        except Exception, ex:
            self._logger.error('Unknown error in wfp process: %s. \n Closing all threads'%ex)
            print traceback.format_exc()

            if not self._enqueue_thread_terminate.is_set():
                self._logger.info('Terminating enqueue thread')
                self._enqueue_thread_terminate.set()
                self._enqueue_thread.join()

            if not self._dequeue_thread_terminate.is_set():
                self._logger.info('Terminating dequeue thread')
                self._dequeue_thread_terminate.set()
                self._dequeue_thread.join()

            self._logger.info('WFprocessor process terminated')

            raise UnknownError(text=ex)           


    def enqueue(self):

        try:

            self._logger.info('Enqueue thread started')

            mq_connection = pika.BlockingConnection(pika.ConnectionParameters(host=self._mq_hostname))
            mq_channel = mq_connection.channel()

            while not self._enqueue_thread_terminate.is_set():

                for pipe in self._workload:

                    with pipe.stage_lock:

                        if not pipe.completed:
    
                            self._logger.debug('Pipe %s lock acquired'%(pipe.uid))
    
                            # Update corresponding pipeline's state
                            if not pipe.state == states.SCHEDULED:
                                pipe.state = states.SCHEDULED
    
                            if pipe.stages[pipe.current_stage].state in [states.NEW]:
    
                                executable_stage = pipe.stages[pipe.current_stage]
                                executable_tasks = executable_stage.tasks
    
                                try:

                                    tasks_submitted=False
    
                                    for executable_task in executable_tasks:
    
                                        if executable_task.state == states.NEW:
                                            
                                            self._logger.debug('Task: %s,%s ; Stage: %s; Pipeline: %s'%(
                                                                executable_task.uid,
                                                                executable_task.state,
                                                                executable_task.parent_stage,
                                                                executable_task.parent_pipeline))
    
                                            # Try-exception block for tasks
                                            try:
    
                                                # Update specific task's state if put to pending_queue
                                                executable_task.state = states.QUEUED
    
                                                task_as_dict = json.dumps(executable_task.to_dict())
    
                                                self._logger.debug('Publishing task %s to %s'
                                                                            %(executable_task.uid,
                                                                                self._pending_queue[0])
                                                                        )

                                                mq_channel.basic_publish( exchange='',
                                                                                routing_key=self._pending_queue[0],
                                                                                body=task_as_dict
                                                                                #properties=pika.BasicProperties(
                                                                                # make message persistent
                                                                                #delivery_mode = 2, 
                                                                                #)
                                                                            )

                                                tasks_submitted = True
                                                self._logger.debug('Task %s published to queue'% executable_task.uid)
                                                
                                                # Update corresponding stage's state
                                                if not pipe.stages[pipe.current_stage].state == states.SCHEDULED:
                                                    pipe.stages[pipe.current_stage].state = states.SCHEDULED
    
                                            except Exception, ex:
    
                                                # Rolling back queue status
                                                self._logger.error('Error while updating task '+
                                                                    'state, rolling back. Error: %s'%ex)
                                               
                                                # Revert task status
                                                executable_task.state = states.NEW
                                                raise # should go to the next exception
                                    
                                    if tasks_submitted:
                                        self._logger.info('Stage %s of Pipeline %s: %s'%(
                                                            pipe.stages[pipe.current_stage].uid,
                                                            pipe.uid,
                                                            pipe.stages[pipe.current_stage].state))

                                        tasks_submitted = False

                                    if slow_run:
                                        sleep(1)
    
                                                                                
                                except Exception, ex:
    
                                    # Rolling back queue status
                                    self._logger.error('Error while updating stage '+
                                                        'state, rolling back. Error: %s'%ex)
    
                                    # Revert stage state
                                    pipe.stages[pipe.current_stage].state = states.NEW   
                                    raise   

                            if slow_run:
                                sleep(1)

            self._logger.info('Enqueue thread terminated')                                  
            mq_connection.close()
                                    
        except KeyboardInterrupt:

            self._logger.error('Execution interrupted by user (you probably hit Ctrl+C), '+
                                'trying to cancel enqueuer thread gracefully...')

            mq_connection.close()

        except Exception, ex:

            self._logger.error('Unknown error in wfp process: %s. \n Closing all threads'%ex)
            print traceback.format_exc()
            mq_connection.close()

            raise UnknownError(text=ex) 



    def dequeue(self):

        try:

            self._logger.info('Dequeue thread started')

            mq_connection = pika.BlockingConnection(pika.ConnectionParameters(host=self._mq_hostname))
            mq_channel = mq_connection.channel()

            while not self._dequeue_thread_terminate.is_set():

                try:

                    method_frame, header_frame, body = mq_channel.basic_get(queue=self._completed_queue[0])

                    if body:

                        # Get task from the message
                        completed_task = Task()
                        completed_task.load_from_dict(json.loads(body))

                        self._logger.debug('Got finished task %s from queue'%(completed_task.uid))

                        for pipe in self._workload:

                            with pipe.stage_lock:

                                if not pipe.completed:

                                    if completed_task.parent_pipeline == pipe.uid:

                                        self._logger.debug('Found parent pipeline: %s'%pipe.uid)
                                
                                        for stage in pipe.stages:

                                            if completed_task.parent_stage == stage.uid:
                                                self._logger.debug('Found parent stage: %s'%(stage.uid))

                                                #task.state = states.DONE
                                                self._logger.debug('Task: %s,%s ; Stage: %s; Pipeline: %s'%(
                                                                        completed_task.uid,
                                                                        completed_task.state,
                                                                        pipe.stages[pipe.current_stage].uid,
                                                                        pipe.uid)
                                                                    )

                                                for task in stage.tasks:

                                                    if task.uid == completed_task.uid:
                                                        task.state = str(completed_task.state)

                                                        if task.state == states.DONE:

                                                            if stage.check_tasks_status():

                                                                try:

                                                                    stage.state = states.DONE

                                                                    self._logger.info('Stage %s of Pipeline %s: %s'%(
                                                                                stage.uid,
                                                                                pipe.uid,
                                                                                stage.state))

                                                                    pipe.increment_stage()

                                                                except Exception, ex:
                                                                    # Rolling back stage status
                                                                    self._logger.error('Error while updating stage '+
                                                                            'state, rolling back. Error: %s'%ex)
                                                                    stage.state = stage.SCHEDULED
                                                                    pipe.decrement_stage()

                                                                    raise

                                                                if pipe.completed:
                                                                    #self._workload.remove(pipe)                                                                    
                                                                    pipe.state = states.DONE
                                                                    self._logger.info('Pipeline %s: %s'%(
                                                                                            pipe.uid, 
                                                                                            pipe.state)
                                                                                        )

                                                        elif completed_task.state == states.FAILED:

                                                            if self._resubmit_failed:

                                                                try:
                                                                    new_task = Task()
                                                                    new_task.replicate(completed_task)

                                                                    pipe.stages[pipe.current_stage].add_tasks(new_task)

                                                                except Exception, ex:
                                                                    self._logger.error("Resubmission of task %s failed, error: %s"%
                                                                                                    (completed_task.uid,ex))
                                                                    raise

                                                            else:

                                                                if stage.check_tasks_status():

                                                                    try:
                                                                    
                                                                        stage.state = states.DONE

                                                                        self._logger.info('Stage %s of Pipeline %s: %s'%(
                                                                                stage.uid,
                                                                                pipe.uid,
                                                                                stage.state))

                                                                        pipe.increment_stage()

                                                                    except Exception, ex:
                                                                        # Rolling back stage status
                                                                        self._logger.error('Error while updating stage '+
                                                                                        'state, rolling back. Error: %s'%ex)
                                                                        stage.state = stage.SCHEDULED
                                                                        pipe.decrement_stage()

                                                                        raise                                        

                                                                    if pipe.completed:
                                                                        #self._workload.remove(pipe)
                                                                        pipe.state = states.DONE
                                                                        self._logger.info('Pipeline %s: %s'%(
                                                                                            pipe.uid, 
                                                                                            pipe.state)
                                                                                        )

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

                except Exception, ex:
                    self._logger.error('Unable to receive message from completed queue: %s'%ex)
                    raise


            self._logger.info('Terminated dequeue thread')
            mq_connection.close()

        except KeyboardInterrupt:

            self._logger.error('Execution interrupted by user (you probably hit Ctrl+C), '+
                            'trying to exit gracefully...')

            mq_connection.close()

        except Exception, ex:
            self._logger.error('Unknown error in thread: %s'%ex)
            print traceback.format_exc()

            mq_connection.close()

            raise UnknownError(text=ex)


    def check_alive(self):

        return self._wfp_process.is_alive()